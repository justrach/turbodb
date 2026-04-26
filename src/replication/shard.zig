const std = @import("std");
const runtime = @import("runtime");

pub const NodeId = u16;
pub const PartitionId = u16;
pub const ShardId = u32;

/// Mapping of partition → node (shard map).
pub const ShardEntry = struct {
    partition_id: PartitionId,
    node_id: NodeId,
    shard_id: ShardId,
    is_primary: bool,
    address: [64]u8,
    address_len: u8,
    port: u16,
};

/// Consistent hashing ring for partition → shard mapping.
pub const HashRing = struct {
    /// Virtual nodes for balance (150 vnodes per physical node)
    vnodes: std.ArrayListUnmanaged(VNode),
    alloc: std.mem.Allocator,
    vnodes_per_node: u32 = 150,

    pub const VNode = struct {
        hash: u64,
        node_id: NodeId,
    };

    pub fn init(alloc: std.mem.Allocator) HashRing {
        return .{
            .vnodes = .empty,
            .alloc = alloc,
        };
    }

    pub fn deinit(self: *HashRing) void {
        self.vnodes.deinit(self.alloc);
    }

    /// Add a node with 150 virtual nodes spread around the ring.
    pub fn addNode(self: *HashRing, node_id: NodeId) !void {
        var i: u32 = 0;
        while (i < self.vnodes_per_node) : (i += 1) {
            const hash = computeVNodeHash(node_id, i);
            try self.vnodes.append(self.alloc, .{ .hash = hash, .node_id = node_id });
        }
        // Sort by hash for binary search in getNode.
        std.mem.sort(VNode, self.vnodes.items, {}, struct {
            fn lessThan(_: void, a: VNode, b: VNode) bool {
                return a.hash < b.hash;
            }
        }.lessThan);
    }

    /// Remove all virtual nodes for a given physical node.
    pub fn removeNode(self: *HashRing, node_id: NodeId) void {
        var write: usize = 0;
        for (self.vnodes.items) |vn| {
            if (vn.node_id != node_id) {
                self.vnodes.items[write] = vn;
                write += 1;
            }
        }
        self.vnodes.shrinkRetainingCapacity(write);
    }

    /// Find the node that owns a given key hash (first vnode with hash >= key_hash).
    pub fn getNode(self: *const HashRing, key_hash: u64) ?NodeId {
        if (self.vnodes.items.len == 0) return null;

        // Binary search for the first vnode with hash >= key_hash.
        var lo: usize = 0;
        var hi: usize = self.vnodes.items.len;
        while (lo < hi) {
            const mid = lo + (hi - lo) / 2;
            if (self.vnodes.items[mid].hash < key_hash) {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        // Wrap around the ring.
        const idx = if (lo >= self.vnodes.items.len) 0 else lo;
        return self.vnodes.items[idx].node_id;
    }

    /// Get `count` distinct nodes for replication (walk the ring).
    pub fn getNodes(self: *const HashRing, key_hash: u64, count: u8, buf: []NodeId) []NodeId {
        if (self.vnodes.items.len == 0) return buf[0..0];

        // Find starting position.
        var lo: usize = 0;
        var hi: usize = self.vnodes.items.len;
        while (lo < hi) {
            const mid = lo + (hi - lo) / 2;
            if (self.vnodes.items[mid].hash < key_hash) {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }

        var found: u8 = 0;
        const n = self.vnodes.items.len;
        var i: usize = 0;
        while (i < n and found < count) : (i += 1) {
            const idx = (lo + i) % n;
            const nid = self.vnodes.items[idx].node_id;
            // Deduplicate: only add distinct node IDs.
            var dup = false;
            for (buf[0..found]) |existing| {
                if (existing == nid) {
                    dup = true;
                    break;
                }
            }
            if (!dup) {
                buf[found] = nid;
                found += 1;
            }
        }
        return buf[0..found];
    }

    fn computeVNodeHash(node_id: NodeId, index: u32) u64 {
        var hasher = std.hash.Wyhash.init(0);
        hasher.update(std.mem.asBytes(&node_id));
        hasher.update(std.mem.asBytes(&index));
        return hasher.final();
    }
};

/// Shard manager — maintains the shard map and handles migration.
pub const ShardManager = struct {
    shard_map: std.AutoHashMapUnmanaged(PartitionId, ShardEntry),
    ring: HashRing,
    local_node_id: NodeId,
    alloc: std.mem.Allocator,
    mu: std.Io.RwLock,
    migrations: std.ArrayListUnmanaged(Migration),

    pub fn init(alloc: std.mem.Allocator, local_node_id: NodeId) ShardManager {
        return .{
            .shard_map = .empty,
            .ring = HashRing.init(alloc),
            .local_node_id = local_node_id,
            .alloc = alloc,
            .mu = .init,
            .migrations = .empty,
        };
    }

    pub fn deinit(self: *ShardManager) void {
        self.shard_map.deinit(self.alloc);
        self.ring.deinit();
        self.migrations.deinit(self.alloc);
    }

    /// Register a partition on a node.
    pub fn registerPartition(self: *ShardManager, entry: ShardEntry) !void {
        self.mu.lockUncancelable(runtime.io);
        defer self.mu.unlock(runtime.io);
        try self.shard_map.put(self.alloc, entry.partition_id, entry);
    }

    /// Look up which node owns a partition.
    pub fn getOwner(self: *ShardManager, partition_id: PartitionId) ?ShardEntry {
        self.mu.lockSharedUncancelable(runtime.io);
        defer self.mu.unlockShared(runtime.io);
        return self.shard_map.get(partition_id);
    }

    /// Look up which node should own a key (via consistent hashing).
    pub fn routeKey(self: *ShardManager, key_hash: u64) ?NodeId {
        self.mu.lockSharedUncancelable(runtime.io);
        defer self.mu.unlockShared(runtime.io);
        return self.ring.getNode(key_hash);
    }

    /// Check if a partition is local.
    pub fn isLocal(self: *ShardManager, partition_id: PartitionId) bool {
        self.mu.lockSharedUncancelable(runtime.io);
        defer self.mu.unlockShared(runtime.io);
        if (self.shard_map.get(partition_id)) |entry| {
            return entry.node_id == self.local_node_id;
        }
        return false;
    }

    /// Get all local partition IDs.
    pub fn localPartitions(self: *ShardManager, alloc_: std.mem.Allocator) ![]PartitionId {
        self.mu.lockSharedUncancelable(runtime.io);
        defer self.mu.unlockShared(runtime.io);

        var list: std.ArrayListUnmanaged(PartitionId) = .empty;
        var it = self.shard_map.iterator();
        while (it.next()) |kv| {
            if (kv.value_ptr.node_id == self.local_node_id) {
                try list.append(alloc_, kv.key_ptr.*);
            }
        }
        return list.toOwnedSlice(alloc_);
    }

    /// Initiate migration of a partition to a target node.
    pub fn migratePartition(self: *ShardManager, partition_id: PartitionId, target_node: NodeId) !void {
        self.mu.lockUncancelable(runtime.io);
        defer self.mu.unlock(runtime.io);

        const entry = self.shard_map.get(partition_id) orelse return error.PartitionNotFound;
        const migration = Migration{
            .partition_id = partition_id,
            .source_node = entry.node_id,
            .target_node = target_node,
            .state = .streaming,
            .bytes_transferred = 0,
            .total_bytes = 0,
        };
        try self.migrations.append(self.alloc, migration);
    }

    /// Get shard map snapshot for serialization.
    pub fn snapshot(self: *ShardManager, alloc_: std.mem.Allocator) ![]ShardEntry {
        self.mu.lockSharedUncancelable(runtime.io);
        defer self.mu.unlockShared(runtime.io);

        var list: std.ArrayListUnmanaged(ShardEntry) = .empty;
        var it = self.shard_map.iterator();
        while (it.next()) |kv| {
            try list.append(alloc_, kv.value_ptr.*);
        }
        return list.toOwnedSlice(alloc_);
    }
};

/// Migration state tracking.
pub const MigrationState = enum { idle, streaming, catchup, cutover, complete };

pub const Migration = struct {
    partition_id: PartitionId,
    source_node: NodeId,
    target_node: NodeId,
    state: MigrationState,
    bytes_transferred: u64,
    total_bytes: u64,

    /// Progress from 0.0 to 1.0.
    pub fn progress(self: *const Migration) f64 {
        if (self.state == .complete) return 1.0;
        if (self.state == .idle) return 0.0;
        if (self.total_bytes == 0) return 0.0;
        const transferred: f64 = @floatFromInt(self.bytes_transferred);
        const total: f64 = @floatFromInt(self.total_bytes);
        return transferred / total;
    }
};

// ─── Tests ──────────────────────────────────────────────────────────────────

test "HashRing: addNode populates vnodes" {
    var ring = HashRing.init(std.testing.allocator);
    defer ring.deinit();

    try ring.addNode(1);
    try std.testing.expectEqual(@as(usize, 150), ring.vnodes.items.len);

    try ring.addNode(2);
    try std.testing.expectEqual(@as(usize, 300), ring.vnodes.items.len);
}

test "HashRing: getNode returns a node" {
    var ring = HashRing.init(std.testing.allocator);
    defer ring.deinit();

    try ring.addNode(1);
    try ring.addNode(2);

    const node = ring.getNode(12345);
    try std.testing.expect(node != null);
    try std.testing.expect(node.? == 1 or node.? == 2);
}

test "HashRing: getNode distribution is reasonably balanced" {
    var ring = HashRing.init(std.testing.allocator);
    defer ring.deinit();

    try ring.addNode(1);
    try ring.addNode(2);
    try ring.addNode(3);

    var counts = [_]u32{0} ** 4; // index 0 unused, 1-3 for nodes
    var i: u64 = 0;
    while (i < 10000) : (i += 1) {
        // Use FNV-1a style mixing for better key distribution.
        const key = i *% 14695981039346656037;
        if (ring.getNode(key)) |nid| {
            counts[nid] += 1;
        }
    }
    // Each of 3 nodes should get at least some keys. With 10K samples
    // and 150 vnodes per node, even worst-case should get >5%.
    try std.testing.expect(counts[1] > 100);
    try std.testing.expect(counts[2] > 100);
    try std.testing.expect(counts[3] > 100);
    // And total should be 10000.
    try std.testing.expectEqual(@as(u32, 10000), counts[1] + counts[2] + counts[3]);
}

test "HashRing: removeNode removes all vnodes for that node" {
    var ring = HashRing.init(std.testing.allocator);
    defer ring.deinit();

    try ring.addNode(1);
    try ring.addNode(2);
    try std.testing.expectEqual(@as(usize, 300), ring.vnodes.items.len);

    ring.removeNode(1);
    try std.testing.expectEqual(@as(usize, 150), ring.vnodes.items.len);

    // All remaining vnodes should belong to node 2.
    for (ring.vnodes.items) |vn| {
        try std.testing.expectEqual(@as(NodeId, 2), vn.node_id);
    }
}

test "HashRing: getNode on empty ring returns null" {
    var ring = HashRing.init(std.testing.allocator);
    defer ring.deinit();
    try std.testing.expectEqual(@as(?NodeId, null), ring.getNode(42));
}

test "ShardManager: registerPartition and getOwner" {
    var mgr = ShardManager.init(std.testing.allocator, 1);
    defer mgr.deinit();

    var addr = [_]u8{0} ** 64;
    const host = "127.0.0.1";
    @memcpy(addr[0..host.len], host);

    const entry = ShardEntry{
        .partition_id = 10,
        .node_id = 1,
        .shard_id = 100,
        .is_primary = true,
        .address = addr,
        .address_len = @intCast(host.len),
        .port = 5000,
    };

    try mgr.registerPartition(entry);

    const owner = mgr.getOwner(10);
    try std.testing.expect(owner != null);
    try std.testing.expectEqual(@as(NodeId, 1), owner.?.node_id);
    try std.testing.expectEqual(@as(u16, 5000), owner.?.port);
}

test "ShardManager: getOwner returns null for unknown partition" {
    var mgr = ShardManager.init(std.testing.allocator, 1);
    defer mgr.deinit();
    try std.testing.expectEqual(@as(?ShardEntry, null), mgr.getOwner(999));
}

test "ShardManager: isLocal" {
    var mgr = ShardManager.init(std.testing.allocator, 1);
    defer mgr.deinit();

    const addr = [_]u8{0} ** 64;

    try mgr.registerPartition(.{
        .partition_id = 10,
        .node_id = 1, // local
        .shard_id = 100,
        .is_primary = true,
        .address = addr,
        .address_len = 0,
        .port = 5000,
    });
    try mgr.registerPartition(.{
        .partition_id = 20,
        .node_id = 2, // remote
        .shard_id = 200,
        .is_primary = true,
        .address = addr,
        .address_len = 0,
        .port = 5001,
    });

    try std.testing.expect(mgr.isLocal(10));
    try std.testing.expect(!mgr.isLocal(20));
    try std.testing.expect(!mgr.isLocal(99));
}

test "ShardManager: routeKey via hash ring" {
    var mgr = ShardManager.init(std.testing.allocator, 1);
    defer mgr.deinit();

    try mgr.ring.addNode(1);
    try mgr.ring.addNode(2);

    const node = mgr.routeKey(42);
    try std.testing.expect(node != null);
}

test "ShardManager: snapshot returns all entries" {
    var mgr = ShardManager.init(std.testing.allocator, 1);
    defer mgr.deinit();

    const addr = [_]u8{0} ** 64;
    try mgr.registerPartition(.{ .partition_id = 1, .node_id = 1, .shard_id = 10, .is_primary = true, .address = addr, .address_len = 0, .port = 5000 });
    try mgr.registerPartition(.{ .partition_id = 2, .node_id = 2, .shard_id = 20, .is_primary = true, .address = addr, .address_len = 0, .port = 5001 });

    const snap = try mgr.snapshot(std.testing.allocator);
    defer std.testing.allocator.free(snap);
    try std.testing.expectEqual(@as(usize, 2), snap.len);
}

test "Migration: state tracking and progress" {
    var m = Migration{
        .partition_id = 1,
        .source_node = 1,
        .target_node = 2,
        .state = .idle,
        .bytes_transferred = 0,
        .total_bytes = 1000,
    };

    try std.testing.expectEqual(@as(f64, 0.0), m.progress());

    m.state = .streaming;
    m.bytes_transferred = 500;
    try std.testing.expectApproxEqAbs(@as(f64, 0.5), m.progress(), 0.001);

    m.state = .catchup;
    m.bytes_transferred = 750;
    try std.testing.expectApproxEqAbs(@as(f64, 0.75), m.progress(), 0.001);

    m.state = .complete;
    try std.testing.expectEqual(@as(f64, 1.0), m.progress());
}

test "Migration: progress with zero total_bytes" {
    const m = Migration{
        .partition_id = 1,
        .source_node = 1,
        .target_node = 2,
        .state = .streaming,
        .bytes_transferred = 0,
        .total_bytes = 0,
    };
    try std.testing.expectEqual(@as(f64, 0.0), m.progress());
}
