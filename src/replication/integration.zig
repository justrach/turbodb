/// TurboDB — Calvin Integration Layer
///
/// Bridges the Calvin replication protocol to TurboDB's Collection API.
/// This is the glue that makes replication actually work:
///
///   Client write → Sequencer → Batch → Broadcast → CalvinExecutor → Collection
///
/// The `ReplicatedDB` wraps a Database (as opaque handle) and optionally
/// routes writes through the Calvin sequencer. Reads always go directly
/// to the local collection.
///
/// Design: Uses opaque pointers for Database/Collection to avoid cross-module
/// imports. The actual DB operations are routed through function pointers
/// set at init time by the caller (main.zig).
const std = @import("std");
const sequencer = @import("sequencer.zig");
const calvin = @import("calvin.zig");
const peer = @import("peer.zig");

const Transaction = sequencer.Transaction;
const Sequencer = sequencer.Sequencer;
const CalvinExecutor = calvin.CalvinExecutor;
const ReplicationManager = calvin.ReplicationManager;
const PeerSender = peer.PeerSender;
const PeerReceiver = peer.PeerReceiver;

/// Function pointers for database operations. Set by main.zig at init.
/// This avoids importing collection.zig from the replication directory.
pub const DBOps = struct {
    /// fn(db: *anyopaque, col_name: []const u8, key: []const u8, value: []const u8) -> ?u64
    insert_fn: ?*const fn (*anyopaque, []const u8, []const u8, []const u8) ?u64 = null,
    /// fn(db: *anyopaque, col_name: []const u8, key: []const u8, value: []const u8) -> bool
    update_fn: ?*const fn (*anyopaque, []const u8, []const u8, []const u8) bool = null,
    /// fn(db: *anyopaque, col_name: []const u8, key: []const u8) -> bool
    delete_fn: ?*const fn (*anyopaque, []const u8, []const u8) bool = null,
};

/// FNV-1a hash (duplicated here to avoid cross-module import of doc.zig).
fn fnv1a(s: []const u8) u64 {
    var h: u64 = 14695981039346656037;
    for (s) |b| {
        h ^= @as(u64, b);
        h = h *% 1099511628211;
    }
    return h;
}

/// Replication configuration.
pub const ReplicationConfig = struct {
    enabled: bool = false,
    node_id: u16 = 0,
    is_leader: bool = false,
    repl_port: u16 = 27117,
    partitions: []const u16 = &.{0},
};

/// A replicated database wrapper. Intercepts writes and routes them
/// through Calvin when replication is enabled.
pub const ReplicatedDB = struct {
    db_handle: *anyopaque,
    ops: DBOps,
    config: ReplicationConfig,
    repl_mgr: ?ReplicationManager,
    sender: PeerSender,
    receiver: ?PeerReceiver,
    alloc: std.mem.Allocator,
    next_txn_id: std.atomic.Value(u64),

    pub fn init(
        alloc: std.mem.Allocator,
        db_handle: *anyopaque,
        ops: DBOps,
        config: ReplicationConfig,
    ) !ReplicatedDB {
        var rdb = ReplicatedDB{
            .db_handle = db_handle,
            .ops = ops,
            .config = config,
            .repl_mgr = null,
            .sender = PeerSender{},
            .receiver = null,
            .alloc = alloc,
            .next_txn_id = std.atomic.Value(u64).init(1),
        };

        if (config.enabled) {
            var mgr = ReplicationManager.init(alloc, config.node_id, config.is_leader);
            try mgr.executor.setLocalPartitions(config.partitions);
            rdb.repl_mgr = mgr;
        }

        return rdb;
    }

    pub fn deinit(self: *ReplicatedDB) void {
        if (self.receiver) |*r| r.stop();
        if (self.repl_mgr) |*mgr| mgr.deinit();
    }

    /// Start replication (leader batch loop + replica receiver).
    pub fn startReplication(self: *ReplicatedDB) !void {
        if (self.repl_mgr) |*mgr| {
            // Set global state for the executor callback
            g_db_handle = self.db_handle;
            g_ops = self.ops;

            try mgr.start();

            if (!self.config.is_leader) {
                self.receiver = PeerReceiver.init(
                    self.alloc,
                    self.config.repl_port,
                    &mgr.executor,
                    &executeTransaction,
                );
                const t = try std.Thread.spawn(.{}, PeerReceiver.run, .{&self.receiver.?});
                t.detach();
            }
        }
    }

    /// Add a peer replica for batch broadcast (leader only).
    pub fn addPeer(self: *ReplicatedDB, host: []const u8, port: u16) void {
        self.sender.addPeer(host, port);
    }

    // ── Write operations (routed through Calvin when replicated) ─────────

    /// Insert a document. If replicated, submits to sequencer; otherwise direct.
    pub fn insert(self: *ReplicatedDB, col_name: []const u8, key: []const u8, value: []const u8) ?u64 {
        if (self.repl_mgr) |*mgr| {
            const key_hash = fnv1a(key);
            const partition: u16 = @intCast(key_hash % 256);

            var data_buf: [4096]u8 = undefined;
            const data_len = packTxnData(&data_buf, col_name, key, value);

            const data = self.alloc.alloc(u8, data_len) catch return null;
            @memcpy(data, data_buf[0..data_len]);

            const ws = self.alloc.alloc(u64, 1) catch return null;
            ws[0] = key_hash;

            const txn_id = self.next_txn_id.fetchAdd(1, .monotonic);
            const txn = Transaction{
                .txn_id = txn_id,
                .txn_type = .put,
                .partition_id = partition,
                .key_hash = key_hash,
                .data = data,
                .read_set = &.{},
                .write_set = ws,
            };

            mgr.submit(txn) catch return null;
            return txn_id;
        }

        // Direct path
        if (self.ops.insert_fn) |f| return f(self.db_handle, col_name, key, value);
        return null;
    }

    /// Update a document.
    pub fn update(self: *ReplicatedDB, col_name: []const u8, key: []const u8, value: []const u8) bool {
        if (self.repl_mgr) |*mgr| {
            const key_hash = fnv1a(key);
            const partition: u16 = @intCast(key_hash % 256);

            var data_buf: [4096]u8 = undefined;
            const data_len = packTxnData(&data_buf, col_name, key, value);

            const data = self.alloc.alloc(u8, data_len) catch return false;
            @memcpy(data, data_buf[0..data_len]);

            const ws = self.alloc.alloc(u64, 1) catch return false;
            ws[0] = key_hash;

            const txn = Transaction{
                .txn_id = self.next_txn_id.fetchAdd(1, .monotonic),
                .txn_type = .put, // update = put with same key
                .partition_id = partition,
                .key_hash = key_hash,
                .data = data,
                .read_set = &.{},
                .write_set = ws,
            };

            mgr.submit(txn) catch return false;
            return true;
        }

        if (self.ops.update_fn) |f| return f(self.db_handle, col_name, key, value);
        return false;
    }

    /// Delete a document.
    pub fn delete(self: *ReplicatedDB, col_name: []const u8, key: []const u8) bool {
        if (self.repl_mgr) |*mgr| {
            const key_hash = fnv1a(key);
            const partition: u16 = @intCast(key_hash % 256);

            var data_buf: [256]u8 = undefined;
            const data_len = packTxnData(&data_buf, col_name, key, "");

            const data = self.alloc.alloc(u8, data_len) catch return false;
            @memcpy(data, data_buf[0..data_len]);

            const ws = self.alloc.alloc(u64, 1) catch return false;
            ws[0] = key_hash;

            const txn = Transaction{
                .txn_id = self.next_txn_id.fetchAdd(1, .monotonic),
                .txn_type = .delete,
                .partition_id = partition,
                .key_hash = key_hash,
                .data = data,
                .read_set = &.{},
                .write_set = ws,
            };

            mgr.submit(txn) catch return false;
            return true;
        }

        if (self.ops.delete_fn) |f| return f(self.db_handle, col_name, key);
        return false;
    }

    /// Get replication lag (0 if not replicated).
    pub fn replicationLag(self: *const ReplicatedDB) u64 {
        if (self.repl_mgr) |*mgr| return mgr.lag();
        return 0;
    }

    pub fn isLeader(self: *const ReplicatedDB) bool {
        return self.config.is_leader;
    }
};

// ── Transaction data packing ─────────────────────────────────────────────────
// Format: [col_name_len:u8][col_name][key_len:u16 LE][key][value]

fn packTxnData(buf: []u8, col_name: []const u8, key: []const u8, value: []const u8) usize {
    var pos: usize = 0;
    buf[pos] = @intCast(col_name.len);
    pos += 1;
    @memcpy(buf[pos..][0..col_name.len], col_name);
    pos += col_name.len;
    std.mem.writeInt(u16, buf[pos..][0..2], @intCast(key.len), .little);
    pos += 2;
    @memcpy(buf[pos..][0..key.len], key);
    pos += key.len;
    @memcpy(buf[pos..][0..value.len], value);
    pos += value.len;
    return pos;
}

fn unpackTxnData(data: []const u8) ?struct { col: []const u8, key: []const u8, val: []const u8 } {
    if (data.len < 4) return null;
    var pos: usize = 0;
    const col_len: usize = data[pos];
    pos += 1;
    if (pos + col_len > data.len) return null;
    const col = data[pos..][0..col_len];
    pos += col_len;
    if (pos + 2 > data.len) return null;
    const key_len = std.mem.readInt(u16, data[pos..][0..2], .little);
    pos += 2;
    if (pos + key_len > data.len) return null;
    const key = data[pos..][0..key_len];
    pos += key_len;
    return .{ .col = col, .key = key, .val = data[pos..] };
}

// ── Transaction executor (called by CalvinExecutor) ──────────────────────────

var g_db_handle: ?*anyopaque = null;
var g_ops: DBOps = .{};

fn executeTransaction(txn: *const Transaction) anyerror!void {
    const db = g_db_handle orelse return error.DatabaseNotInitialized;
    const parsed = unpackTxnData(txn.data) orelse return error.InvalidData;

    switch (txn.txn_type) {
        .put => {
            // put = insert or update (same semantics: upsert)
            if (g_ops.insert_fn) |f| {
                _ = f(db, parsed.col, parsed.key, parsed.val);
            }
        },
        .delete => {
            if (g_ops.delete_fn) |f| {
                _ = f(db, parsed.col, parsed.key);
            }
        },
        .read => {},
    }
}

// ── Tests ────────────────────────────────────────────────────────────────────

test "packTxnData and unpackTxnData round-trip" {
    var buf: [256]u8 = undefined;
    const len = packTxnData(&buf, "users", "alice", "{\"name\":\"Alice\"}");

    const parsed = unpackTxnData(buf[0..len]).?;
    try std.testing.expectEqualStrings("users", parsed.col);
    try std.testing.expectEqualStrings("alice", parsed.key);
    try std.testing.expectEqualStrings("{\"name\":\"Alice\"}", parsed.val);
}

test "packTxnData empty value (delete)" {
    var buf: [256]u8 = undefined;
    const len = packTxnData(&buf, "users", "alice", "");

    const parsed = unpackTxnData(buf[0..len]).?;
    try std.testing.expectEqualStrings("users", parsed.col);
    try std.testing.expectEqualStrings("alice", parsed.key);
    try std.testing.expectEqual(@as(usize, 0), parsed.val.len);
}

test "unpackTxnData invalid data" {
    const result = unpackTxnData("ab");
    try std.testing.expect(result == null);
}

test "ReplicatedDB direct mode" {
    const ops = DBOps{
        .insert_fn = struct {
            fn f(_: *anyopaque, _: []const u8, _: []const u8, _: []const u8) ?u64 {
                return 42;
            }
        }.f,
    };

    var dummy_db: u8 = 0;
    var rdb = try ReplicatedDB.init(std.testing.allocator, @ptrCast(&dummy_db), ops, .{});
    defer rdb.deinit();

    const result = rdb.insert("col", "key", "val");
    try std.testing.expectEqual(@as(?u64, 42), result);
    try std.testing.expectEqual(@as(u64, 0), rdb.replicationLag());
}

test "ReplicatedDB replicated mode submits to sequencer" {
    var dummy_db: u8 = 0;
    var rdb = try ReplicatedDB.init(std.testing.allocator, @ptrCast(&dummy_db), .{}, .{
        .enabled = true,
        .node_id = 1,
        .is_leader = true,
    });
    defer rdb.deinit();

    // Insert through sequencer returns a txn_id
    const result = rdb.insert("users", "alice", "{\"name\":\"Alice\"}");
    try std.testing.expect(result != null);
    try std.testing.expect(result.? >= 1);
    try std.testing.expect(rdb.isLeader());
}

test "fnv1a matches expected" {
    const h = fnv1a("hello");
    try std.testing.expect(h != 0);
    // Deterministic
    try std.testing.expectEqual(h, fnv1a("hello"));
    // Different inputs → different hashes
    try std.testing.expect(h != fnv1a("world"));
}
