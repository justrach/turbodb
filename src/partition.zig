/// TurboDB — Hash/Range Partitioning
///
/// Distributes documents across N independent Collection shards.
/// Each partition is a separate .pages file: `{col_name}_p{idx}.pages`
///
/// Routing strategies:
///   - Hash:  partition_idx = fnv1a(key) % n_partitions
///   - Range: binary search over sorted boundary array
///
const std = @import("std");
const collection_mod = @import("collection.zig");
const doc_mod = @import("doc.zig");
const wal_mod = @import("wal");
const epoch_mod = @import("epoch");
const cdc_mod = @import("cdc.zig");

const Collection = collection_mod.Collection;
const Doc = doc_mod.Doc;
const ScanResult = Collection.ScanResult;
const WAL = wal_mod.WAL;
const EpochManager = epoch_mod.EpochManager;

// ─── Strategy ────────────────────────────────────────────────────────────

pub const Strategy = enum { hash, range };

pub const RangeSpec = struct {
    boundaries: []const u64, // sorted key_hash boundaries; len == n_partitions - 1
};

// ─── Partition Stats ─────────────────────────────────────────────────────

pub const PartitionStat = struct {
    partition_id: u16,
    doc_count: u64,
    page_count: u32,
};

// ─── PartitionedCollection ───────────────────────────────────────────────

pub const PartitionedCollection = struct {
    name_buf: [64]u8,
    name_len: u8,
    partitions: []*Collection,
    n_partitions: u16,
    strategy: Strategy,
    range_spec: ?RangeSpec,
    alloc: std.mem.Allocator,

    /// Open a partitioned collection. Creates N underlying Collection shards,
    /// each backed by `{col_name}_p{idx}.pages`.
    pub fn open(
        alloc: std.mem.Allocator,
        data_dir: []const u8,
        col_name: []const u8,
        n_partitions: u16,
        strategy: Strategy,
        wal_log: *WAL,
        epochs: *EpochManager,
        cdc: *cdc_mod.CDCManager,
    ) !*PartitionedCollection {
        if (n_partitions == 0) return error.InvalidPartitionCount;

        const pc = try alloc.create(PartitionedCollection);
        errdefer alloc.destroy(pc);

        // Store name
        if (col_name.len > 64) return error.NameTooLong;
        var buf: [64]u8 = undefined;
        @memcpy(buf[0..col_name.len], col_name);
        pc.name_buf = buf;
        pc.name_len = @intCast(col_name.len);
        pc.n_partitions = n_partitions;
        pc.strategy = strategy;
        pc.range_spec = null;
        pc.alloc = alloc;

        // Allocate partition slice
        const parts = try alloc.alloc(*Collection, n_partitions);
        errdefer alloc.free(parts);

        var opened: u16 = 0;
        errdefer {
            for (0..opened) |i| parts[i].close();
        }

        var part_name_buf: [128]u8 = undefined;
        for (0..n_partitions) |i| {
            const part_name = std.fmt.bufPrint(&part_name_buf, "{s}_p{d}", .{ col_name, i }) catch
                return error.NameTooLong;
            parts[i] = try Collection.open(alloc, data_dir, part_name, part_name, wal_log, epochs, cdc);
            opened += 1;
        }

        pc.partitions = parts;
        return pc;
    }

    /// Close all partition shards and free resources.
    pub fn close(self: *PartitionedCollection) void {
        for (self.partitions) |p| p.close();
        self.alloc.free(self.partitions);
        self.alloc.destroy(self);
    }

    /// Collection name.
    pub fn name(self: *const PartitionedCollection) []const u8 {
        return self.name_buf[0..self.name_len];
    }

    // ─── routing ─────────────────────────────────────────────────────────

    /// Route a key to its partition index.
    pub fn route(self: *const PartitionedCollection, key: []const u8) u16 {
        const h = doc_mod.fnv1a(key);
        return self.routeByHash(h);
    }

    fn routeByHash(self: *const PartitionedCollection, key_hash: u64) u16 {
        switch (self.strategy) {
            .hash => return @intCast(key_hash % self.n_partitions),
            .range => {
                const spec = self.range_spec orelse return 0;
                const bounds = spec.boundaries;
                // Binary search: find first boundary > key_hash
                var lo: usize = 0;
                var hi: usize = bounds.len;
                while (lo < hi) {
                    const mid = lo + (hi - lo) / 2;
                    if (bounds[mid] <= key_hash) {
                        lo = mid + 1;
                    } else {
                        hi = mid;
                    }
                }
                return @intCast(lo);
            },
        }
    }

    // ─── CRUD ────────────────────────────────────────────────────────────

    /// Insert a document, routing to the correct partition.
    pub fn insert(self: *PartitionedCollection, key: []const u8, value: []const u8) !u64 {
        const idx = self.route(key);
        return self.partitions[idx].insert(key, value);
    }

    /// Get a document by key, routing to the correct partition.
    pub fn get(self: *PartitionedCollection, key: []const u8) ?Doc {
        const idx = self.route(key);
        return self.partitions[idx].get(key);
    }

    /// Update a document, routing to the correct partition.
    pub fn update(self: *PartitionedCollection, key: []const u8, new_value: []const u8) !bool {
        const idx = self.route(key);
        return self.partitions[idx].update(key, new_value);
    }

    /// Delete a document, routing to the correct partition.
    pub fn delete(self: *PartitionedCollection, key: []const u8) !bool {
        const idx = self.route(key);
        return self.partitions[idx].delete(key);
    }

    // ─── scatter-gather scan ─────────────────────────────────────────────

    /// Scatter-gather scan across all partitions. Collects docs, sorts by
    /// doc_id, then applies offset/limit.
    pub fn scan(
        self: *PartitionedCollection,
        limit: u32,
        offset: u32,
        alloc_: std.mem.Allocator,
    ) !ScanResult {
        var all_docs: std.ArrayList(Doc) = .{};
        defer all_docs.deinit(alloc_);

        // Gather from each partition — request enough to cover offset + limit
        const fetch_limit: u32 = if (offset <= std.math.maxInt(u32) - limit) offset + limit else std.math.maxInt(u32);
        for (self.partitions[0..self.n_partitions]) |p| {
            const partial = try p.scan(fetch_limit, 0, alloc_);
            defer alloc_.free(partial.docs);
            try all_docs.appendSlice(alloc_, partial.docs);
        }

        // Sort by doc_id for deterministic ordering
        std.mem.sortUnstable(Doc, all_docs.items, {}, docIdLessThan);

        // Apply offset/limit
        const start = @min(offset, @as(u32, @intCast(all_docs.items.len)));
        const end = @min(start + limit, @as(u32, @intCast(all_docs.items.len)));
        const slice = all_docs.items[start..end];

        const out = try alloc_.alloc(Doc, slice.len);
        @memcpy(out, slice);
        return ScanResult{ .docs = out, .alloc = alloc_ };
    }

    /// Parallel scatter-gather scan. Spawns one thread per partition,
    /// merges results sorted by doc_id, then applies limit.
    pub fn parallelScan(
        self: *PartitionedCollection,
        limit: u32,
        alloc_: std.mem.Allocator,
    ) !ScanResult {
        const n: usize = self.n_partitions;

        const results = try alloc_.alloc(?ScanResult, n);
        defer alloc_.free(results);
        for (results) |*r| r.* = null;

        const errors = try alloc_.alloc(?anyerror, n);
        defer alloc_.free(errors);
        for (errors) |*e| e.* = null;

        // Spawn threads
        const threads = try alloc_.alloc(std.Thread, n);
        defer alloc_.free(threads);

        for (0..n) |i| {
            threads[i] = try std.Thread.spawn(.{}, partitionScanWorker, .{
                self.partitions[i], limit, alloc_, &results[i], &errors[i],
            });
        }

        // Join all
        for (0..n) |i| threads[i].join();

        // Check for errors
        for (errors) |maybe_err| {
            if (maybe_err) |e| return e;
        }

        // Merge
        var merged: std.ArrayList(Doc) = .{};
        defer merged.deinit(alloc_);

        for (results) |maybe_res| {
            if (maybe_res) |res| {
                defer alloc_.free(res.docs);
                try merged.appendSlice(alloc_, res.docs);
            }
        }

        std.mem.sortUnstable(Doc, merged.items, {}, docIdLessThan);

        const capped = @min(limit, @as(u32, @intCast(merged.items.len)));
        const out = try alloc_.alloc(Doc, capped);
        @memcpy(out, merged.items[0..capped]);
        return ScanResult{ .docs = out, .alloc = alloc_ };
    }

    fn partitionScanWorker(
        partition: *Collection,
        limit: u32,
        alloc_: std.mem.Allocator,
        result: *?ScanResult,
        err_out: *?anyerror,
    ) void {
        result.* = partition.scan(limit, 0, alloc_) catch |e| {
            err_out.* = e;
            return;
        };
    }

    // ─── stats ───────────────────────────────────────────────────────────

    /// Return per-partition statistics.
    /// Return per-partition statistics.
    pub fn partitionStats(self: *PartitionedCollection) ![]PartitionStat {
        const stats = try self.alloc.alloc(PartitionStat, self.n_partitions);
        for (0..self.n_partitions) |i| {
            const p = self.partitions[i];
            // Scan with a very high limit to count docs
            const sr = p.scan(std.math.maxInt(u32), 0, self.alloc) catch {
                stats[i] = .{ .partition_id = @intCast(i), .doc_count = 0, .page_count = 0 };
                continue;
            };
            defer sr.deinit();
            stats[i] = .{
                .partition_id = @intCast(i),
                .doc_count = sr.docs.len,
                .page_count = p.pf.next_alloc.load(.monotonic),
            };
        }
        return stats;
    }

    /// Set the range spec for range-based partitioning.
    pub fn setRangeSpec(self: *PartitionedCollection, spec: RangeSpec) void {
        self.range_spec = spec;
    }

    // ─── helpers ─────────────────────────────────────────────────────────

    fn docIdLessThan(_: void, a: Doc, b: Doc) bool {
        return a.header.doc_id < b.header.doc_id;
    }
};

// ═══════════════════════════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════════════════════════

test "hash partition routing distributes keys" {
    // Verify that fnv1a-based routing maps different keys to partitions
    const n: u16 = 4;
    var counts = [_]u32{0} ** 4;

    const keys = [_][]const u8{ "user:1", "user:2", "user:3", "user:4", "user:5", "user:6", "user:7", "user:8" };
    for (keys) |key| {
        const h = doc_mod.fnv1a(key);
        const idx: usize = @intCast(h % n);
        counts[idx] += 1;
    }

    // At least one key should land in a non-zero partition (distribution check)
    var any_nonzero = false;
    for (counts[1..]) |c| {
        if (c > 0) any_nonzero = true;
    }
    try std.testing.expect(any_nonzero);
}

test "range routing: binary search boundaries" {
    // Simulate range routing with boundaries
    const boundaries = [_]u64{ 100, 200, 300 }; // 4 partitions: [0,100), [100,200), [200,300), [300,..)
    const spec = RangeSpec{ .boundaries = &boundaries };

    // Build a fake PartitionedCollection just for routing
    var pc: PartitionedCollection = undefined;
    pc.strategy = .range;
    pc.range_spec = spec;
    pc.n_partitions = 4;

    // key_hash < 100 -> partition 0
    try std.testing.expectEqual(@as(u16, 0), pc.routeByHash(50));
    // key_hash == 100 -> partition 1 (boundary is <=)
    try std.testing.expectEqual(@as(u16, 1), pc.routeByHash(100));
    // key_hash == 150 -> partition 1
    try std.testing.expectEqual(@as(u16, 1), pc.routeByHash(150));
    // key_hash == 200 -> partition 2
    try std.testing.expectEqual(@as(u16, 2), pc.routeByHash(200));
    // key_hash == 250 -> partition 2
    try std.testing.expectEqual(@as(u16, 2), pc.routeByHash(250));
    // key_hash == 300 -> partition 3
    try std.testing.expectEqual(@as(u16, 3), pc.routeByHash(300));
    // key_hash == 999 -> partition 3
    try std.testing.expectEqual(@as(u16, 3), pc.routeByHash(999));
}

test "hash routing consistency" {
    // Same key always routes to the same partition
    var pc: PartitionedCollection = undefined;
    pc.strategy = .hash;
    pc.n_partitions = 8;
    pc.range_spec = null;

    const key = "consistent-key";
    const h = doc_mod.fnv1a(key);
    const expected: u16 = @intCast(h % 8);

    // Route 100 times — must always return same index
    for (0..100) |_| {
        try std.testing.expectEqual(expected, pc.routeByHash(h));
    }
}

test "range routing: empty boundaries means single partition" {
    const boundaries = [_]u64{};
    const spec = RangeSpec{ .boundaries = &boundaries };

    var pc: PartitionedCollection = undefined;
    pc.strategy = .range;
    pc.range_spec = spec;
    pc.n_partitions = 1;

    // All hashes go to partition 0
    try std.testing.expectEqual(@as(u16, 0), pc.routeByHash(0));
    try std.testing.expectEqual(@as(u16, 0), pc.routeByHash(std.math.maxInt(u64)));
}

test "range routing: edge cases at boundaries" {
    const boundaries = [_]u64{ 0, std.math.maxInt(u64) };
    const spec = RangeSpec{ .boundaries = &boundaries };

    var pc: PartitionedCollection = undefined;
    pc.strategy = .range;
    pc.range_spec = spec;
    pc.n_partitions = 3;

    // key_hash == 0 -> boundary[0] <= 0, so lo=1 -> partition 1
    try std.testing.expectEqual(@as(u16, 1), pc.routeByHash(0));
    // key_hash == maxInt -> boundary[1] <= maxInt, so lo=2 -> partition 2
    try std.testing.expectEqual(@as(u16, 2), pc.routeByHash(std.math.maxInt(u64)));
}
