const std = @import("std");
const btree = @import("btree.zig");
const compat = @import("compat");
const runtime = @import("runtime");
const BTreeEntry = btree.BTreeEntry;

// ─── BloomFilter ─────────────────────────────────────────────────────────────

pub const BloomFilter = struct {
    bits: []u8,
    n_hashes: u8,

    const BITS_PER_ITEM = 10; // ~1% false positive rate
    const DEFAULT_HASHES = 7; // optimal k for 10 bits/item

    pub fn init(alloc: std.mem.Allocator, expected_items: u32) !BloomFilter {
        const n_bits: usize = @as(usize, expected_items) * BITS_PER_ITEM;
        const n_bytes = @max((n_bits + 7) / 8, 1);
        const bits = try alloc.alloc(u8, n_bytes);
        @memset(bits, 0);
        return .{
            .bits = bits,
            .n_hashes = DEFAULT_HASHES,
        };
    }

    pub fn deinit(self: *BloomFilter, alloc: std.mem.Allocator) void {
        alloc.free(self.bits);
        self.bits = &.{};
    }

    /// Derive two independent 64-bit hashes from key_hash using murmur-style mixing.
    fn hashes(key_hash: u64) struct { h1: u64, h2: u64 } {
        const h1 = key_hash;
        // Mix to produce an independent h2 (splitmix64 finalizer).
        var x = key_hash *% 0xbf58476d1ce4e5b9;
        x ^= x >> 31;
        x *%= 0x94d049bb133111eb;
        x ^= x >> 31;
        return .{ .h1 = h1, .h2 = x | 1 }; // ensure h2 is odd (non-zero)
    }

    pub fn add(self: *BloomFilter, key_hash: u64) void {
        const n_bits: u64 = @as(u64, self.bits.len) * 8;
        if (n_bits == 0) return;
        const h = hashes(key_hash);
        var i: u8 = 0;
        while (i < self.n_hashes) : (i += 1) {
            const bit = (h.h1 +% @as(u64, i) *% h.h2) % n_bits;
            self.bits[@intCast(bit / 8)] |= @as(u8, 1) << @intCast(bit % 8);
        }
    }

    pub fn mayContain(self: *const BloomFilter, key_hash: u64) bool {
        const n_bits: u64 = @as(u64, self.bits.len) * 8;
        if (n_bits == 0) return true;
        const h = hashes(key_hash);
        var i: u8 = 0;
        while (i < self.n_hashes) : (i += 1) {
            const bit = (h.h1 +% @as(u64, i) *% h.h2) % n_bits;
            if (self.bits[@intCast(bit / 8)] & (@as(u8, 1) << @intCast(bit % 8)) == 0)
                return false;
        }
        return true;
    }
};

// ─── KVEntry ─────────────────────────────────────────────────────────────────

pub const KVEntry = struct {
    key_hash: u64,
    value: EntryValue,

    pub const EntryValue = union(enum) {
        live: BTreeEntry,
        tombstone: void,
    };

    fn orderByKey(_: void, a: KVEntry, b: KVEntry) bool {
        return a.key_hash < b.key_hash;
    }
};

// ─── MemTable ────────────────────────────────────────────────────────────────

pub const MemTable = struct {
    /// key_hash → EntryValue. O(1) put/get/delete. Order is materialized
    /// once per flush via `sortedSnapshot`, so the hot ingest path pays
    /// no per-op sort cost.
    map: std.AutoHashMapUnmanaged(u64, KVEntry.EntryValue),
    size_bytes: usize,
    alloc: std.mem.Allocator,

    const ENTRY_OVERHEAD = @sizeOf(u64) + @sizeOf(KVEntry.EntryValue);

    pub fn init(alloc: std.mem.Allocator) MemTable {
        return .{
            .map = .empty,
            .size_bytes = 0,
            .alloc = alloc,
        };
    }

    pub fn deinit(self: *MemTable) void {
        self.map.deinit(self.alloc);
        self.size_bytes = 0;
    }

    pub fn put(self: *MemTable, key_hash: u64, entry: BTreeEntry) !void {
        const gop = try self.map.getOrPut(self.alloc, key_hash);
        if (!gop.found_existing) self.size_bytes += ENTRY_OVERHEAD;
        gop.value_ptr.* = .{ .live = entry };
    }

    pub fn get(self: *const MemTable, key_hash: u64) ?BTreeEntry {
        const v = self.map.get(key_hash) orelse return null;
        return switch (v) {
            .live => |e| e,
            .tombstone => null,
        };
    }

    /// Probe for a key. Returns the raw `EntryValue` (live OR tombstone) if
    /// present in this memtable, or null if the key is not here at all.
    /// Callers that need to stop descending to SSTables on tombstone use
    /// this instead of `get`.
    pub fn probe(self: *const MemTable, key_hash: u64) ?KVEntry.EntryValue {
        return self.map.get(key_hash);
    }

    pub fn delete(self: *MemTable, key_hash: u64) !void {
        const gop = try self.map.getOrPut(self.alloc, key_hash);
        if (!gop.found_existing) self.size_bytes += ENTRY_OVERHEAD;
        gop.value_ptr.* = .tombstone;
    }

    pub fn isEmpty(self: *const MemTable) bool {
        return self.map.count() == 0;
    }

    pub fn count(self: *const MemTable) usize {
        return self.map.count();
    }

    pub fn isFull(self: *const MemTable) bool {
        return self.size_bytes >= LSMTree.MEMTABLE_SIZE;
    }

    /// Build a sorted-by-key_hash snapshot of all entries. Caller owns the
    /// returned slice and must free it via `alloc`. Used on flush — the
    /// O(n log n) sort cost is paid once per memtable rotation, not per
    /// put.
    pub fn sortedSnapshot(self: *const MemTable, alloc: std.mem.Allocator) ![]KVEntry {
        const n = self.map.count();
        var out = try alloc.alloc(KVEntry, n);
        errdefer alloc.free(out);
        var it = self.map.iterator();
        var i: usize = 0;
        while (it.next()) |e| : (i += 1) {
            out[i] = .{ .key_hash = e.key_ptr.*, .value = e.value_ptr.* };
        }
        std.mem.sort(KVEntry, out, {}, KVEntry.orderByKey);
        return out;
    }

    pub const Iterator = struct {
        items: []const KVEntry,
        pos: usize,
        owned_alloc: ?std.mem.Allocator,

        pub fn next(self: *Iterator) ?KVEntry {
            if (self.pos >= self.items.len) return null;
            const e = self.items[self.pos];
            self.pos += 1;
            return e;
        }

        pub fn deinit(self: *Iterator) void {
            if (self.owned_alloc) |a| a.free(self.items);
        }
    };

    /// Yields entries in ascending key_hash order. The iterator owns an
    /// allocated sorted snapshot; callers must call `deinit` on it.
    pub fn iterator(self: *const MemTable, alloc: std.mem.Allocator) !Iterator {
        const snap = try self.sortedSnapshot(alloc);
        return .{ .items = snap, .pos = 0, .owned_alloc = alloc };
    }
};

// ─── SSTable ─────────────────────────────────────────────────────────────────

pub const SSTable = struct {
    level: u8,
    id: u32,
    min_key: u64,
    max_key: u64,
    entry_count: u32,
    bloom: BloomFilter,
    data_path: [256]u8,
    index_path: [256]u8,
    data_path_len: usize,
    index_path_len: usize,

    // On-disk sizes.
    const LIVE_ENTRY_SIZE: usize = 8 + 1 + BTreeEntry.size; // key_hash + flags + entry (33)
    const TOMB_ENTRY_SIZE: usize = 8 + 1; // key_hash + flags (9)
    const INDEX_ENTRY_SIZE: usize = 8 + 8; // key_hash + data_offset (16)
    const FOOTER_SIZE: usize = 4 + 8 + 8 + 8; // entry_count + min_key + max_key + bloom_offset (28)
    const INDEX_INTERVAL: usize = 64;

    const FLAG_LIVE: u8 = 0x01;
    const FLAG_TOMBSTONE: u8 = 0x00;

    /// Write a u64 in little-endian to a file.
    fn writeU64(file: std.Io.File, val: u64) !void {
        var buf: [8]u8 = undefined;
        std.mem.writeInt(u64, &buf, val, .little);
        try file.writeStreamingAll(runtime.io, &buf);
    }

    /// Write a u32 in little-endian to a file.
    fn writeU32(file: std.Io.File, val: u32) !void {
        var buf: [4]u8 = undefined;
        std.mem.writeInt(u32, &buf, val, .little);
        try file.writeStreamingAll(runtime.io, &buf);
    }

    /// Read a u64 in little-endian from a file.
    fn readU64(file: std.Io.File) !u64 {
        var buf: [8]u8 = undefined;
        var bufs = [_][]u8{&buf}; const n = try file.readStreaming(runtime.io, &bufs);
        if (n < 8) return error.UnexpectedEof;
        return std.mem.readInt(u64, &buf, .little);
    }

    /// Read a u32 in little-endian from a file.
    fn readU32(file: std.Io.File) !u32 {
        var buf: [4]u8 = undefined;
        var bufs = [_][]u8{&buf}; const n = try file.readStreaming(runtime.io, &bufs);
        if (n < 4) return error.UnexpectedEof;
        return std.mem.readInt(u32, &buf, .little);
    }

    /// Read a single byte from a file.
    fn readByte(file: std.Io.File) !u8 {
        var buf: [1]u8 = undefined;
        var bufs = [_][]u8{&buf}; const n = try file.readStreaming(runtime.io, &bufs);
        if (n < 1) return error.UnexpectedEof;
        return buf[0];
    }

    /// Create an SSTable from a sorted slice of KVEntry.
    pub fn create(entries: []const KVEntry, level: u8, id: u32, data_dir: []const u8) !SSTable {
        var sst: SSTable = undefined;
        sst.level = level;
        sst.id = id;
        sst.entry_count = @intCast(entries.len);
        sst.min_key = if (entries.len > 0) entries[0].key_hash else 0;
        sst.max_key = if (entries.len > 0) entries[entries.len - 1].key_hash else 0;

        // Build paths.
        const data_fmt = "{s}/sst_{d}_{d}.data";
        const index_fmt = "{s}/sst_{d}_{d}.idx";

        sst.data_path = std.mem.zeroes([256]u8);
        sst.index_path = std.mem.zeroes([256]u8);

        const dp = std.fmt.bufPrint(&sst.data_path, data_fmt, .{ data_dir, level, id }) catch
            return error.PathTooLong;
        sst.data_path_len = dp.len;

        const ip = std.fmt.bufPrint(&sst.index_path, index_fmt, .{ data_dir, level, id }) catch
            return error.PathTooLong;
        sst.index_path_len = ip.len;

        // Build bloom filter.
        sst.bloom = try BloomFilter.init(std.heap.page_allocator, sst.entry_count);
        for (entries) |e| {
            sst.bloom.add(e.key_hash);
        }

        // Write data file.
        const data_file = try compat.fs.cwdCreateFile(dp, .{});
        defer data_file.close(runtime.io);

        // Write index file.
        const idx_file = try compat.fs.cwdCreateFile(ip, .{});
        defer idx_file.close(runtime.io);

        var data_offset: u64 = 0;
        for (entries, 0..) |e, i| {
            // Sparse index every INDEX_INTERVAL entries.
            if (i % INDEX_INTERVAL == 0) {
                try writeU64(idx_file, e.key_hash);
                try writeU64(idx_file, data_offset);
            }

            // Write key_hash.
            try writeU64(data_file, e.key_hash);

            switch (e.value) {
                .live => |bte| {
                    try data_file.writeStreamingAll(runtime.io, &[_]u8{FLAG_LIVE});
                    try data_file.writeStreamingAll(runtime.io, std.mem.asBytes(&bte));
                    data_offset += LIVE_ENTRY_SIZE;
                },
                .tombstone => {
                    try data_file.writeStreamingAll(runtime.io, &[_]u8{FLAG_TOMBSTONE});
                    data_offset += TOMB_ENTRY_SIZE;
                },
            }
        }

        // Write bloom filter data to end of data file.
        const bloom_offset = data_offset;
        try data_file.writeStreamingAll(runtime.io, sst.bloom.bits);
        data_offset += sst.bloom.bits.len;

        // Footer.
        try writeU32(data_file, sst.entry_count);
        try writeU64(data_file, sst.min_key);
        try writeU64(data_file, sst.max_key);
        try writeU64(data_file, bloom_offset);

        return sst;
    }

    /// Point lookup in the SSTable using sparse index + scan.
    pub fn get(self: *const SSTable, key_hash: u64) !?BTreeEntry {
        // Quick bloom filter check.
        if (!self.bloom.mayContain(key_hash)) return null;

        // Range check.
        if (key_hash < self.min_key or key_hash > self.max_key) return null;

        const data_file = try compat.fs.cwdOpenFile(
            self.data_path[0..self.data_path_len],
            .{},
        );
        defer data_file.close(runtime.io);

        // Load sparse index to find starting offset.
        const idx_file = try compat.fs.cwdOpenFile(
            self.index_path[0..self.index_path_len],
            .{},
        );
        defer idx_file.close(runtime.io);

        const idx_stat = .{ .size = try compat.fs.fileSize(idx_file.handle) };
        const n_index_entries = idx_stat.size / INDEX_ENTRY_SIZE;

        // Binary search the sparse index.
        var scan_offset: u64 = 0;
        if (n_index_entries > 0) {
            var lo: u64 = 0;
            var hi: u64 = n_index_entries;
            var best_offset: u64 = 0;
            while (lo < hi) {
                const mid = lo + (hi - lo) / 2;
                compat.fs.fileSeekTo(idx_file, mid * INDEX_ENTRY_SIZE);
                var idx_buf: [INDEX_ENTRY_SIZE]u8 = undefined;
                const read_n = try compat.fs.fileReadAll(idx_file, &idx_buf);
                if (read_n < INDEX_ENTRY_SIZE) break;
                const idx_key = std.mem.readInt(u64, idx_buf[0..8], .little);
                const idx_off = std.mem.readInt(u64, idx_buf[8..16], .little);
                if (idx_key <= key_hash) {
                    best_offset = idx_off;
                    lo = mid + 1;
                } else {
                    hi = mid;
                }
            }
            scan_offset = best_offset;
        }

        // Linear scan from scan_offset.
        compat.fs.fileSeekTo(data_file, scan_offset);
        var scanned: u32 = 0;
        while (scanned < self.entry_count) : (scanned += 1) {
            const k = readU64(data_file) catch return null;
            const flag = readByte(data_file) catch return null;
            if (flag == FLAG_LIVE) {
                var entry_bytes: [BTreeEntry.size]u8 = undefined;
                const n_read = compat.fs.fileReadAll(data_file, &entry_bytes) catch return null;
                if (n_read < BTreeEntry.size) return null;
                if (k == key_hash) {
                    return std.mem.bytesToValue(BTreeEntry, &entry_bytes);
                }
            } else {
                // Tombstone — no payload.
                if (k == key_hash) return null;
            }
            // Past our key in sorted order — not found.
            if (k > key_hash) return null;
        }
        return null;
    }

    pub const Iterator = struct {
        file: std.Io.File,
        remaining: u32,

        pub fn next(self: *Iterator) ?KVEntry {
            if (self.remaining == 0) return null;
            self.remaining -= 1;
            const k = readU64(self.file) catch return null;
            const flag = readByte(self.file) catch return null;
            if (flag == FLAG_LIVE) {
                var entry_bytes: [BTreeEntry.size]u8 = undefined;
                const n_read = compat.fs.fileReadAll(self.file, &entry_bytes) catch return null;
                if (n_read < BTreeEntry.size) return null;
                return .{
                    .key_hash = k,
                    .value = .{ .live = std.mem.bytesToValue(BTreeEntry, &entry_bytes) },
                };
            } else {
                return .{
                    .key_hash = k,
                    .value = .tombstone,
                };
            }
        }

        pub fn deinit(self: *Iterator) void {
            self.file.close(runtime.io);
        }
    };

    pub fn iterator(self: *const SSTable, alloc: std.mem.Allocator) !Iterator {
        _ = alloc;
        const f = try compat.fs.cwdOpenFile(
            self.data_path[0..self.data_path_len],
            .{},
        );
        return .{
            .file = f,
            .remaining = self.entry_count,
        };
    }

    pub fn close(self: *SSTable) void {
        self.bloom.deinit(std.heap.page_allocator);
    }
};

// ─── LSMTree ─────────────────────────────────────────────────────────────────

pub const LSMTree = struct {
    active_mem: MemTable,
    immutable_mem: ?MemTable,
    levels: [MAX_LEVELS]std.ArrayList(SSTable),
    next_sst_id: u32,
    data_dir: [256]u8,
    data_dir_len: usize,
    alloc: std.mem.Allocator,
    flush_mu: std.Io.Mutex,

    pub const MAX_LEVELS = 7;
    pub const MEMTABLE_SIZE = 4 * 1024 * 1024; // 4MB
    pub const L0_COMPACTION_TRIGGER = 4;
    pub const LN_COMPACTION_TRIGGER = 10;

    pub fn init(alloc: std.mem.Allocator, data_dir: []const u8) !LSMTree {
        var lsm: LSMTree = undefined;
        lsm.active_mem = MemTable.init(alloc);
        lsm.immutable_mem = null;
        lsm.alloc = alloc;
        lsm.next_sst_id = 0;
        lsm.flush_mu = .init;

        lsm.data_dir = std.mem.zeroes([256]u8);
        if (data_dir.len > 256) return error.PathTooLong;
        @memcpy(lsm.data_dir[0..data_dir.len], data_dir);
        lsm.data_dir_len = data_dir.len;

        for (&lsm.levels) |*lvl| {
            lvl.* = .empty;
        }

        // Ensure data directory exists.
        compat.fs.cwdMakeDir(data_dir) catch |err| switch (err) {
            error.PathAlreadyExists => {},
            else => return err,
        };

        return lsm;
    }

    pub fn deinit(self: *LSMTree) void {
        self.active_mem.deinit();
        if (self.immutable_mem) |*imm| imm.deinit();
        for (&self.levels) |*lvl| {
            for (lvl.items) |*sst| sst.close();
            lvl.deinit(self.alloc);
        }
    }

    pub fn put(self: *LSMTree, key_hash: u64, entry: BTreeEntry) !void {
        try self.active_mem.put(key_hash, entry);

        if (self.active_mem.isFull()) {
            try self.flush();
        }
    }

    pub fn get(self: *const LSMTree, key_hash: u64) ?BTreeEntry {
        // 1. Active memtable — hit OR tombstone stops the descent.
        if (self.active_mem.probe(key_hash)) |v| {
            return switch (v) { .live => |e| e, .tombstone => null };
        }
        // 2. Immutable memtable (rotating into L0).
        if (self.immutable_mem) |imm| {
            if (imm.probe(key_hash)) |v| {
                return switch (v) { .live => |e| e, .tombstone => null };
            }
        }
        // 3. SSTables level by level, newest first.
        for (self.levels) |lvl| {
            // Iterate in reverse (newest SSTable first within a level).
            var i = lvl.items.len;
            while (i > 0) {
                i -= 1;
                const sst = &lvl.items[i];
                if (!sst.bloom.mayContain(key_hash)) continue;
                if (key_hash < sst.min_key or key_hash > sst.max_key) continue;
                if (sst.get(key_hash) catch null) |bte| return bte;
            }
        }
        return null;
    }

    pub fn delete(self: *LSMTree, key_hash: u64) !void {
        try self.active_mem.delete(key_hash);
    }

    /// Flush the active memtable to an L0 SSTable.
    pub fn flush(self: *LSMTree) !void {
        self.flush_mu.lockUncancelable(runtime.io);
        defer self.flush_mu.unlock(runtime.io);

        if (self.active_mem.isEmpty()) return;

        // Rotate: move active → immutable.
        var old = self.active_mem;
        self.active_mem = MemTable.init(self.alloc);

        defer {
            old.deinit();
            self.immutable_mem = null;
        }
        self.immutable_mem = old;

        // Materialize a sorted snapshot once for the SSTable writer — the
        // memtable is unsorted (hashmap), but SSTable.create needs entries
        // in ascending key_hash order for the sparse index + scan path.
        const snap = try old.sortedSnapshot(self.alloc);
        defer self.alloc.free(snap);

        const id = self.next_sst_id;
        self.next_sst_id += 1;
        const sst = try SSTable.create(
            snap,
            0,
            id,
            self.data_dir[0..self.data_dir_len],
        );
        try self.levels[0].append(self.alloc, sst);

        // Trigger L0 compaction if needed.
        if (self.levels[0].items.len >= L0_COMPACTION_TRIGGER) {
            try self.compact(0);
        }
    }

    /// Merge all SSTables at `level` into one SSTable at `level+1`.
    pub fn compact(self: *LSMTree, level: u8) !void {
        if (level >= MAX_LEVELS - 1) return;
        const src = &self.levels[level];
        if (src.items.len == 0) return;

        // Collect all entries via k-way merge (simple: gather + sort + dedup).
        var all: std.ArrayList(KVEntry) = .empty;
        defer all.deinit(self.alloc);

        // Read entries from all SSTables in this level (oldest first).
        for (src.items) |*sst| {
            var it = try sst.iterator(self.alloc);
            defer it.deinit();
            while (it.next()) |e| {
                try all.append(self.alloc, e);
            }
        }

        // Sort by key.
        std.mem.sort(KVEntry, all.items, {}, KVEntry.orderByKey);

        // Deduplicate: for duplicate keys keep the last one (latest write wins).
        // Since we iterated SSTables oldest-first and appended in order, for
        // duplicate keys the *last* occurrence is the newest.
        var deduped: std.ArrayList(KVEntry) = .empty;
        defer deduped.deinit(self.alloc);
        {
            var i: usize = 0;
            while (i < all.items.len) {
                var j = i + 1;
                while (j < all.items.len and all.items[j].key_hash == all.items[i].key_hash) {
                    j += 1;
                }
                // j-1 is the newest entry for this key.
                const entry = all.items[j - 1];
                // Drop tombstones when compacting to deeper levels (> L1).
                if (level > 0 or entry.value != .tombstone) {
                    try deduped.append(self.alloc, entry);
                }
                i = j;
            }
        }

        // Create new SSTable at level+1.
        if (deduped.items.len > 0) {
            const id = self.next_sst_id;
            self.next_sst_id += 1;
            const new_sst = try SSTable.create(
                deduped.items,
                level + 1,
                id,
                self.data_dir[0..self.data_dir_len],
            );
            try self.levels[level + 1].append(self.alloc, new_sst);
        }

        // Close and remove old SSTables at this level.
        for (src.items) |*sst| {
            // Delete files.
            compat.fs.cwdDeleteFile(sst.data_path[0..sst.data_path_len]) catch {};
            compat.fs.cwdDeleteFile(sst.index_path[0..sst.index_path_len]) catch {};
            sst.close();
        }
        src.clearRetainingCapacity();

        // Trigger next level compaction if needed.
        if (self.levels[level + 1].items.len >= LN_COMPACTION_TRIGGER) {
            try self.compact(level + 1);
        }
    }
};

// ─── Tests ───────────────────────────────────────────────────────────────────

fn testEntry(key: u64, doc: u64) BTreeEntry {
    return .{ .key_hash = key, .doc_id = doc, .page_no = 0, .page_off = 0 };
}

test "MemTable put/get/delete" {
    var mt = MemTable.init(std.testing.allocator);
    defer mt.deinit();

    try mt.put(100, testEntry(100, 1));
    try mt.put(50, testEntry(50, 2));
    try mt.put(200, testEntry(200, 3));

    // Get existing keys.
    try std.testing.expectEqual(@as(u64, 1), mt.get(100).?.doc_id);
    try std.testing.expectEqual(@as(u64, 2), mt.get(50).?.doc_id);
    try std.testing.expectEqual(@as(u64, 3), mt.get(200).?.doc_id);

    // Get non-existent key.
    try std.testing.expect(mt.get(999) == null);

    // Update existing key.
    try mt.put(100, testEntry(100, 42));
    try std.testing.expectEqual(@as(u64, 42), mt.get(100).?.doc_id);

    // Delete.
    try mt.delete(100);
    try std.testing.expect(mt.get(100) == null);

    // Delete non-existent key (creates tombstone).
    try mt.delete(777);
    try std.testing.expect(mt.get(777) == null);
}
test "MemTable isFull threshold" {
    var mt = MemTable.init(std.testing.allocator);
    defer mt.deinit();

    try std.testing.expect(!mt.isFull());

    // Fill until full: ceil(4MB / sizeof(KVEntry)) entries guarantees >= 4MB.
    const entry_size = @sizeOf(KVEntry);
    const target = (LSMTree.MEMTABLE_SIZE + entry_size - 1) / entry_size;
    var i: u64 = 0;
    while (i < target) : (i += 1) {
        try mt.put(i, testEntry(i, i));
    }
    try std.testing.expect(mt.isFull());
}
test "MemTable iterator order" {
    var mt = MemTable.init(std.testing.allocator);
    defer mt.deinit();

    try mt.put(300, testEntry(300, 3));
    try mt.put(100, testEntry(100, 1));
    try mt.put(200, testEntry(200, 2));

    var it = try mt.iterator(std.testing.allocator);
    defer it.deinit();
    const first = it.next().?;
    try std.testing.expectEqual(@as(u64, 100), first.key_hash);
    const second = it.next().?;
    try std.testing.expectEqual(@as(u64, 200), second.key_hash);
    const third = it.next().?;
    try std.testing.expectEqual(@as(u64, 300), third.key_hash);
    try std.testing.expect(it.next() == null);
}

test "BloomFilter add/mayContain" {
    var bf = try BloomFilter.init(std.testing.allocator, 1000);
    defer bf.deinit(std.testing.allocator);

    // Add some keys.
    var i: u64 = 0;
    while (i < 1000) : (i += 1) {
        bf.add(i * 17);
    }

    // All added keys must be found.
    i = 0;
    while (i < 1000) : (i += 1) {
        try std.testing.expect(bf.mayContain(i * 17));
    }

    // Check false positive rate: test 10000 keys that were never added.
    var false_positives: u32 = 0;
    var k: u64 = 1_000_000;
    while (k < 1_010_000) : (k += 1) {
        if (bf.mayContain(k)) false_positives += 1;
    }
    // With 10 bits/item and 7 hashes, expect ~1% FP rate. Allow up to 5%.
    try std.testing.expect(false_positives < 500);
}

test "SSTable create and read back" {
    // Use a tmp directory.
    var tmp_dir = std.testing.tmpDir(.{});
    defer tmp_dir.cleanup();
    const tmp_path = tmp_dir.dir.realpathAlloc(std.testing.allocator, ".") catch unreachable;
    defer std.testing.allocator.free(tmp_path);

    // Create sorted entries.
    var entries: [5]KVEntry = undefined;
    for (&entries, 0..) |*e, idx| {
        const k: u64 = (idx + 1) * 100;
        e.* = .{ .key_hash = k, .value = .{ .live = testEntry(k, @intCast(idx + 1)) } };
    }

    var sst = try SSTable.create(&entries, 0, 0, tmp_path);
    defer sst.close();

    // Read back each key.
    for (1..6) |idx| {
        const k: u64 = idx * 100;
        const found = try sst.get(k);
        try std.testing.expect(found != null);
        try std.testing.expectEqual(@as(u64, idx), found.?.doc_id);
    }

    // Non-existent key.
    const miss = try sst.get(999);
    try std.testing.expect(miss == null);
}

test "LSMTree put/get across memtable and SSTable" {
    var tmp_dir = std.testing.tmpDir(.{});
    defer tmp_dir.cleanup();
    const tmp_path = tmp_dir.dir.realpathAlloc(std.testing.allocator, ".") catch unreachable;
    defer std.testing.allocator.free(tmp_path);

    var lsm = try LSMTree.init(std.testing.allocator, tmp_path);
    defer lsm.deinit();

    // Put a few entries.
    try lsm.put(10, testEntry(10, 1));
    try lsm.put(20, testEntry(20, 2));
    try lsm.put(30, testEntry(30, 3));

    // Get from active memtable.
    try std.testing.expectEqual(@as(u64, 1), lsm.get(10).?.doc_id);
    try std.testing.expectEqual(@as(u64, 2), lsm.get(20).?.doc_id);
    try std.testing.expectEqual(@as(u64, 3), lsm.get(30).?.doc_id);

    // Flush to L0.
    try lsm.flush();

    // Get from SSTable (memtable is now empty).
    try std.testing.expectEqual(@as(u64, 1), lsm.get(10).?.doc_id);
    try std.testing.expectEqual(@as(u64, 2), lsm.get(20).?.doc_id);
    try std.testing.expectEqual(@as(u64, 3), lsm.get(30).?.doc_id);

    // Put more and verify we see the union.
    try lsm.put(40, testEntry(40, 4));
    try std.testing.expectEqual(@as(u64, 4), lsm.get(40).?.doc_id);
    try std.testing.expectEqual(@as(u64, 1), lsm.get(10).?.doc_id);
}

test "LSMTree flush" {
    var tmp_dir = std.testing.tmpDir(.{});
    defer tmp_dir.cleanup();
    const tmp_path = tmp_dir.dir.realpathAlloc(std.testing.allocator, ".") catch unreachable;
    defer std.testing.allocator.free(tmp_path);

    var lsm = try LSMTree.init(std.testing.allocator, tmp_path);
    defer lsm.deinit();

    try lsm.put(1, testEntry(1, 10));
    try lsm.put(2, testEntry(2, 20));
    try lsm.flush();

    // Active memtable should be empty after flush.
    try std.testing.expectEqual(@as(usize, 0), lsm.active_mem.count());
    // L0 should have one SSTable.
    try std.testing.expectEqual(@as(usize, 1), lsm.levels[0].items.len);
    // Data still readable.
    try std.testing.expectEqual(@as(u64, 10), lsm.get(1).?.doc_id);
}
test "Tombstone handling" {
    var tmp_dir = std.testing.tmpDir(.{});
    defer tmp_dir.cleanup();
    const tmp_path = tmp_dir.dir.realpathAlloc(std.testing.allocator, ".") catch unreachable;
    defer std.testing.allocator.free(tmp_path);

    var lsm = try LSMTree.init(std.testing.allocator, tmp_path);
    defer lsm.deinit();

    // Write, flush, then delete in memtable — should shadow SSTable.
    try lsm.put(42, testEntry(42, 99));
    try lsm.flush();

    // Verify it's on disk.
    try std.testing.expectEqual(@as(u64, 99), lsm.get(42).?.doc_id);

    // Delete from active memtable — tombstone shadows SSTable entry.
    try lsm.delete(42);
    try std.testing.expect(lsm.get(42) == null);

    // Write a new value — should be visible again.
    try lsm.put(42, testEntry(42, 100));
    try std.testing.expectEqual(@as(u64, 100), lsm.get(42).?.doc_id);
}
