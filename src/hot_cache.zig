/// TurboDB — Hot Page Cache (CLOCK eviction, direct-mapped with linear probing)
/// Sits in front of mmap'd page reads to accelerate GET point lookups.
/// No allocation — fixed-size array, entirely struct-embedded.
const std = @import("std");

pub const HotCache = struct {
    entries: [CACHE_SIZE]CacheEntry,
    hand: u32,
    hits: std.atomic.Value(u64),
    misses: std.atomic.Value(u64),

    const CACHE_SIZE: u32 = 16384; // power of 2 for fast modulo
    const MASK: u32 = CACHE_SIZE - 1;
    const PROBE_LIMIT: u32 = 4;

    const CacheEntry = struct {
        key_hash: u64, // 0 = empty
        page_no: u32,
        page_off: u16,
        referenced: bool, // CLOCK bit
        valid: bool,
    };

    const empty_entry = CacheEntry{
        .key_hash = 0,
        .page_no = 0,
        .page_off = 0,
        .referenced = false,
        .valid = false,
    };

    pub fn init() HotCache {
        var self: HotCache = undefined;
        @memset(&self.entries, empty_entry);
        self.hand = 0;
        self.hits = std.atomic.Value(u64).init(0);
        self.misses = std.atomic.Value(u64).init(0);
        return self;
    }

    pub fn lookup(self: *HotCache, key_hash: u64) ?struct { page_no: u32, page_off: u16 } {
        if (key_hash == 0) return null; // 0 is our sentinel
        const base = @as(u32, @truncate(key_hash)) & MASK;
        var i: u32 = 0;
        while (i < PROBE_LIMIT) : (i += 1) {
            const slot = (base +% i) & MASK;
            const e = &self.entries[slot];
            if (e.key_hash == key_hash and e.valid) {
                e.referenced = true;
                _ = self.hits.fetchAdd(1, .monotonic);
                return .{ .page_no = e.page_no, .page_off = e.page_off };
            }
            if (e.key_hash == 0) break; // empty slot = end of probe chain
        }
        _ = self.misses.fetchAdd(1, .monotonic);
        return null;
    }

    pub fn insert(self: *HotCache, key_hash: u64, page_no: u32, page_off: u16) void {
        if (key_hash == 0) return;
        const base = @as(u32, @truncate(key_hash)) & MASK;
        // Try to find existing or empty slot within probe window
        var i: u32 = 0;
        while (i < PROBE_LIMIT) : (i += 1) {
            const slot = (base +% i) & MASK;
            const e = &self.entries[slot];
            if (e.key_hash == key_hash or e.key_hash == 0 or !e.valid) {
                e.* = .{
                    .key_hash = key_hash,
                    .page_no = page_no,
                    .page_off = page_off,
                    .referenced = true,
                    .valid = true,
                };
                return;
            }
        }
        // All probe slots occupied — use CLOCK to evict one
        const slot = self.clockEvict();
        self.entries[slot] = .{
            .key_hash = key_hash,
            .page_no = page_no,
            .page_off = page_off,
            .referenced = true,
            .valid = true,
        };
    }

    pub fn invalidate(self: *HotCache, key_hash: u64) void {
        if (key_hash == 0) return;
        const base = @as(u32, @truncate(key_hash)) & MASK;
        var i: u32 = 0;
        while (i < PROBE_LIMIT) : (i += 1) {
            const slot = (base +% i) & MASK;
            const e = &self.entries[slot];
            if (e.key_hash == key_hash and e.valid) {
                e.valid = false;
                e.key_hash = 0;
                return;
            }
            if (e.key_hash == 0) return;
        }
    }

    pub fn hitRate(self: *const HotCache) f64 {
        const h = self.hits.load(.monotonic);
        const m = self.misses.load(.monotonic);
        const total = h + m;
        if (total == 0) return 0.0;
        return @as(f64, @floatFromInt(h)) / @as(f64, @floatFromInt(total));
    }

    fn clockEvict(self: *HotCache) u32 {
        // Sweep until we find a non-referenced slot
        var sweeps: u32 = 0;
        while (sweeps < CACHE_SIZE * 2) : (sweeps += 1) {
            const slot = self.hand;
            self.hand = (self.hand +% 1) & MASK;
            const e = &self.entries[slot];
            if (!e.valid) return slot;
            if (!e.referenced) {
                e.valid = false;
                e.key_hash = 0;
                return slot;
            }
            e.referenced = false; // give it a second chance
        }
        // Fallback: just evict at hand
        const slot = self.hand;
        self.hand = (self.hand +% 1) & MASK;
        self.entries[slot].valid = false;
        self.entries[slot].key_hash = 0;
        return slot;
    }
};

// ─── Tests ───────────────────────────────────────────────────────────────────

test "insert + lookup returns correct values" {
    var cache = HotCache.init();
    cache.insert(42, 10, 200);
    const result = cache.lookup(42);
    try std.testing.expect(result != null);
    try std.testing.expectEqual(@as(u32, 10), result.?.page_no);
    try std.testing.expectEqual(@as(u16, 200), result.?.page_off);
}

test "miss returns null" {
    var cache = HotCache.init();
    const result = cache.lookup(999);
    try std.testing.expect(result == null);
}

test "invalidate removes entry" {
    var cache = HotCache.init();
    cache.insert(42, 10, 200);
    cache.invalidate(42);
    const result = cache.lookup(42);
    try std.testing.expect(result == null);
}

test "hit rate tracking" {
    var cache = HotCache.init();
    cache.insert(1, 0, 0);
    _ = cache.lookup(1); // hit
    _ = cache.lookup(1); // hit
    _ = cache.lookup(2); // miss
    const rate = cache.hitRate();
    // 2 hits from lookup, plus 2 misses from lookup(2) + the init miss stats
    // Actually: lookup(1) hits twice, lookup(2) misses once => 2/(2+1) = 0.666...
    try std.testing.expect(rate > 0.6);
    try std.testing.expect(rate < 0.7);
}

test "eviction works with more than CACHE_SIZE inserts" {
    var cache = HotCache.init();
    // Insert more entries than CACHE_SIZE to force eviction
    var i: u64 = 1; // skip 0 (sentinel)
    while (i <= HotCache.CACHE_SIZE + 100) : (i += 1) {
        cache.insert(i, @truncate(i), @truncate(i));
    }
    // The most recently inserted entries should still be findable
    const result = cache.lookup(HotCache.CACHE_SIZE + 50);
    try std.testing.expect(result != null);
}
