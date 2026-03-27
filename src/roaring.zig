const std = @import("std");
const Allocator = std.mem.Allocator;
const testing = std.testing;

// ── Container types ─────────────────────────────────────────────────────────

const ARRAY_MAX: u16 = 4096;

const ArrayContainer = struct {
    values: std.ArrayListUnmanaged(u16) = .empty,

    fn deinit(self: *ArrayContainer, alloc: Allocator) void {
        self.values.deinit(alloc);
    }

    fn add(self: *ArrayContainer, alloc: Allocator, val: u16) !bool {
        // Binary search for insertion point
        const idx = binarySearch(u16, self.values.items, val);
        if (idx < self.values.items.len and self.values.items[idx] == val) {
            return false; // already present
        }
        try self.values.insert(alloc, idx, val);
        return true;
    }

    fn contains(self: *const ArrayContainer, val: u16) bool {
        const idx = binarySearch(u16, self.values.items, val);
        return idx < self.values.items.len and self.values.items[idx] == val;
    }

    fn cardinality(self: *const ArrayContainer) u32 {
        return @intCast(self.values.items.len);
    }
};

const BitmapContainer = struct {
    bits: *[1024]u64,

    fn init(alloc: Allocator) !BitmapContainer {
        const bits = try alloc.create([1024]u64);
        @memset(bits, 0);
        return .{ .bits = bits };
    }

    fn deinit(self: *BitmapContainer, alloc: Allocator) void {
        alloc.destroy(self.bits);
    }

    fn add(self: *BitmapContainer, val: u16) bool {
        const word: usize = val >> 6;
        const bit: u6 = @truncate(val);
        const mask: u64 = @as(u64, 1) << bit;
        if (self.bits[word] & mask != 0) return false;
        self.bits[word] |= mask;
        return true;
    }

    fn contains(self: *const BitmapContainer, val: u16) bool {
        const word: usize = val >> 6;
        const bit: u6 = @truncate(val);
        return (self.bits[word] & (@as(u64, 1) << bit)) != 0;
    }

    fn cardinality(self: *const BitmapContainer) u32 {
        var count: u32 = 0;
        for (self.bits) |w| {
            count += @popCount(w);
        }
        return count;
    }
};

const Container = union(enum) {
    array: ArrayContainer,
    bitmap: BitmapContainer,
};

const Chunk = struct {
    key: u16,
    container: Container,
    cached_card: u32 = 0,
};

// ── Binary search helper ────────────────────────────────────────────────────

fn binarySearch(comptime T: type, items: []const T, needle: T) usize {
    var lo: usize = 0;
    var hi: usize = items.len;
    while (lo < hi) {
        const mid = lo + (hi - lo) / 2;
        if (items[mid] < needle) {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }
    return lo;
}

fn chunkKeySearch(chunks: []const Chunk, key: u16) usize {
    var lo: usize = 0;
    var hi: usize = chunks.len;
    while (lo < hi) {
        const mid = lo + (hi - lo) / 2;
        if (chunks[mid].key < key) {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }
    return lo;
}

// ── RoaringBitmap ───────────────────────────────────────────────────────────

pub const RoaringBitmap = struct {
    chunks: std.ArrayListUnmanaged(Chunk) = .empty,

    pub fn init(_: Allocator) RoaringBitmap {
        return .{};
    }

    pub fn deinit(self: *RoaringBitmap, alloc: Allocator) void {
        for (self.chunks.items) |*chunk| {
            switch (chunk.container) {
                .array => |*a| a.deinit(alloc),
                .bitmap => |*b| b.deinit(alloc),
            }
        }
        self.chunks.deinit(alloc);
    }

    pub fn add(self: *RoaringBitmap, alloc: Allocator, value: u32) !void {
        const hi: u16 = @truncate(value >> 16);
        const lo: u16 = @truncate(value);

        const idx = chunkKeySearch(self.chunks.items, hi);

        if (idx < self.chunks.items.len and self.chunks.items[idx].key == hi) {
            // Existing chunk
            const chunk = &self.chunks.items[idx];
            switch (chunk.container) {
                .array => |*a| {
                    const added = try a.add(alloc, lo);
                    if (added) {
                        chunk.cached_card += 1;
                        // Check promotion threshold
                        if (chunk.cached_card >= ARRAY_MAX) {
                            try self.promoteArrayToBitmap(alloc, idx);
                        }
                    }
                },
                .bitmap => |*b| {
                    if (b.add(lo)) {
                        chunk.cached_card += 1;
                    }
                },
            }
        } else {
            // New chunk — start as array
            var arr = ArrayContainer{};
            _ = try arr.add(alloc, lo);
            try self.chunks.insert(alloc, idx, .{
                .key = hi,
                .container = .{ .array = arr },
                .cached_card = 1,
            });
        }
    }

    fn promoteArrayToBitmap(self: *RoaringBitmap, alloc: Allocator, idx: usize) !void {
        const chunk = &self.chunks.items[idx];
        var bmp = try BitmapContainer.init(alloc);
        const arr = &chunk.container.array;
        for (arr.values.items) |v| {
            _ = bmp.add(v);
        }
        arr.deinit(alloc);
        chunk.container = .{ .bitmap = bmp };
    }

    pub fn contains(self: *const RoaringBitmap, value: u32) bool {
        const hi: u16 = @truncate(value >> 16);
        const lo: u16 = @truncate(value);

        const idx = chunkKeySearch(self.chunks.items, hi);
        if (idx >= self.chunks.items.len or self.chunks.items[idx].key != hi) {
            return false;
        }

        return switch (self.chunks.items[idx].container) {
            .array => |a| a.contains(lo),
            .bitmap => |b| b.contains(lo),
        };
    }

    pub fn cardinality(self: *const RoaringBitmap) u64 {
        var total: u64 = 0;
        for (self.chunks.items) |chunk| {
            total += chunk.cached_card;
        }
        return total;
    }

    pub fn andWith(self: *const RoaringBitmap, other: *const RoaringBitmap, alloc: Allocator) !RoaringBitmap {
        var result = RoaringBitmap{};
        errdefer result.deinit(alloc);

        var i: usize = 0;
        var j: usize = 0;
        while (i < self.chunks.items.len and j < other.chunks.items.len) {
            const a = &self.chunks.items[i];
            const b = &other.chunks.items[j];
            if (a.key < b.key) {
                i += 1;
            } else if (a.key > b.key) {
                j += 1;
            } else {
                // Matching keys — intersect containers
                const maybe_chunk = try intersectContainers(alloc, a.key, &a.container, &b.container);
                if (maybe_chunk) |chunk| {
                    try result.chunks.append(alloc, chunk);
                }
                i += 1;
                j += 1;
            }
        }
        return result;
    }

    pub fn orWith(self: *const RoaringBitmap, other: *const RoaringBitmap, alloc: Allocator) !RoaringBitmap {
        var result = RoaringBitmap{};
        errdefer result.deinit(alloc);

        var i: usize = 0;
        var j: usize = 0;
        while (i < self.chunks.items.len and j < other.chunks.items.len) {
            const a = &self.chunks.items[i];
            const b = &other.chunks.items[j];
            if (a.key < b.key) {
                try result.chunks.append(alloc, try cloneChunk(alloc, a));
                i += 1;
            } else if (a.key > b.key) {
                try result.chunks.append(alloc, try cloneChunk(alloc, b));
                j += 1;
            } else {
                try result.chunks.append(alloc, try unionContainers(alloc, a.key, &a.container, &b.container));
                i += 1;
                j += 1;
            }
        }
        while (i < self.chunks.items.len) : (i += 1) {
            try result.chunks.append(alloc, try cloneChunk(alloc, &self.chunks.items[i]));
        }
        while (j < other.chunks.items.len) : (j += 1) {
            try result.chunks.append(alloc, try cloneChunk(alloc, &other.chunks.items[j]));
        }
        return result;
    }

    pub const Iterator = struct {
        chunks: []const Chunk,
        chunk_idx: usize = 0,
        // Array iteration state
        arr_pos: usize = 0,
        // Bitmap iteration state
        bmp_word: usize = 0,
        bmp_bits: u64 = 0,

        pub fn next(self: *Iterator) ?u32 {
            while (self.chunk_idx < self.chunks.len) {
                const chunk = &self.chunks[self.chunk_idx];
                const base: u32 = @as(u32, chunk.key) << 16;

                switch (chunk.container) {
                    .array => |a| {
                        if (self.arr_pos < a.values.items.len) {
                            const val = base | @as(u32, a.values.items[self.arr_pos]);
                            self.arr_pos += 1;
                            return val;
                        }
                    },
                    .bitmap => |b| {
                        while (self.bmp_word < 1024) {
                            if (self.bmp_bits == 0) {
                                // Load next non-zero word
                                while (self.bmp_word < 1024 and b.bits[self.bmp_word] == 0) {
                                    self.bmp_word += 1;
                                }
                                if (self.bmp_word >= 1024) break;
                                self.bmp_bits = b.bits[self.bmp_word];
                            }
                            // Extract lowest set bit
                            const bit_pos: u6 = @truncate(@ctz(self.bmp_bits));
                            self.bmp_bits &= self.bmp_bits - 1; // clear lowest bit
                            const lo: u32 = @as(u32, @intCast(self.bmp_word)) * 64 + bit_pos;
                            return base | lo;
                        }
                    },
                }
                // Move to next chunk
                self.chunk_idx += 1;
                self.arr_pos = 0;
                self.bmp_word = 0;
                self.bmp_bits = 0;
            }
            return null;
        }
    };

    pub fn iterator(self: *const RoaringBitmap) Iterator {
        return .{ .chunks = self.chunks.items };
    }

    pub fn toArray(self: *const RoaringBitmap, alloc: Allocator) ![]u32 {
        const card = self.cardinality();
        var arr = try alloc.alloc(u32, @intCast(card));
        var it = self.iterator();
        var i: usize = 0;
        while (it.next()) |v| {
            arr[i] = v;
            i += 1;
        }
        return arr;
    }
};

// ── Container intersection/union helpers ────────────────────────────────────

fn intersectContainers(alloc: Allocator, key: u16, a: *const Container, b: *const Container) !?Chunk {
    switch (a.*) {
        .bitmap => |ba| switch (b.*) {
            .bitmap => |bb| {
                // Bitmap & Bitmap — bitwise AND
                var bmp = try BitmapContainer.init(alloc);
                var card: u32 = 0;
                for (bmp.bits, ba.bits, bb.bits) |*r, wa, wb| {
                    r.* = wa & wb;
                    card += @popCount(r.*);
                }
                if (card == 0) {
                    bmp.deinit(alloc);
                    return null;
                }
                // Demote to array if sparse
                if (card < ARRAY_MAX) {
                    var arr = ArrayContainer{};
                    try arr.values.ensureTotalCapacity(alloc, card);
                    var w: usize = 0;
                    while (w < 1024) : (w += 1) {
                        var bits = bmp.bits[w];
                        while (bits != 0) {
                            const bit_pos: u6 = @truncate(@ctz(bits));
                            const val: u16 = @intCast(w * 64 + @as(usize, bit_pos));
                            arr.values.appendAssumeCapacity(val);
                            bits &= bits - 1;
                        }
                    }
                    bmp.deinit(alloc);
                    return .{ .key = key, .container = .{ .array = arr }, .cached_card = card };
                }
                return .{ .key = key, .container = .{ .bitmap = bmp }, .cached_card = card };
            },
            .array => |ab| return intersectBitmapArray(alloc, key, &ba, &ab),
        },
        .array => |aa| switch (b.*) {
            .bitmap => |bb| return intersectBitmapArray(alloc, key, &bb, &aa),
            .array => |ab| {
                // Array & Array — merge intersection
                var arr = ArrayContainer{};
                var i: usize = 0;
                var j: usize = 0;
                while (i < aa.values.items.len and j < ab.values.items.len) {
                    if (aa.values.items[i] < ab.values.items[j]) {
                        i += 1;
                    } else if (aa.values.items[i] > ab.values.items[j]) {
                        j += 1;
                    } else {
                        try arr.values.append(alloc, aa.values.items[i]);
                        i += 1;
                        j += 1;
                    }
                }
                const card: u32 = @intCast(arr.values.items.len);
                if (card == 0) {
                    arr.deinit(alloc);
                    return null;
                }
                return .{ .key = key, .container = .{ .array = arr }, .cached_card = card };
            },
        },
    }
}

fn intersectBitmapArray(alloc: Allocator, key: u16, bmp: *const BitmapContainer, arr: *const ArrayContainer) !?Chunk {
    var result = ArrayContainer{};
    for (arr.values.items) |v| {
        if (bmp.contains(v)) {
            try result.values.append(alloc, v);
        }
    }
    const card: u32 = @intCast(result.values.items.len);
    if (card == 0) {
        result.deinit(alloc);
        return null;
    }
    return .{ .key = key, .container = .{ .array = result }, .cached_card = card };
}

fn unionContainers(alloc: Allocator, key: u16, a: *const Container, b: *const Container) !Chunk {
    switch (a.*) {
        .bitmap => |ba| switch (b.*) {
            .bitmap => |bb| {
                const bmp = try BitmapContainer.init(alloc);
                var card: u32 = 0;
                for (bmp.bits, ba.bits, bb.bits) |*r, wa, wb| {
                    r.* = wa | wb;
                    card += @popCount(r.*);
                }
                return .{ .key = key, .container = .{ .bitmap = bmp }, .cached_card = card };
            },
            .array => |ab| return unionBitmapArray(alloc, key, &ba, &ab),
        },
        .array => |aa| switch (b.*) {
            .bitmap => |bb| return unionBitmapArray(alloc, key, &bb, &aa),
            .array => |ab| {
                // Array | Array — merge union, might need promotion
                var arr = ArrayContainer{};
                var i: usize = 0;
                var j: usize = 0;
                while (i < aa.values.items.len and j < ab.values.items.len) {
                    if (aa.values.items[i] < ab.values.items[j]) {
                        try arr.values.append(alloc, aa.values.items[i]);
                        i += 1;
                    } else if (aa.values.items[i] > ab.values.items[j]) {
                        try arr.values.append(alloc, ab.values.items[j]);
                        j += 1;
                    } else {
                        try arr.values.append(alloc, aa.values.items[i]);
                        i += 1;
                        j += 1;
                    }
                }
                while (i < aa.values.items.len) : (i += 1) {
                    try arr.values.append(alloc, aa.values.items[i]);
                }
                while (j < ab.values.items.len) : (j += 1) {
                    try arr.values.append(alloc, ab.values.items[j]);
                }
                const card: u32 = @intCast(arr.values.items.len);
                if (card >= ARRAY_MAX) {
                    // Promote to bitmap
                    var bmp = try BitmapContainer.init(alloc);
                    for (arr.values.items) |v| {
                        _ = bmp.add(v);
                    }
                    arr.deinit(alloc);
                    return .{ .key = key, .container = .{ .bitmap = bmp }, .cached_card = card };
                }
                return .{ .key = key, .container = .{ .array = arr }, .cached_card = card };
            },
        },
    }
}

fn unionBitmapArray(alloc: Allocator, key: u16, bmp: *const BitmapContainer, arr: *const ArrayContainer) !Chunk {
    var result = try BitmapContainer.init(alloc);
    @memcpy(result.bits, bmp.bits);
    for (arr.values.items) |v| {
        _ = result.add(v);
    }
    const card = result.cardinality();
    return .{ .key = key, .container = .{ .bitmap = result }, .cached_card = card };
}

fn cloneChunk(alloc: Allocator, chunk: *const Chunk) !Chunk {
    switch (chunk.container) {
        .array => |a| {
            var new_arr = ArrayContainer{};
            try new_arr.values.ensureTotalCapacity(alloc, a.values.items.len);
            new_arr.values.appendSliceAssumeCapacity(a.values.items);
            return .{ .key = chunk.key, .container = .{ .array = new_arr }, .cached_card = chunk.cached_card };
        },
        .bitmap => |b| {
            const new_bmp = try BitmapContainer.init(alloc);
            @memcpy(new_bmp.bits, b.bits);
            return .{ .key = chunk.key, .container = .{ .bitmap = new_bmp }, .cached_card = chunk.cached_card };
        },
    }
}

// ── Tests ───────────────────────────────────────────────────────────────────

test "sequential values — cardinality" {
    const alloc = testing.allocator;
    var bm = RoaringBitmap.init(alloc);
    defer bm.deinit(alloc);

    var i: u32 = 0;
    while (i < 10_000) : (i += 1) {
        try bm.add(alloc, i);
    }
    try testing.expectEqual(@as(u64, 10_000), bm.cardinality());
}

test "sparse values — contains" {
    const alloc = testing.allocator;
    var bm = RoaringBitmap.init(alloc);
    defer bm.deinit(alloc);

    try bm.add(alloc, 0);
    try bm.add(alloc, 100_000);
    try bm.add(alloc, 1_000_000);
    try bm.add(alloc, 4_000_000_000);

    try testing.expect(bm.contains(0));
    try testing.expect(bm.contains(100_000));
    try testing.expect(bm.contains(1_000_000));
    try testing.expect(bm.contains(4_000_000_000));
    try testing.expect(!bm.contains(1));
    try testing.expect(!bm.contains(99_999));
}

test "intersection of two bitmaps" {
    const alloc = testing.allocator;
    var a = RoaringBitmap.init(alloc);
    defer a.deinit(alloc);
    var b = RoaringBitmap.init(alloc);
    defer b.deinit(alloc);

    // a = {0..999}, b = {500..1499}
    var i: u32 = 0;
    while (i < 1000) : (i += 1) {
        try a.add(alloc, i);
    }
    i = 500;
    while (i < 1500) : (i += 1) {
        try b.add(alloc, i);
    }

    var result = try a.andWith(&b, alloc);
    defer result.deinit(alloc);

    // Intersection should be {500..999} = 500 elements
    try testing.expectEqual(@as(u64, 500), result.cardinality());
    try testing.expect(result.contains(500));
    try testing.expect(result.contains(999));
    try testing.expect(!result.contains(499));
    try testing.expect(!result.contains(1000));
}

test "auto-promotion array to bitmap at 4096" {
    const alloc = testing.allocator;
    var bm = RoaringBitmap.init(alloc);
    defer bm.deinit(alloc);

    // Add 4095 values — should remain array
    var i: u32 = 0;
    while (i < 4095) : (i += 1) {
        try bm.add(alloc, i);
    }
    try testing.expect(bm.chunks.items[0].container == .array);

    // Add one more — should promote to bitmap
    try bm.add(alloc, 4095);
    try testing.expect(bm.chunks.items[0].container == .bitmap);
    try testing.expectEqual(@as(u64, 4096), bm.cardinality());

    // Values should still be accessible
    try testing.expect(bm.contains(0));
    try testing.expect(bm.contains(4095));
}

test "iterator produces sorted output" {
    const alloc = testing.allocator;
    var bm = RoaringBitmap.init(alloc);
    defer bm.deinit(alloc);

    // Add values from multiple chunks, out of order
    try bm.add(alloc, 200_000); // chunk 3
    try bm.add(alloc, 50);      // chunk 0
    try bm.add(alloc, 70_000);  // chunk 1
    try bm.add(alloc, 10);      // chunk 0
    try bm.add(alloc, 200_001); // chunk 3

    var it = bm.iterator();
    var prev: u32 = 0;
    var first = true;
    var count: u64 = 0;
    while (it.next()) |v| {
        if (!first) {
            try testing.expect(v > prev);
        }
        prev = v;
        first = false;
        count += 1;
    }
    try testing.expectEqual(@as(u64, 5), count);
    try testing.expectEqual(bm.cardinality(), count);
}

test "union of two bitmaps" {
    const alloc = testing.allocator;
    var a = RoaringBitmap.init(alloc);
    defer a.deinit(alloc);
    var b = RoaringBitmap.init(alloc);
    defer b.deinit(alloc);

    var i: u32 = 0;
    while (i < 100) : (i += 1) {
        try a.add(alloc, i);
    }
    i = 50;
    while (i < 150) : (i += 1) {
        try b.add(alloc, i);
    }

    var result = try a.orWith(&b, alloc);
    defer result.deinit(alloc);

    // Union should be {0..149} = 150 elements
    try testing.expectEqual(@as(u64, 150), result.cardinality());
    try testing.expect(result.contains(0));
    try testing.expect(result.contains(149));
}

test "toArray materializes sorted values" {
    const alloc = testing.allocator;
    var bm = RoaringBitmap.init(alloc);
    defer bm.deinit(alloc);

    try bm.add(alloc, 300);
    try bm.add(alloc, 100);
    try bm.add(alloc, 200);

    const arr = try bm.toArray(alloc);
    defer alloc.free(arr);

    try testing.expectEqual(@as(usize, 3), arr.len);
    try testing.expectEqual(@as(u32, 100), arr[0]);
    try testing.expectEqual(@as(u32, 200), arr[1]);
    try testing.expectEqual(@as(u32, 300), arr[2]);
}

test "duplicate adds don't increase cardinality" {
    const alloc = testing.allocator;
    var bm = RoaringBitmap.init(alloc);
    defer bm.deinit(alloc);

    try bm.add(alloc, 42);
    try bm.add(alloc, 42);
    try bm.add(alloc, 42);

    try testing.expectEqual(@as(u64, 1), bm.cardinality());
}
