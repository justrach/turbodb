/// TurboDB Trigram Search Index
/// =============================
/// Cursor-style trigram inverted index for fast substring/regex search.
///
/// Extract overlapping 3-char trigrams from document values,
/// store inverted posting lists (trigram -> list of doc_ids).
/// Query: decompose search string into trigrams, intersect posting lists,
/// return candidate doc_ids for full verification by caller.
const std = @import("std");

/// Pack 3 bytes into a u24 trigram key (case-insensitive).
pub fn trigramKey(a: u8, b: u8, c: u8) u24 {
    return @as(u24, toLower(a)) << 16 | @as(u24, toLower(b)) << 8 | @as(u24, toLower(c));
}

fn toLower(c: u8) u8 {
    return if (c >= 'A' and c <= 'Z') c + 32 else c;
}

/// Trigram inverted index.
pub const TrigramIndex = struct {
    postings: std.AutoHashMap(u24, std.ArrayList(u64)),
    alloc: std.mem.Allocator,
    doc_count: u64,
    trigram_count: u64,

    /// Maximum posting list length per trigram. Trigrams appearing in more docs
    /// than this are too common to be useful for search — stop tracking them to
    /// bound memory. 10K docs × 8 bytes = 80KB per trigram, manageable.
    const MAX_POSTINGS_PER_TRIGRAM: usize = 10_000;

    pub fn init(alloc: std.mem.Allocator) TrigramIndex {
        return .{
            .postings = std.AutoHashMap(u24, std.ArrayList(u64)).init(alloc),
            .alloc = alloc,
            .doc_count = 0,
            .trigram_count = 0,
        };
    }

    pub fn deinit(self: *TrigramIndex) void {
        var it = self.postings.valueIterator();
        while (it.next()) |list| {
            list.deinit(self.alloc);
        }
        self.postings.deinit();
    }

    /// Index a document's value. Extracts all overlapping trigrams.
    pub fn indexDoc(self: *TrigramIndex, doc_id: u64, value: []const u8) !void {
        if (value.len < 3) return;
        var i: usize = 0;
        while (i + 2 < value.len) : (i += 1) {
            const key = trigramKey(value[i], value[i + 1], value[i + 2]);
            const gop = try self.postings.getOrPut(key);
            if (!gop.found_existing) {
                gop.value_ptr.* = .empty;
                self.trigram_count += 1;
            }
            const list = gop.value_ptr;
            // Skip overly common trigrams to bound memory.
            if (list.items.len >= MAX_POSTINGS_PER_TRIGRAM) continue;
            if (list.items.len == 0 or list.items[list.items.len - 1] != doc_id) {
                try list.append(self.alloc, doc_id);
            }
        }
        self.doc_count += 1;
    }

    /// Remove a document from all posting lists (compact in-place).
    pub fn removeDoc(self: *TrigramIndex, doc_id: u64, value: []const u8) void {
        if (value.len < 3) return;
        var i: usize = 0;
        while (i + 2 < value.len) : (i += 1) {
            const key = trigramKey(value[i], value[i + 1], value[i + 2]);
            if (self.postings.getPtr(key)) |list| {
                var write: usize = 0;
                for (list.items) |id| {
                    if (id != doc_id) {
                        list.items[write] = id;
                        write += 1;
                    }
                }
                list.items.len = write;
            }
        }
        if (self.doc_count > 0) self.doc_count -= 1;
    }

    /// Extract trigrams from a search query string.
    pub fn extractQueryTrigrams(query: []const u8, out: []u24) u32 {
        if (query.len < 3) return 0;
        var count: u32 = 0;
        var i: usize = 0;
        while (i + 2 < query.len) : (i += 1) {
            if (count >= out.len) break;
            out[count] = trigramKey(query[i], query[i + 1], query[i + 2]);
            count += 1;
        }
        return count;
    }

    /// Search: find candidate doc_ids that contain ALL query trigrams.
    /// Returns sorted, deduplicated slice. Caller owns memory.
    pub fn search(self: *TrigramIndex, query: []const u8, alloc: std.mem.Allocator) !SearchResult {
        var tri_buf: [256]u24 = undefined;
        const n_tris = extractQueryTrigrams(query, &tri_buf);

        if (n_tris == 0) {
            return SearchResult{ .candidates = &.{}, .trigrams_used = 0, .total_docs = self.doc_count };
        }

        // Find smallest posting list
        var smallest_idx: u32 = 0;
        var smallest_len: usize = std.math.maxInt(usize);
        var ti: u32 = 0;
        while (ti < n_tris) : (ti += 1) {
            if (self.postings.get(tri_buf[ti])) |list| {
                if (list.items.len < smallest_len) {
                    smallest_len = list.items.len;
                    smallest_idx = ti;
                }
            } else {
                return SearchResult{ .candidates = &.{}, .trigrams_used = n_tris, .total_docs = self.doc_count };
            }
        }

        // Start with smallest posting list
        const seed = self.postings.get(tri_buf[smallest_idx]).?.items;
        var result: std.ArrayList(u64) = .empty;
        errdefer result.deinit(alloc);

        for (seed) |id| {
            try result.append(alloc, id);
        }

        // Intersect with remaining
        ti = 0;
        while (ti < n_tris) : (ti += 1) {
            if (ti == smallest_idx) continue;
            const other = self.postings.get(tri_buf[ti]).?.items;
            var write: usize = 0;
            for (result.items) |id| {
                if (containsId(other, id)) {
                    result.items[write] = id;
                    write += 1;
                }
            }
            result.items.len = write;
            if (write == 0) break;
        }

        // Deduplicate
        if (result.items.len > 1) {
            std.mem.sort(u64, result.items, {}, std.sort.asc(u64));
            var w: usize = 1;
            var ri: usize = 1;
            while (ri < result.items.len) : (ri += 1) {
                if (result.items[ri] != result.items[w - 1]) {
                    result.items[w] = result.items[ri];
                    w += 1;
                }
            }
            result.items.len = w;
        }

        const owned = try result.toOwnedSlice(alloc);
        return SearchResult{ .candidates = owned, .trigrams_used = n_tris, .total_docs = self.doc_count };
    }

    fn containsId(list: []const u64, target: u64) bool {
        for (list) |id| {
            if (id == target) return true;
        }
        return false;
    }
};

pub const SearchResult = struct {
    candidates: []u64,
    trigrams_used: u32,
    total_docs: u64,

    pub fn deinit(self: *SearchResult, alloc: std.mem.Allocator) void {
        if (self.candidates.len > 0) alloc.free(self.candidates);
    }
};

test "trigramKey case-insensitive" {
    const k = trigramKey('a', 'b', 'c');
    const k2 = trigramKey('A', 'B', 'C');
    try std.testing.expectEqual(k, k2);
}

test "index and search" {
    const alloc = std.testing.allocator;
    var idx = TrigramIndex.init(alloc);
    defer idx.deinit();

    try idx.indexDoc(1, "the quick brown fox");
    try idx.indexDoc(2, "the lazy brown dog");
    try idx.indexDoc(3, "a fox jumps over");

    var res = try idx.search("brown", alloc);
    defer res.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 2), res.candidates.len);

    var res2 = try idx.search("fox", alloc);
    defer res2.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 2), res2.candidates.len);

    var res3 = try idx.search("quick brown", alloc);
    defer res3.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), res3.candidates.len);
    try std.testing.expectEqual(@as(u64, 1), res3.candidates[0]);

    var res4 = try idx.search("xyz", alloc);
    defer res4.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 0), res4.candidates.len);
}
