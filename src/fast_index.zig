const std = @import("std");

/// High-performance trigram index using a flat direct-mapped array.
///
/// Replaces the nested `AutoHashMap(Trigram, StringHashMap(PostingMask))` approach
/// with a cache-friendly flat array (262,144 entries, ~2MB) for O(1) trigram lookup,
/// plus per-slot posting lists stored as ArrayLists of file IDs.
pub const FastTrigramIndex = struct {
    /// Reduced alphabet size: 6 bits per character (64 symbols).
    const ALPHA_SIZE = 64;
    /// Total flat array entries: 64^3 = 262,144.
    const TABLE_SIZE = ALPHA_SIZE * ALPHA_SIZE * ALPHA_SIZE;

    /// Each slot in the flat table holds a list of file IDs that contain this trigram.
    const PostingList = std.ArrayList(u32);

    /// Flat lookup table: trigram_key -> PostingList.
    table: []PostingList,

    /// Number of active (non-empty) trigram slots.
    active_trigrams: u32,

    /// File ID registry: index = file_id, value = owned path copy (empty slice = removed).
    file_paths: std.ArrayList([]const u8),

    /// Reverse lookup: path -> file_id.
    path_to_id: std.StringHashMap(u32),

    /// Per-file trigram key tracking for removeFile support.
    /// Index = file_id, value = list of trigram keys this file contributed.
    file_trigram_keys: std.ArrayList(std.ArrayList(u32)),

    /// Recycled file_id slots (from removeFile).
    free_ids: std.ArrayList(u32),

    allocator: std.mem.Allocator,

    /// Map a byte to the reduced 0-63 alphabet.
    /// a-z -> 0-25, 0-9 -> 26-35, _ -> 36, common punctuation -> 37-63, rest -> 63.
    fn charMap(c: u8) u6 {
        return switch (c) {
            'a'...'z' => @intCast(c - 'a'),
            'A'...'Z' => @intCast(c - 'A'), // case-insensitive
            '0'...'9' => @intCast(c - '0' + 26),
            '_' => 36,
            '.' => 37,
            ',' => 38,
            ';' => 39,
            ':' => 40,
            '!' => 41,
            '?' => 42,
            '(' => 43,
            ')' => 44,
            '[' => 45,
            ']' => 46,
            '{' => 47,
            '}' => 48,
            '<' => 49,
            '>' => 50,
            '+' => 51,
            '-' => 52,
            '*' => 53,
            '/' => 54,
            '=' => 55,
            '&' => 56,
            '|' => 57,
            '^' => 58,
            '~' => 59,
            '#' => 60,
            '@' => 61,
            ' ' => 62,
            else => 63,
        };
    }

    /// Compute flat array index from three characters.
    fn trigramKey(c0: u8, c1: u8, c2: u8) u32 {
        const a: u32 = @intCast(charMap(c0));
        const b: u32 = @intCast(charMap(c1));
        const c: u32 = @intCast(charMap(c2));
        return a * (ALPHA_SIZE * ALPHA_SIZE) + b * ALPHA_SIZE + c;
    }

    pub fn init(alloc: std.mem.Allocator) FastTrigramIndex {
        const table = alloc.alloc(PostingList, TABLE_SIZE) catch
            @panic("FastTrigramIndex: failed to allocate flat table");
        var i: usize = 0;
        while (i < TABLE_SIZE) : (i += 1) {
            table[i] = .empty;
        }
        return .{
            .table = table,
            .active_trigrams = 0,
            .file_paths = .empty,
            .path_to_id = std.StringHashMap(u32).init(alloc),
            .file_trigram_keys = .empty,
            .free_ids = .empty,
            .allocator = alloc,
        };
    }

    pub fn deinit(self: *FastTrigramIndex) void {
        var i: usize = 0;
        while (i < TABLE_SIZE) : (i += 1) {
            self.table[i].deinit(self.allocator);
        }
        self.allocator.free(self.table);

        for (self.file_paths.items) |p| {
            if (p.len > 0) self.allocator.free(p);
        }
        self.file_paths.deinit(self.allocator);
        self.path_to_id.deinit();

        for (self.file_trigram_keys.items) |*tl| {
            tl.deinit(self.allocator);
        }
        self.file_trigram_keys.deinit(self.allocator);
        self.free_ids.deinit(self.allocator);
    }

    pub fn indexFile(self: *FastTrigramIndex, path: []const u8, content: []const u8) !void {
        // If file was previously indexed, remove it first.
        if (self.path_to_id.contains(path)) {
            self.removeFile(path);
        }

        // Allocate or recycle a file_id.
        const file_id: u32 = if (self.free_ids.items.len > 0) blk: {
            const last = self.free_ids.items.len - 1;
            const id = self.free_ids.items[last];
            self.free_ids.shrinkRetainingCapacity(last);
            break :blk id;
        } else @intCast(self.file_paths.items.len);

        const owned_path = try self.allocator.dupe(u8, path);
        errdefer self.allocator.free(owned_path);

        if (file_id < self.file_paths.items.len) {
            // Reuse slot.
            self.file_paths.items[file_id] = owned_path;
        } else {
            try self.file_paths.append(self.allocator, owned_path);
        }
        try self.path_to_id.put(owned_path, file_id);

        // Track trigram keys for this file.
        var file_keys: std.ArrayList(u32) = .empty;
        errdefer file_keys.deinit(self.allocator);

        if (content.len >= 3) {
            // Phase 1: Collect unique trigram keys.
            var seen = std.AutoHashMap(u32, void).init(self.allocator);
            defer seen.deinit();

            const limit = content.len - 2;
            var i: usize = 0;
            while (i < limit) : (i += 1) {
                const key = trigramKey(content[i], content[i + 1], content[i + 2]);
                const gop = try seen.getOrPut(key);
                if (!gop.found_existing) {
                    try file_keys.append(self.allocator, key);
                }
            }

            // Phase 2: Append file_id to each trigram's posting list.
            for (file_keys.items) |key| {
                const pl = &self.table[key];
                if (pl.items.len == 0) {
                    self.active_trigrams += 1;
                }
                try pl.append(self.allocator, file_id);
            }
        }

        // Store the trigram keys list for this file_id.
        if (file_id < self.file_trigram_keys.items.len) {
            self.file_trigram_keys.items[file_id] = file_keys;
        } else {
            // Fill any gap with empty lists.
            while (self.file_trigram_keys.items.len < file_id) {
                try self.file_trigram_keys.append(self.allocator, .empty);
            }
            try self.file_trigram_keys.append(self.allocator, file_keys);
        }
    }

    pub fn removeFile(self: *FastTrigramIndex, path: []const u8) void {
        const file_id = self.path_to_id.get(path) orelse return;

        // Remove file_id from each trigram's posting list.
        if (file_id < self.file_trigram_keys.items.len) {
            const keys = self.file_trigram_keys.items[file_id].items;
            for (keys) |key| {
                const pl = &self.table[key];
                // Swap-remove file_id from posting list.
                var j: usize = 0;
                while (j < pl.items.len) {
                    if (pl.items[j] == file_id) {
                        _ = pl.swapRemove(j);
                        // Don't increment j; check the swapped-in element.
                    } else {
                        j += 1;
                    }
                }
                if (pl.items.len == 0) {
                    if (self.active_trigrams > 0) self.active_trigrams -= 1;
                }
            }
            self.file_trigram_keys.items[file_id].deinit(self.allocator);
            self.file_trigram_keys.items[file_id] = .empty;
        }

        // Remove from path_to_id.
        _ = self.path_to_id.remove(path);

        // Free the owned path and mark slot as empty.
        if (file_id < self.file_paths.items.len) {
            const p = self.file_paths.items[file_id];
            if (p.len > 0) self.allocator.free(p);
            self.file_paths.items[file_id] = &.{};
        }

        // Recycle the file_id.
        self.free_ids.append(self.allocator, file_id) catch {};
    }

    pub fn candidates(self: *FastTrigramIndex, query: []const u8, alloc: std.mem.Allocator) ?[]const []const u8 {
        if (query.len < 3) return null;

        const tri_count = query.len - 2;

        // Collect unique trigram keys from query.
        var unique_keys: std.ArrayList(u32) = .empty;
        defer unique_keys.deinit(alloc);
        var seen = std.AutoHashMap(u32, void).init(alloc);
        defer seen.deinit();

        {
            var i: usize = 0;
            while (i < tri_count) : (i += 1) {
                const key = trigramKey(query[i], query[i + 1], query[i + 2]);
                const gop = seen.getOrPut(key) catch return null;
                if (!gop.found_existing) {
                    unique_keys.append(alloc, key) catch return null;
                }
            }
        }

        if (unique_keys.items.len == 0) return null;

        // Find the trigram with the smallest posting list.
        var min_idx: usize = 0;
        var min_count: usize = self.table[unique_keys.items[0]].items.len;
        {
            var k: usize = 1;
            while (k < unique_keys.items.len) : (k += 1) {
                const cnt = self.table[unique_keys.items[k]].items.len;
                if (cnt < min_count) {
                    min_count = cnt;
                    min_idx = k;
                }
            }
        }

        // If any trigram has zero postings, no files can match.
        if (min_count == 0) {
            return alloc.alloc([]const u8, 0) catch null;
        }

        // Start with file_ids from the smallest posting list.
        var candidate_set = std.AutoHashMap(u32, void).init(alloc);
        defer candidate_set.deinit();
        {
            const pl = &self.table[unique_keys.items[min_idx]];
            for (pl.items) |fid| {
                candidate_set.put(fid, {}) catch return null;
            }
        }

        // Intersect with all other trigram posting lists.
        {
            var k: usize = 0;
            while (k < unique_keys.items.len) : (k += 1) {
                if (k == min_idx) continue;
                const pl = &self.table[unique_keys.items[k]];
                if (pl.items.len == 0) {
                    return alloc.alloc([]const u8, 0) catch null;
                }

                // Build set from this posting list.
                var other_set = std.AutoHashMap(u32, void).init(alloc);
                defer other_set.deinit();
                for (pl.items) |fid| {
                    other_set.put(fid, {}) catch return null;
                }

                // Remove from candidate_set anything not in other_set.
                var to_remove: std.ArrayList(u32) = .empty;
                defer to_remove.deinit(alloc);
                var iter = candidate_set.keyIterator();
                while (iter.next()) |key_ptr| {
                    if (!other_set.contains(key_ptr.*)) {
                        to_remove.append(alloc, key_ptr.*) catch return null;
                    }
                }
                for (to_remove.items) |fid| {
                    _ = candidate_set.remove(fid);
                }

                if (candidate_set.count() == 0) {
                    return alloc.alloc([]const u8, 0) catch null;
                }
            }
        }

        // Convert file_ids to path slices.
        var result: std.ArrayList([]const u8) = .empty;
        result.ensureTotalCapacity(alloc, candidate_set.count()) catch return null;
        var iter = candidate_set.keyIterator();
        while (iter.next()) |fid_ptr| {
            const fid = fid_ptr.*;
            if (fid < self.file_paths.items.len) {
                const p = self.file_paths.items[fid];
                if (p.len > 0) {
                    result.appendAssumeCapacity(p);
                }
            }
        }

        return result.toOwnedSlice(alloc) catch {
            result.deinit(alloc);
            return null;
        };
    }

    pub fn fileCount(self: *FastTrigramIndex) u32 {
        return @intCast(self.path_to_id.count());
    }

    pub fn trigramCount(self: *FastTrigramIndex) u32 {
        return self.active_trigrams;
    }
};

// ────────────────────────── Tests ──────────────────────────

test "FastTrigramIndex: charMap basics" {
    const a = FastTrigramIndex.charMap('a');
    try std.testing.expectEqual(@as(u6, 0), a);
    const z = FastTrigramIndex.charMap('z');
    try std.testing.expectEqual(@as(u6, 25), z);
    const zero = FastTrigramIndex.charMap('0');
    try std.testing.expectEqual(@as(u6, 26), zero);
    const nine = FastTrigramIndex.charMap('9');
    try std.testing.expectEqual(@as(u6, 35), nine);
    const under = FastTrigramIndex.charMap('_');
    try std.testing.expectEqual(@as(u6, 36), under);
    // Case insensitive
    const upper_a = FastTrigramIndex.charMap('A');
    try std.testing.expectEqual(@as(u6, 0), upper_a);
}

test "FastTrigramIndex: trigramKey deterministic" {
    const k1 = FastTrigramIndex.trigramKey('a', 'b', 'c');
    const k2 = FastTrigramIndex.trigramKey('a', 'b', 'c');
    try std.testing.expectEqual(k1, k2);
    // Case insensitive
    const k3 = FastTrigramIndex.trigramKey('A', 'B', 'C');
    try std.testing.expectEqual(k1, k3);
    // Different trigrams produce different keys
    const k4 = FastTrigramIndex.trigramKey('x', 'y', 'z');
    try std.testing.expect(k1 != k4);
}

test "FastTrigramIndex: init and deinit" {
    var idx = FastTrigramIndex.init(std.testing.allocator);
    defer idx.deinit();
    try std.testing.expectEqual(@as(u32, 0), idx.fileCount());
    try std.testing.expectEqual(@as(u32, 0), idx.trigramCount());
}

test "FastTrigramIndex: indexFile and candidates" {
    var idx = FastTrigramIndex.init(std.testing.allocator);
    defer idx.deinit();

    try idx.indexFile("src/main.zig", "fn main() void { }");
    try idx.indexFile("src/util.zig", "fn helper() void { main(); }");
    try idx.indexFile("src/test.zig", "test { expect(true); }");

    try std.testing.expectEqual(@as(u32, 3), idx.fileCount());
    try std.testing.expect(idx.trigramCount() > 0);

    // "main" should match files containing that substring.
    const results = idx.candidates("main", std.testing.allocator) orelse {
        return error.TestUnexpectedResult;
    };
    defer std.testing.allocator.free(results);

    // Both main.zig and util.zig contain "main".
    try std.testing.expectEqual(@as(usize, 2), results.len);

    // Verify both expected paths are present.
    var found_main = false;
    var found_util = false;
    for (results) |p| {
        if (std.mem.eql(u8, p, "src/main.zig")) found_main = true;
        if (std.mem.eql(u8, p, "src/util.zig")) found_util = true;
    }
    try std.testing.expect(found_main);
    try std.testing.expect(found_util);
}

test "FastTrigramIndex: short query returns null" {
    var idx = FastTrigramIndex.init(std.testing.allocator);
    defer idx.deinit();

    try idx.indexFile("a.zig", "hello world");
    const r = idx.candidates("ab", std.testing.allocator);
    try std.testing.expect(r == null);
}

test "FastTrigramIndex: no match returns empty" {
    var idx = FastTrigramIndex.init(std.testing.allocator);
    defer idx.deinit();

    try idx.indexFile("a.zig", "hello world");
    const results = idx.candidates("zzzzz", std.testing.allocator) orelse {
        return error.TestUnexpectedResult;
    };
    defer std.testing.allocator.free(results);
    try std.testing.expectEqual(@as(usize, 0), results.len);
}

test "FastTrigramIndex: removeFile" {
    var idx = FastTrigramIndex.init(std.testing.allocator);
    defer idx.deinit();

    try idx.indexFile("a.zig", "fn foobar() {}");
    try idx.indexFile("b.zig", "fn foobar() {}");
    try std.testing.expectEqual(@as(u32, 2), idx.fileCount());

    // Both files match "foobar".
    {
        const results = idx.candidates("foobar", std.testing.allocator) orelse {
            return error.TestUnexpectedResult;
        };
        defer std.testing.allocator.free(results);
        try std.testing.expectEqual(@as(usize, 2), results.len);
    }

    // Remove one file.
    idx.removeFile("a.zig");
    try std.testing.expectEqual(@as(u32, 1), idx.fileCount());

    // Now only b.zig should match.
    {
        const results = idx.candidates("foobar", std.testing.allocator) orelse {
            return error.TestUnexpectedResult;
        };
        defer std.testing.allocator.free(results);
        try std.testing.expectEqual(@as(usize, 1), results.len);
        try std.testing.expect(std.mem.eql(u8, results[0], "b.zig"));
    }
}

test "FastTrigramIndex: case insensitive search" {
    var idx = FastTrigramIndex.init(std.testing.allocator);
    defer idx.deinit();

    try idx.indexFile("a.zig", "Hello World");
    // Search with lowercase should still find the file.
    const results = idx.candidates("hello", std.testing.allocator) orelse {
        return error.TestUnexpectedResult;
    };
    defer std.testing.allocator.free(results);
    try std.testing.expectEqual(@as(usize, 1), results.len);
}

test "FastTrigramIndex: reindex same file" {
    var idx = FastTrigramIndex.init(std.testing.allocator);
    defer idx.deinit();

    try idx.indexFile("a.zig", "fn alpha() {}");
    try idx.indexFile("a.zig", "fn beta() {}");
    try std.testing.expectEqual(@as(u32, 1), idx.fileCount());

    // "alpha" should no longer match after re-indexing.
    const r1 = idx.candidates("alpha", std.testing.allocator) orelse {
        return error.TestUnexpectedResult;
    };
    defer std.testing.allocator.free(r1);
    try std.testing.expectEqual(@as(usize, 0), r1.len);

    // "beta" should match.
    const r2 = idx.candidates("beta", std.testing.allocator) orelse {
        return error.TestUnexpectedResult;
    };
    defer std.testing.allocator.free(r2);
    try std.testing.expectEqual(@as(usize, 1), r2.len);
}

test "FastTrigramIndex: file id recycling" {
    var idx = FastTrigramIndex.init(std.testing.allocator);
    defer idx.deinit();

    try idx.indexFile("a.zig", "fn aaa() {}");
    try idx.indexFile("b.zig", "fn bbb() {}");
    try std.testing.expectEqual(@as(u32, 2), idx.fileCount());

    idx.removeFile("a.zig");
    try std.testing.expectEqual(@as(u32, 1), idx.fileCount());

    // Index a new file — should recycle the freed slot.
    try idx.indexFile("c.zig", "fn ccc() {}");
    try std.testing.expectEqual(@as(u32, 2), idx.fileCount());

    const results = idx.candidates("ccc", std.testing.allocator) orelse {
        return error.TestUnexpectedResult;
    };
    defer std.testing.allocator.free(results);
    try std.testing.expectEqual(@as(usize, 1), results.len);
    try std.testing.expect(std.mem.eql(u8, results[0], "c.zig"));
}
