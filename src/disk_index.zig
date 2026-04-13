/// TurboDB On-Disk Trigram Index
/// ==============================
/// Cursor-style mmap'd posting lists for GitHub-scale search.
///
/// Architecture (from the Cursor blog):
///   1. Lookup table: sorted array of (trigram_hash, posting_offset) — mmap'd
///   2. Postings file: packed file_id lists — read on demand via pread
///   3. Frequency-weighted sparse n-grams: variable-length n-grams weighted by
///      character pair rarity — fewer lookups, higher selectivity
///
/// On-disk layout:
///   index.tdb  — lookup table: [n_entries: u32][entries: (hash: u32, offset: u32, count: u32)...]
///   posts.tdb  — posting lists: packed u32 file_ids, one list per trigram
///   files.tdb  — file ID → path mapping: [n_files: u32][(offset: u32, len: u16)...][paths...]
///
const std = @import("std");

// ─── Types ──────────────────────────────────────────────────────────────────

pub const Trigram = u24;
const TRIGRAM_HASH_BITS = 24;

/// On-disk lookup entry: 12 bytes, naturally aligned.
pub const LookupEntry = extern struct {
    hash: u32,      // trigram packed as u24, stored in u32
    offset: u32,    // byte offset into postings file
    count: u32,     // number of file_ids in this posting list
};

/// Character pair frequency table for sparse n-gram weighting.
/// Lower frequency = higher weight = better discriminator.
pub const FreqTable = struct {
    counts: [256 * 256]u32 = [_]u32{0} ** (256 * 256),
    total: u64 = 0,

    pub fn record(self: *FreqTable, a: u8, b: u8) void {
        self.counts[@as(usize, a) * 256 + b] += 1;
        self.total += 1;
    }

    /// Weight: inverse frequency. Rare pairs get high weight.
    pub fn weight(self: *const FreqTable, a: u8, b: u8) u32 {
        const c = self.counts[@as(usize, a) * 256 + b];
        if (c == 0) return std.math.maxInt(u32);
        // Inverse frequency scaled to u32
        return @intCast(@min(self.total / c, std.math.maxInt(u32)));
    }
};

// ─── Builder (in-memory → on-disk) ──────────────────────────────────────────

pub const DiskIndexBuilder = struct {
    /// trigram → list of file_ids
    postings: std.AutoHashMap(Trigram, std.ArrayList(u32)),
    /// file_id → path
    file_paths: std.ArrayList([]const u8),
    /// path → file_id (dedup)
    path_to_id: std.StringHashMap(u32),
    /// Character pair frequency table
    freq: FreqTable,
    alloc: std.mem.Allocator,

    pub fn init(alloc: std.mem.Allocator) DiskIndexBuilder {
        return .{
            .postings = std.AutoHashMap(Trigram, std.ArrayList(u32)).init(alloc),
            .file_paths = .empty,
            .path_to_id = std.StringHashMap(u32).init(alloc),
            .freq = .{},
            .alloc = alloc,
        };
    }

    pub fn deinit(self: *DiskIndexBuilder) void {
        var it = self.postings.valueIterator();
        while (it.next()) |list| list.deinit(self.alloc);
        self.postings.deinit();
        for (self.file_paths.items) |p| self.alloc.free(p);
        self.file_paths.deinit(self.alloc);
        self.path_to_id.deinit();
    }

    /// Register a file and get its ID.
    pub fn addFile(self: *DiskIndexBuilder, path: []const u8) !u32 {
        if (self.path_to_id.get(path)) |id| return id;
        const id: u32 = @intCast(self.file_paths.items.len);
        const owned = try self.alloc.dupe(u8, path);
        try self.file_paths.append(self.alloc, owned);
        try self.path_to_id.put(owned, id);
        return id;
    }

    /// Index file content: extract trigrams, record frequency pairs.
    pub fn indexFile(self: *DiskIndexBuilder, path: []const u8, content: []const u8) !void {
        const file_id = try self.addFile(path);

        if (content.len < 3) return;

        // Record pair frequencies for sparse n-grams
        var fi: usize = 0;
        while (fi + 1 < content.len) : (fi += 1) {
            self.freq.record(toLower(content[fi]), toLower(content[fi + 1]));
        }

        // Extract trigrams
        var seen = std.AutoHashMap(Trigram, void).init(self.alloc);
        defer seen.deinit();

        var i: usize = 0;
        while (i + 2 < content.len) : (i += 1) {
            const tri = packTrigram(
                toLower(content[i]),
                toLower(content[i + 1]),
                toLower(content[i + 2]),
            );
            const gop = try seen.getOrPut(tri);
            if (!gop.found_existing) {
                const post_gop = try self.postings.getOrPut(tri);
                if (!post_gop.found_existing) {
                    post_gop.value_ptr.* = .empty;
                }
                try post_gop.value_ptr.append(self.alloc, file_id);
            }
        }
    }

    /// Flush to disk. Creates three files at `dir_path/`.
    pub fn writeToDisk(self: *DiskIndexBuilder, dir_path: []const u8) !DiskIndexStats {
        var stats = DiskIndexStats{};

        // ── 1. Sort trigrams by hash for binary search ──────────────────
        var entries: std.ArrayList(LookupEntry) = .empty;
        defer entries.deinit(self.alloc);

        var tri_iter = self.postings.iterator();
        while (tri_iter.next()) |kv| {
            try entries.append(self.alloc, .{
                .hash = kv.key_ptr.*,
                .offset = 0, // filled below
                .count = @intCast(kv.value_ptr.items.len),
            });
        }

        // Sort by hash for binary search
        std.mem.sort(LookupEntry, entries.items, {}, struct {
            fn cmp(_: void, a: LookupEntry, b: LookupEntry) bool {
                return a.hash < b.hash;
            }
        }.cmp);

        // ── 2. Write postings file (packed u32 file_ids) ────────────────
        var posts_path_buf: [512]u8 = undefined;
        const posts_path = try std.fmt.bufPrint(&posts_path_buf, "{s}/posts.tdb", .{dir_path});

        const posts_file = try std.fs.cwd().createFile(posts_path, .{});
        defer posts_file.close();

        var offset: u32 = 0;
        for (entries.items) |*entry| {
            const tri = @as(Trigram, @intCast(entry.hash));
            const list = self.postings.get(tri).?;

            // Sort file_ids for cache-friendly access
            std.mem.sort(u32, @constCast(list.items), {}, std.sort.asc(u32));

            entry.offset = offset;
            const bytes = std.mem.sliceAsBytes(list.items);
            try posts_file.writeAll(bytes);
            offset += @intCast(bytes.len);
        }
        stats.postings_bytes = offset;

        // ── 3. Write lookup table (sorted entries for binary search) ────
        var idx_path_buf: [512]u8 = undefined;
        const idx_path = try std.fmt.bufPrint(&idx_path_buf, "{s}/index.tdb", .{dir_path});

        const idx_file = try std.fs.cwd().createFile(idx_path, .{});
        defer idx_file.close();

        // Header: entry count
        const n_entries: u32 = @intCast(entries.items.len);
        try idx_file.writeAll(std.mem.asBytes(&n_entries));

        // Entries
        const entry_bytes = std.mem.sliceAsBytes(entries.items);
        try idx_file.writeAll(entry_bytes);
        stats.index_bytes = 4 + @as(u32, @intCast(entry_bytes.len));
        stats.n_trigrams = n_entries;

        // ── 4. Write file path table ────────────────────────────────────
        var files_path_buf: [512]u8 = undefined;
        const files_path = try std.fmt.bufPrint(&files_path_buf, "{s}/files.tdb", .{dir_path});

        const files_file = try std.fs.cwd().createFile(files_path, .{});
        defer files_file.close();

        const n_files: u32 = @intCast(self.file_paths.items.len);
        try files_file.writeAll(std.mem.asBytes(&n_files));

        // Offset table: (offset: u32, len: u16) per file
        var path_offset: u32 = 0;
        for (self.file_paths.items) |p| {
            try files_file.writeAll(std.mem.asBytes(&path_offset));
            const plen: u16 = @intCast(p.len);
            try files_file.writeAll(std.mem.asBytes(&plen));
            path_offset += @intCast(p.len);
        }

        // Path strings
        for (self.file_paths.items) |p| {
            try files_file.writeAll(p);
        }
        stats.files_bytes = @intCast(try files_file.getPos());
        stats.n_files = n_files;

        // ── 5. Write frequency table ────────────────────────────────────
        var freq_path_buf: [512]u8 = undefined;
        const freq_path = try std.fmt.bufPrint(&freq_path_buf, "{s}/freq.tdb", .{dir_path});
        const freq_file = try std.fs.cwd().createFile(freq_path, .{});
        defer freq_file.close();
        try freq_file.writeAll(std.mem.asBytes(&self.freq.counts));
        stats.freq_bytes = 256 * 256 * 4;

        return stats;
    }
};

pub const DiskIndexStats = struct {
    n_trigrams: u32 = 0,
    n_files: u32 = 0,
    index_bytes: u32 = 0,
    postings_bytes: u32 = 0,
    files_bytes: u32 = 0,
    freq_bytes: u32 = 0,

    pub fn totalBytes(self: DiskIndexStats) u64 {
        return @as(u64, self.index_bytes) + self.postings_bytes + self.files_bytes + self.freq_bytes;
    }
};

// ─── Reader (mmap'd, zero-copy queries) ─────────────────────────────────────

pub const DiskIndex = struct {
    /// mmap'd lookup table
    lookup: []const LookupEntry,
    lookup_mmap: []align(std.heap.page_size_min) const u8,
    /// Postings file handle (pread for on-demand loading)
    posts_fd: std.fs.File,
    /// File path table (mmap'd)
    files_mmap: []align(std.heap.page_size_min) const u8,
    n_files: u32,
    /// Frequency table (mmap'd)
    freq_mmap: []align(std.heap.page_size_min) const u8,
    alloc: std.mem.Allocator,

    pub fn open(dir_path: []const u8, alloc: std.mem.Allocator) !DiskIndex {
        var buf: [512]u8 = undefined;

        // mmap lookup table
        const idx_path = try std.fmt.bufPrint(&buf, "{s}/index.tdb", .{dir_path});
        const idx_file = try std.fs.cwd().openFile(idx_path, .{});
        defer idx_file.close();
        const idx_stat = try idx_file.stat();
        const idx_mmap = try std.posix.mmap(null, idx_stat.size, std.posix.PROT.READ, .{ .TYPE = .PRIVATE }, idx_file.handle, 0);
        errdefer std.posix.munmap(idx_mmap);

        const n_entries = std.mem.bytesToValue(u32, idx_mmap[0..4]);
        const entries_start = 4;
        const entries_end = entries_start + n_entries * @sizeOf(LookupEntry);
        if (entries_end > idx_stat.size) return error.CorruptIndex;
        const lookup: []const LookupEntry = @alignCast(std.mem.bytesAsSlice(LookupEntry, idx_mmap[entries_start..entries_end]));

        // Open postings file for pread
        const posts_path = try std.fmt.bufPrint(&buf, "{s}/posts.tdb", .{dir_path});
        const posts_fd = try std.fs.cwd().openFile(posts_path, .{});
        errdefer posts_fd.close();

        // mmap files table
        const files_path = try std.fmt.bufPrint(&buf, "{s}/files.tdb", .{dir_path});
        const files_file = try std.fs.cwd().openFile(files_path, .{});
        defer files_file.close();
        const files_stat = try files_file.stat();
        const files_mmap = try std.posix.mmap(null, files_stat.size, std.posix.PROT.READ, .{ .TYPE = .PRIVATE }, files_file.handle, 0);
        errdefer std.posix.munmap(files_mmap);
        const n_files = std.mem.bytesToValue(u32, files_mmap[0..4]);

        // mmap frequency table
        const freq_path = try std.fmt.bufPrint(&buf, "{s}/freq.tdb", .{dir_path});
        const freq_file = try std.fs.cwd().openFile(freq_path, .{});
        defer freq_file.close();
        const freq_stat = try freq_file.stat();
        const freq_mmap = try std.posix.mmap(null, freq_stat.size, std.posix.PROT.READ, .{ .TYPE = .PRIVATE }, freq_file.handle, 0);
        errdefer std.posix.munmap(freq_mmap);

        return .{
            .lookup = lookup,
            .lookup_mmap = idx_mmap,
            .posts_fd = posts_fd,
            .files_mmap = files_mmap,
            .n_files = n_files,
            .freq_mmap = freq_mmap,
            .alloc = alloc,
        };
    }

    pub fn close(self: *DiskIndex) void {
        std.posix.munmap(self.lookup_mmap);
        std.posix.munmap(self.files_mmap);
        std.posix.munmap(self.freq_mmap);
        self.posts_fd.close();
    }

    /// Binary search the sorted lookup table for a trigram.
    pub fn findTrigram(self: *const DiskIndex, tri: Trigram) ?LookupEntry {
        const hash: u32 = tri;
        var lo: usize = 0;
        var hi: usize = self.lookup.len;
        while (lo < hi) {
            const mid = lo + (hi - lo) / 2;
            if (self.lookup[mid].hash < hash) {
                lo = mid + 1;
            } else if (self.lookup[mid].hash > hash) {
                hi = mid;
            } else {
                return self.lookup[mid];
            }
        }
        return null;
    }

    /// Load a posting list from disk via pread. Returns owned slice.
    pub fn loadPostingList(self: *DiskIndex, entry: LookupEntry) ![]u32 {
        const result = try self.alloc.alloc(u32, entry.count);
        errdefer self.alloc.free(result);

        const buf = std.mem.sliceAsBytes(result);
        const n = try self.posts_fd.pread(buf, entry.offset);
        if (n != buf.len) return error.ShortRead;

        return result;
    }

    /// Resolve file_id → path from the mmap'd files table.
    pub fn filePath(self: *const DiskIndex, file_id: u32) ?[]const u8 {
        if (file_id >= self.n_files) return null;
        // Each entry: (offset: u32, len: u16) = 6 bytes, starting at byte 4
        const entry_off = 4 + @as(usize, file_id) * 6;
        if (entry_off + 6 > self.files_mmap.len) return null;
        const path_offset = std.mem.bytesToValue(u32, self.files_mmap[entry_off..][0..4]);
        const path_len = std.mem.bytesToValue(u16, self.files_mmap[entry_off + 4 ..][0..2]);
        // Path strings start after the header + all entries
        const paths_start = 4 + @as(usize, self.n_files) * 6;
        const start = paths_start + path_offset;
        const end = start + path_len;
        if (end > self.files_mmap.len) return null;
        return self.files_mmap[start..end];
    }

    /// Search: find candidate file_ids that contain ALL query trigrams.
    /// Returns sorted, deduplicated file_ids. Caller owns the slice.
    pub fn search(self: *DiskIndex, query: []const u8) !SearchResult {
        if (query.len < 3) return SearchResult{ .file_ids = &.{}, .n_trigrams = 0 };

        // Extract unique query trigrams
        var unique = std.AutoHashMap(Trigram, void).init(self.alloc);
        defer unique.deinit();

        var i: usize = 0;
        while (i + 2 < query.len) : (i += 1) {
            const tri = packTrigram(toLower(query[i]), toLower(query[i + 1]), toLower(query[i + 2]));
            try unique.put(tri, {});
        }

        // Load posting lists, find smallest
        const PostingInfo = struct { list: []u32, entry: LookupEntry };
        var loaded: std.ArrayList(PostingInfo) = .empty;
        defer {
            for (loaded.items) |p| self.alloc.free(std.mem.sliceAsBytes(p.list));
            loaded.deinit(self.alloc);
        }

        var tri_iter = unique.keyIterator();
        while (tri_iter.next()) |tri_ptr| {
            const entry = self.findTrigram(tri_ptr.*) orelse {
                // Trigram not in index → no results
                return SearchResult{ .file_ids = &.{}, .n_trigrams = unique.count() };
            };
            const list = try self.loadPostingList(entry);
            try loaded.append(self.alloc, .{ .list = list, .entry = entry });
        }

        if (loaded.items.len == 0) {
            return SearchResult{ .file_ids = &.{}, .n_trigrams = 0 };
        }

        // Sort by list size (smallest first for fastest intersection)
        std.mem.sort(PostingInfo, loaded.items, {}, struct {
            fn cmp(_: void, a: PostingInfo, b: PostingInfo) bool {
                return a.entry.count < b.entry.count;
            }
        }.cmp);

        // Start with smallest, intersect with rest
        var result = try self.alloc.dupe(u32, loaded.items[0].list);
        var result_len: usize = result.len;

        var li: usize = 1;
        while (li < loaded.items.len) : (li += 1) {
            const other = loaded.items[li].list;
            // Both sorted → merge intersection
            var ri: usize = 0;
            var oi: usize = 0;
            var wi: usize = 0;
            while (ri < result_len and oi < other.len) {
                if (result[ri] == other[oi]) {
                    result[wi] = result[ri];
                    wi += 1;
                    ri += 1;
                    oi += 1;
                } else if (result[ri] < other[oi]) {
                    ri += 1;
                } else {
                    oi += 1;
                }
            }
            result_len = wi;
            if (result_len == 0) break;
        }

        // Shrink to actual size
        if (result_len < result.len) {
            const shrunk = try self.alloc.realloc(result, result_len);
            return SearchResult{ .file_ids = shrunk, .n_trigrams = @intCast(unique.count()) };
        }
        return SearchResult{ .file_ids = result, .n_trigrams = @intCast(unique.count()) };
    }

    pub const SearchResult = struct {
        file_ids: []u32,
        n_trigrams: u32,
    };

    /// Get the frequency table (for sparse n-gram analysis).
    pub fn getFreqTable(self: *const DiskIndex) *const FreqTable {
        return @ptrCast(@alignCast(self.freq_mmap.ptr));
    }
};

// ─── Sparse N-gram support ──────────────────────────────────────────────────

/// Extract sparse n-grams from a query using the frequency table.
/// Returns covering set — minimal n-grams needed for lookup.
pub fn extractSparseNgrams(
    query: []const u8,
    freq: *const FreqTable,
    out: []SparseNgram,
) u32 {
    if (query.len < 3) return 0;

    // Compute weights for each character pair
    var weights: [1024]u32 = undefined;
    const n_pairs = if (query.len > 1) query.len - 1 else 0;
    var pi: usize = 0;
    while (pi < @min(n_pairs, 1024)) : (pi += 1) {
        weights[pi] = freq.weight(toLower(query[pi]), toLower(query[pi + 1]));
    }

    // Build covering set: find local maxima in weight array
    // These are the "boundaries" of sparse n-grams
    var count: u32 = 0;
    var start: usize = 0;

    pi = 1;
    const max_pairs = @min(n_pairs, 1024);
    while (pi < max_pairs) : (pi += 1) {
        // Is this a local maximum? Both neighbors have lower weight
        const is_boundary = (pi == max_pairs - 1) or
            (weights[pi] >= weights[pi - 1] and weights[pi] >= weights[pi + 1]);

        if (is_boundary or pi - start >= 8) {
            // Emit n-gram from start to pi+2 (include the trigram at boundary)
            const end = @min(pi + 2, query.len);
            if (end - start >= 3 and count < out.len) {
                out[count] = .{ .start = @intCast(start), .len = @intCast(end - start) };
                count += 1;
            }
            start = pi;
        }
    }

    // Emit final n-gram if not covered
    if (start < query.len - 2 and count < out.len) {
        out[count] = .{ .start = @intCast(start), .len = @intCast(query.len - start) };
        count += 1;
    }

    return count;
}

pub const SparseNgram = struct {
    start: u16,
    len: u16,
};

// ─── Helpers ────────────────────────────────────────────────────────────────

fn packTrigram(a: u8, b: u8, c: u8) Trigram {
    return @as(Trigram, a) << 16 | @as(Trigram, b) << 8 | @as(Trigram, c);
}

fn toLower(c: u8) u8 {
    return if (c >= 'A' and c <= 'Z') c + 32 else c;
}

// ─── Tests ──────────────────────────────────────────────────────────────────

test "DiskIndexBuilder roundtrip" {
    const alloc = std.testing.allocator;

    var builder = DiskIndexBuilder.init(alloc);
    defer builder.deinit();

    try builder.indexFile("src/main.ts", "export function handleRequest(req) { return fetch(url); }");
    try builder.indexFile("src/ws.ts", "class WebSocketServer { onMessage(data) {} }");
    try builder.indexFile("src/db.ts", "const pool = createPool({ host: 'localhost', database: 'app' });");

    // Write to temp dir
    const tmp = "/tmp/tdb_disk_test";
    std.fs.cwd().makeDir(tmp) catch {};
    defer std.fs.cwd().deleteTree(tmp) catch {};

    const stats = try builder.writeToDisk(tmp);
    try std.testing.expect(stats.n_trigrams > 0);
    try std.testing.expectEqual(@as(u32, 3), stats.n_files);

    // Open and search
    var idx = try DiskIndex.open(tmp, alloc);
    defer idx.close();

    const r1 = try idx.search("WebSocket");
    defer alloc.free(r1.file_ids);
    try std.testing.expectEqual(@as(usize, 1), r1.file_ids.len);
    const path = idx.filePath(r1.file_ids[0]).?;
    try std.testing.expectEqualStrings("src/ws.ts", path);

    const r2 = try idx.search("handleRequest");
    defer alloc.free(r2.file_ids);
    try std.testing.expectEqual(@as(usize, 1), r2.file_ids.len);

    const r3 = try idx.search("database");
    defer alloc.free(r3.file_ids);
    try std.testing.expectEqual(@as(usize, 1), r3.file_ids.len);

    // Query not in any file
    const r4 = try idx.search("xyznothere");
    defer alloc.free(r4.file_ids);
    try std.testing.expectEqual(@as(usize, 0), r4.file_ids.len);
}

test "FreqTable weighting" {
    var freq = FreqTable{};
    // Record 'th' 100 times, 'qx' 1 time
    var i: u32 = 0;
    while (i < 100) : (i += 1) freq.record('t', 'h');
    freq.record('q', 'x');

    // 'qx' should have much higher weight (rarer)
    const w_th = freq.weight('t', 'h');
    const w_qx = freq.weight('q', 'x');
    try std.testing.expect(w_qx > w_th);
}
