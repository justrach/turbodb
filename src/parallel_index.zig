const std = @import("std");

// ── Types ───────────────────────────────────────────────────────

pub const Trigram = u24;

pub const FileEntry = struct {
    path: []const u8,
    content: []const u8,
};

/// Per-file trigram result from a worker thread.
const FileTrigramResult = struct {
    path: []const u8,
    trigrams: []Trigram,
};

/// Thread-local collection of results built by one worker.
const LocalResults = struct {
    items: std.ArrayList(FileTrigramResult),
    arena: std.heap.ArenaAllocator,

    fn init(backing: std.mem.Allocator) LocalResults {
        return .{
            .items = .empty,
            .arena = std.heap.ArenaAllocator.init(backing),
        };
    }

    fn deinit(self: *LocalResults, backing: std.mem.Allocator) void {
        self.items.deinit(backing);
        self.arena.deinit();
    }
};

// ── Helpers ─────────────────────────────────────────────────────

fn normalizeChar(c: u8) u8 {
    return if (c >= 'A' and c <= 'Z') c + 32 else c;
}

fn packTrigram(a: u8, b: u8, c: u8) Trigram {
    return @as(Trigram, a) << 16 | @as(Trigram, b) << 8 | @as(Trigram, c);
}

/// Extract unique trigrams from content into arena-allocated slice.
fn extractTrigrams(content: []const u8, arena: std.mem.Allocator) ![]Trigram {
    if (content.len < 3) return &.{};

    var seen = std.AutoHashMap(Trigram, void).init(arena);
    defer seen.deinit();

    const limit = content.len - 2;
    var i: usize = 0;
    while (i < limit) : (i += 1) {
        const tri = packTrigram(
            normalizeChar(content[i]),
            normalizeChar(content[i + 1]),
            normalizeChar(content[i + 2]),
        );
        _ = try seen.getOrPut(tri);
    }

    const result = try arena.alloc(Trigram, seen.count());
    var iter = seen.keyIterator();
    var idx: usize = 0;
    while (iter.next()) |key| {
        result[idx] = key.*;
        idx += 1;
    }
    return result;
}

// ── ParallelIndexer ─────────────────────────────────────────────

pub const MergedIndex = struct {
    /// trigram -> list of file paths that contain it
    map: std.AutoHashMap(Trigram, std.ArrayList([]const u8)),
    allocator: std.mem.Allocator,
    total_trigrams: u64,
    files_indexed: u64,

    pub fn deinit(self: *MergedIndex) void {
        var iter = self.map.iterator();
        while (iter.next()) |entry| {
            entry.value_ptr.deinit(self.allocator);
        }
        self.map.deinit();
    }

    /// Return file paths containing the given trigram.
    pub fn lookup(self: *const MergedIndex, tri: Trigram) []const []const u8 {
        if (self.map.getPtr(tri)) |list| {
            return list.items;
        }
        return &.{};
    }
};

pub const ParallelIndexer = struct {
    n_threads: usize,
    allocator: std.mem.Allocator,

    pub fn init(allocator: std.mem.Allocator, n_threads: usize) ParallelIndexer {
        const actual = if (n_threads == 0) 1 else n_threads;
        return .{
            .n_threads = actual,
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *ParallelIndexer) void {
        _ = self;
    }

    /// Index files in parallel, returning a merged trigram -> files map.
    pub fn indexFiles(self: *ParallelIndexer, files: []const FileEntry) !MergedIndex {
        const n = @min(self.n_threads, files.len);
        if (n == 0) {
            return MergedIndex{
                .map = std.AutoHashMap(Trigram, std.ArrayList([]const u8)).init(self.allocator),
                .allocator = self.allocator,
                .total_trigrams = 0,
                .files_indexed = 0,
            };
        }

        // Allocate per-thread local results and thread handles.
        const locals = try self.allocator.alloc(LocalResults, n);
        defer self.allocator.free(locals);

        const threads = try self.allocator.alloc(std.Thread, n);
        defer self.allocator.free(threads);

        // Partition files into chunks and spawn threads.
        const chunk_size = files.len / n;
        const remainder = files.len % n;

        var offset: usize = 0;
        var t: usize = 0;
        while (t < n) : (t += 1) {
            locals[t] = LocalResults.init(self.allocator);
            const extra: usize = if (t < remainder) 1 else 0;
            const this_chunk = chunk_size + extra;
            const chunk = files[offset .. offset + this_chunk];
            offset += this_chunk;

            threads[t] = try std.Thread.spawn(.{}, workerFn, .{ &locals[t], chunk, self.allocator });
        }

        // Join all threads.
        t = 0;
        while (t < n) : (t += 1) {
            threads[t].join();
        }

        // Merge phase: combine all local results into a single map.
        var merged = MergedIndex{
            .map = std.AutoHashMap(Trigram, std.ArrayList([]const u8)).init(self.allocator),
            .allocator = self.allocator,
            .total_trigrams = 0,
            .files_indexed = 0,
        };
        errdefer merged.deinit();

        t = 0;
        while (t < n) : (t += 1) {
            for (locals[t].items.items) |result| {
                merged.files_indexed += 1;
                for (result.trigrams) |tri| {
                    merged.total_trigrams += 1;
                    const gop = try merged.map.getOrPut(tri);
                    if (!gop.found_existing) {
                        gop.value_ptr.* = .empty;
                    }
                    try gop.value_ptr.append(self.allocator, result.path);
                }
            }
            locals[t].deinit(self.allocator);
        }

        return merged;
    }

    fn workerFn(local: *LocalResults, chunk: []const FileEntry, backing: std.mem.Allocator) void {
        const arena_alloc = local.arena.allocator();
        for (chunk) |file| {
            const trigrams = extractTrigrams(file.content, arena_alloc) catch continue;
            local.items.append(backing, .{
                .path = file.path,
                .trigrams = trigrams,
            }) catch continue;
        }
    }
};

// ── Tests ───────────────────────────────────────────────────────

test "parallel indexer - 100 synthetic files across 4 threads" {
    const alloc = std.testing.allocator;

    // Generate 100 synthetic files with deterministic content.
    const n_files = 100;
    var files: [n_files]FileEntry = undefined;

    var content_bufs: [n_files][64]u8 = undefined;
    var i: usize = 0;
    while (i < n_files) : (i += 1) {
        const written = std.fmt.bufPrint(&content_bufs[i], "file_{d:0>3} function handler() {{ return {d}; }}", .{ i, i * 7 }) catch unreachable;
        files[i] = .{
            .path = written[0..8], // "file_NNN"
            .content = written,
        };
    }

    var indexer = ParallelIndexer.init(alloc, 4);
    defer indexer.deinit();

    var merged = try indexer.indexFiles(&files);
    defer merged.deinit();

    // All 100 files should be indexed.
    try std.testing.expectEqual(@as(u64, 100), merged.files_indexed);

    // Total unique trigrams should be > 0.
    try std.testing.expect(merged.total_trigrams > 0);

    // "fun" trigram (from "function") should appear in all files.
    const fun_tri = packTrigram('f', 'u', 'n');
    const fun_files = merged.lookup(fun_tri);
    try std.testing.expectEqual(@as(usize, 100), fun_files.len);

    // "han" trigram (from "handler") should also appear in all files.
    const han_tri = packTrigram('h', 'a', 'n');
    const han_files = merged.lookup(han_tri);
    try std.testing.expectEqual(@as(usize, 100), han_files.len);

    // A trigram that shouldn't exist.
    const xyz_tri = packTrigram('x', 'y', 'z');
    const xyz_files = merged.lookup(xyz_tri);
    try std.testing.expectEqual(@as(usize, 0), xyz_files.len);
}

test "parallel indexer - empty input" {
    const alloc = std.testing.allocator;
    var indexer = ParallelIndexer.init(alloc, 4);
    defer indexer.deinit();

    const empty: [0]FileEntry = .{};
    var merged = try indexer.indexFiles(&empty);
    defer merged.deinit();

    try std.testing.expectEqual(@as(u64, 0), merged.files_indexed);
}

test "parallel indexer - single thread" {
    const alloc = std.testing.allocator;

    const files = [_]FileEntry{
        .{ .path = "a.zig", .content = "hello world" },
        .{ .path = "b.zig", .content = "hello there" },
    };

    var indexer = ParallelIndexer.init(alloc, 1);
    defer indexer.deinit();

    var merged = try indexer.indexFiles(&files);
    defer merged.deinit();

    try std.testing.expectEqual(@as(u64, 2), merged.files_indexed);

    // "hel" should be in both files
    const hel_tri = packTrigram('h', 'e', 'l');
    const hel_files = merged.lookup(hel_tri);
    try std.testing.expectEqual(@as(usize, 2), hel_files.len);
}
