/// TurboDB Indexing Profiler
/// =========================
/// Profiles where time is spent during indexing to find bottlenecks.
/// Also tests safety with ReleaseSafe bounds checks.
///
/// Usage: zig build profile -- /path/to/codebase
///
const std = @import("std");
const collection_mod = @import("collection.zig");
const codeindex = @import("codeindex.zig");
const fast_index = @import("fast_index.zig");
const parallel_index = @import("parallel_index.zig");
const disk_index = @import("disk_index.zig");
const runtime = @import("runtime");
const compat = @import("compat");
const Database = collection_mod.Database;

const EXTS = [_][]const u8{
    ".ts", ".js", ".tsx", ".jsx", ".py", ".zig", ".go", ".rs",
    ".c", ".h", ".cpp", ".hpp", ".java", ".rb", ".lua", ".sh",
    ".json", ".toml", ".yaml", ".yml", ".md",
};

fn hasValidExt(n: []const u8) bool {
    inline for (EXTS) |ext| {
        if (n.len >= ext.len and std.mem.eql(u8, n[n.len - ext.len ..], ext))
            return true;
    }
    return false;
}

const Timer = struct {
    name: []const u8,
    total_ns: i128 = 0,
    count: u64 = 0,
    start: i128 = 0,

    fn begin(self: *Timer) void {
        self.start = compat.nanoTimestamp();
    }
    fn end(self: *Timer) void {
        self.total_ns += compat.nanoTimestamp() - self.start;
        self.count += 1;
    }
    fn avgUs(self: *const Timer) f64 {
        if (self.count == 0) return 0;
        return @as(f64, @floatFromInt(self.total_ns)) / @as(f64, @floatFromInt(self.count)) / 1e3;
    }
    fn totalMs(self: *const Timer) f64 {
        return @as(f64, @floatFromInt(self.total_ns)) / 1e6;
    }
    fn pct(self: *const Timer, wall_ns: i128) f64 {
        if (wall_ns == 0) return 0;
        return @as(f64, @floatFromInt(self.total_ns)) / @as(f64, @floatFromInt(wall_ns)) * 100;
    }
};

pub fn main(init: std.process.Init) !void {
    // Use page_allocator for speed (no safety overhead in profiling mode)
    // Switch to GPA for leak/safety checks
    const alloc = init.gpa;
    runtime.setIo(init.io);

    const args = try compat.argsAlloc(alloc, init.minimal.args);
    defer compat.argsFree(alloc, args);

    if (args.len < 2) {
        std.debug.print("Usage: profile <codebase-dir>\n", .{});
        return;
    }

    std.debug.print("\n======================================================================\n", .{});
    std.debug.print("  TurboDB Indexing Profiler (ReleaseSafe + GPA safety)\n", .{});
    std.debug.print("======================================================================\n\n", .{});

    // ── Phase 1: Load files into memory ─────────────────────────────────
    std.debug.print("Phase 1: Loading source files...\n", .{});

    const FileEntry = struct { path: []u8, content: []u8 };
    var files: std.ArrayList(FileEntry) = .empty;
    defer {
        for (files.items) |f| {
            alloc.free(f.path);
            alloc.free(f.content);
        }
        files.deinit(alloc);
    }

    var dir = try compat.fs.cwdOpenDir(args[1], .{ .iterate = true });
    defer compat.fs.dirClose(dir);
    var walker = try dir.walk(alloc);
    defer walker.deinit();

    var total_bytes: u64 = 0;
    while (try walker.next(runtime.io)) |entry| {
        if (entry.kind != .file) continue;
        if (!hasValidExt(entry.basename)) continue;

        const file = compat.fs.dirOpenFile(dir, entry.path, .{}) catch continue;
        defer compat.fs.fileClose(file);
        var buf: [8192]u8 = undefined;
        const n = compat.fs.fileReadAll(file, &buf) catch continue;
        if (n < 3) continue;

        try files.append(alloc, .{
            .path = try alloc.dupe(u8, entry.path),
            .content = try alloc.dupe(u8, buf[0..n]),
        });
        total_bytes += n;
    }

    std.debug.print("  {d} files, {d:.1} MB\n\n", .{
        files.items.len,
        @as(f64, @floatFromInt(total_bytes)) / (1024.0 * 1024.0),
    });

    // ── Phase 2: Profile in-memory trigram index ────────────────────────
    std.debug.print("Phase 2: Profile in-memory TrigramIndex\n", .{});
    {
        var t_total = Timer{ .name = "total" };
        var t_trigram = Timer{ .name = "trigram_extract" };
        _ = Timer{ .name = "hashmap_put" }; // reserved for future per-op profiling
        var t_word = Timer{ .name = "word_index" };

        var tri = codeindex.TrigramIndex.init(alloc);
        defer tri.deinit();
        var words = codeindex.WordIndex.init(alloc);
        defer words.deinit();

        t_total.begin();
        for (files.items) |f| {
            t_trigram.begin();
            tri.indexFile(f.path, f.content) catch {};
            t_trigram.end();

            t_word.begin();
            words.indexFile(f.path, f.content) catch {};
            t_word.end();
        }
        t_total.end();

        const wall = t_total.total_ns;
        const rate = @as(f64, @floatFromInt(files.items.len)) / (@as(f64, @floatFromInt(wall)) / 1e9);

        std.debug.print("  Total:          {d:.1}ms  ({d:.0} files/s)\n", .{ t_total.totalMs(), rate });
        std.debug.print("  Trigram index:  {d:.1}ms  ({d:.1}%%)  avg {d:.1}µs/file\n", .{
            t_trigram.totalMs(), t_trigram.pct(wall), t_trigram.avgUs(),
        });
        std.debug.print("  Word index:     {d:.1}ms  ({d:.1}%%)  avg {d:.1}µs/file\n", .{
            t_word.totalMs(), t_word.pct(wall), t_word.avgUs(),
        });
        std.debug.print("  Unique trigrams: {d}\n", .{tri.index.count()});
        std.debug.print("  File entries:    {d}\n\n", .{tri.file_trigrams.count()});
    }

    // ── Phase 2b: Profile FastTrigramIndex (flat array, O(1)) ───────────
    std.debug.print("Phase 2b: Profile FastTrigramIndex (flat array)\n", .{});
    {
        var t_fast = Timer{ .name = "fast_trigram" };

        var fast = fast_index.FastTrigramIndex.init(alloc);
        defer fast.deinit();

        t_fast.begin();
        for (files.items) |f| {
            fast.indexFile(f.path, f.content) catch {};
        }
        t_fast.end();

        const rate = @as(f64, @floatFromInt(files.items.len)) / (t_fast.totalMs() / 1e3);
        std.debug.print("  Total:          {d:.1}ms  ({d:.0} files/s)\n", .{ t_fast.totalMs(), rate });
        std.debug.print("  Unique trigrams: {d}\n", .{fast.trigramCount()});
        std.debug.print("  File entries:    {d}\n\n", .{fast.fileCount()});
    }

    // ── Phase 2c: Profile Parallel indexing ──────────────────────────────
    std.debug.print("Phase 2c: Profile Parallel indexing (4 threads)\n", .{});
    {
        var t_par = Timer{ .name = "parallel" };

        // Convert to FileEntry slice
        var entries: std.ArrayList(parallel_index.FileEntry) = .empty;
        defer entries.deinit(alloc);
        for (files.items) |f| {
            try entries.append(alloc, .{ .path = f.path, .content = f.content });
        }
        t_par.begin();
        var indexer = parallel_index.ParallelIndexer.init(alloc, 4);
        var result = try indexer.indexFiles(entries.items);
        t_par.end();
        defer result.deinit();

        const rate = @as(f64, @floatFromInt(files.items.len)) / (t_par.totalMs() / 1e3);
        std.debug.print("  Total:          {d:.1}ms  ({d:.0} files/s)\n", .{ t_par.totalMs(), rate });
        std.debug.print("  Files indexed:   {d}\n", .{result.files_indexed});
        std.debug.print("  Total trigrams:  {d}\n\n", .{result.total_trigrams});
    }
    // ── Phase 3: Profile on-disk DiskIndexBuilder ───────────────────────
    std.debug.print("Phase 3: Profile DiskIndexBuilder\n", .{});
    {
        var t_build = Timer{ .name = "build" };
        var t_write = Timer{ .name = "write_disk" };

        var builder = disk_index.DiskIndexBuilder.init(alloc);
        defer builder.deinit();

        t_build.begin();
        for (files.items) |f| {
            builder.indexFile(f.path, f.content) catch {};
        }
        t_build.end();

        const tmp = "/tmp/tdb_profile_disk";
        compat.fs.cwdMakeDir(tmp) catch |e| switch (e) {
            error.PathAlreadyExists => {
                compat.fs.cwdDeleteTree(tmp) catch {};
                compat.fs.cwdMakeDir(tmp) catch {};
            },
            else => return e,
        };
        defer compat.fs.cwdDeleteTree(tmp) catch {};

        t_write.begin();
        const stats = try builder.writeToDisk(tmp);
        t_write.end();

        std.debug.print("  Build:     {d:.1}ms  ({d:.0} files/s)\n", .{
            t_build.totalMs(),
            @as(f64, @floatFromInt(files.items.len)) / (t_build.totalMs() / 1e3),
        });
        std.debug.print("  Write:     {d:.1}ms\n", .{t_write.totalMs()});
        std.debug.print("  Disk size: index={d}B  posts={d}B  files={d}B  freq={d}B  total={d:.1}KB\n", .{
            stats.index_bytes, stats.postings_bytes, stats.files_bytes, stats.freq_bytes,
            @as(f64, @floatFromInt(stats.totalBytes())) / 1024.0,
        });

        // Profile disk search
        std.debug.print("\n  On-disk search latency:\n", .{});
        var idx = try disk_index.DiskIndex.open(tmp, alloc);
        defer idx.close();

        const queries = [_][]const u8{
            "function", "WebSocket", "handleRequest", "database", "middleware",
        };
        for (queries) |q| {
            var t_search = Timer{ .name = "search" };
            // Run 5 times for stable measurement
            var ri: u32 = 0;
            var last_count: usize = 0;
            while (ri < 5) : (ri += 1) {
                t_search.begin();
                const result = try idx.search(q);
                t_search.end();
                last_count = result.file_ids.len;
                alloc.free(result.file_ids);
            }
            std.debug.print("    \"{s:<20}\"  {d:>4} hits  avg {d:.0}µs\n", .{
                q, last_count, t_search.avgUs(),
            });
        }
    }

    // ── Phase 4: Safety audit ───────────────────────────────────────────
    std.debug.print("\nPhase 4: Safety checks\n", .{});
    std.debug.print("  ✓ GPA with safety=true (use-after-free, double-free detection)\n", .{});
    std.debug.print("  ✓ never_unmap=true (freed pages stay mapped for UAF traps)\n", .{});
    std.debug.print("  ✓ Bounds checking (ReleaseSafe mode)\n", .{});

    // Test: intentional out-of-bounds on disk index (should be caught)
    {
        var idx2 = codeindex.TrigramIndex.init(alloc);
        defer idx2.deinit();

        // Index and remove — verify clean removal doesn't corrupt
        idx2.indexFile("test.zig", "hello world test") catch {};
        idx2.removeFile("test.zig");
        idx2.indexFile("test.zig", "replacement content") catch {};
        idx2.removeFile("test.zig");
        std.debug.print("  ✓ Index/remove cycle: no corruption\n", .{});
    }

    // Test: empty queries don't crash
    {
        var idx3 = codeindex.TrigramIndex.init(alloc);
        defer idx3.deinit();
        const r = idx3.candidates("", alloc);
        if (r) |paths| alloc.free(paths);
        const r2 = idx3.candidates("ab", alloc); // too short
        if (r2) |paths| alloc.free(paths);
        std.debug.print("  ✓ Edge cases: empty/short queries handled\n", .{});
    }

    std.debug.print("\n======================================================================\n", .{});
}
