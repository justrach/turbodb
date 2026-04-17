/// TurboDB Scale Benchmark
/// =======================
/// Simulates 20x OpenClaw (~60K files, ~2.5GB index) to test if the trigram
/// index degrades at scale. Measures: index time, memory, search latency,
/// selectivity, and throughput.
///
/// Usage: zig build scale-bench -- /path/to/openclaw/src
///
const std = @import("std");
const collection_mod = @import("collection.zig");
const codeindex = @import("codeindex.zig");
const runtime = @import("runtime");
const compat = @import("compat");
const Database = collection_mod.Database;
const Collection = collection_mod.Collection;

const COPIES = 20;
const EXTS = [_][]const u8{
    ".ts", ".js", ".tsx", ".jsx", ".py", ".zig", ".go", ".rs",
    ".c", ".h", ".cpp", ".hpp", ".java", ".rb", ".lua", ".sh",
    ".json", ".toml", ".yaml", ".yml", ".md",
};

fn hasValidExt(name_: []const u8) bool {
    inline for (EXTS) |ext| {
        if (name_.len >= ext.len and std.mem.eql(u8, name_[name_.len - ext.len ..], ext))
            return true;
    }
    return false;
}

pub fn main(init: std.process.Init) !void {
    const alloc = init.gpa;
    runtime.setIo(init.io);

    const args = try compat.argsAlloc(alloc, init.minimal.args);
    defer compat.argsFree(alloc, args);

    if (args.len < 2) {
        std.debug.print("Usage: scale-bench <codebase-dir>\n", .{});
        return;
    }
    const src_dir = args[1];

    // ── Phase 0: Walk and cache source files in memory ──────────────────
    std.debug.print("\n{s}\n", .{"=" ** 70});
    std.debug.print("  TurboDB Scale Benchmark — {d}x OpenClaw simulation\n", .{COPIES});
    std.debug.print("{s}\n\n", .{"=" ** 70});

    std.debug.print("Phase 0: Loading source files into memory...\n", .{});
    const FileEntry = struct { path: []const u8, content: []const u8 };
    var files: std.ArrayList(FileEntry) = .empty;
    defer {
        for (files.items) |f| {
            alloc.free(f.path);
            alloc.free(f.content);
        }
        files.deinit(alloc);
    }

    var dir = try std.fs.cwd().openDir(src_dir, .{ .iterate = true });
    defer dir.close();
    var walker = try dir.walk(alloc);
    defer walker.deinit();

    while (try walker.next()) |entry| {
        if (entry.kind != .file) continue;
        if (!hasValidExt(entry.basename)) continue;

        var file = dir.openFile(entry.path, .{}) catch continue;
        defer file.close();
        var buf: [8192]u8 = undefined;
        const n = file.read(&buf) catch continue;
        if (n < 3) continue;

        try files.append(alloc, .{
            .path = try alloc.dupe(u8, entry.path),
            .content = try alloc.dupe(u8, buf[0..n]),
        });
    }

    const base_files = files.items.len;
    std.debug.print("  Base: {d} source files loaded\n", .{base_files});
    std.debug.print("  Simulating {d} copies = {d} total files\n\n", .{ COPIES, base_files * COPIES });

    // ── Phase 1: Index at scale ─────────────────────────────────────────
    std.debug.print("Phase 1: Indexing {d} files...\n", .{base_files * COPIES});

    const data_dir = "/tmp/turbodb_scale_bench";
    std.fs.cwd().makeDir(data_dir) catch |e| switch (e) {
        error.PathAlreadyExists => {
            // Clean it
            std.fs.cwd().deleteTree(data_dir) catch {};
            std.fs.cwd().makeDir(data_dir) catch {};
        },
        else => return e,
    };

    const db = try Database.open(alloc, data_dir);
    defer db.close();
    const col = try db.collection("scale");

    var indexed: u64 = 0;
    var total_bytes: u64 = 0;
    const t_index_start = std.time.nanoTimestamp();

    var copy: u32 = 0;
    while (copy < COPIES) : (copy += 1) {
        for (files.items) |f| {
            // Prefix path with copy number to make unique
            var path_buf: [512]u8 = undefined;
            const prefixed = std.fmt.bufPrint(&path_buf, "repo{d:02}/{s}", .{ copy, f.path }) catch continue;
            const stable_path = alloc.dupe(u8, prefixed) catch continue;

            _ = col.insert(stable_path, f.content) catch continue;
            indexed += 1;
            total_bytes += f.content.len;
        }
        const elapsed_ns = std.time.nanoTimestamp() - t_index_start;
        const elapsed_s = @as(f64, @floatFromInt(elapsed_ns)) / 1e9;
        const rate = @as(f64, @floatFromInt(indexed)) / elapsed_s;
        std.debug.print("\r  Copy {d}/{d}: {d} files indexed ({d:.0} files/s)  ", .{
            copy + 1, COPIES, indexed, rate,
        });
    }

    const t_index_end = std.time.nanoTimestamp();
    const index_s = @as(f64, @floatFromInt(t_index_end - t_index_start)) / 1e9;
    const index_rate = @as(f64, @floatFromInt(indexed)) / index_s;
    const total_mb = @as(f64, @floatFromInt(total_bytes)) / (1024.0 * 1024.0);

    std.debug.print("\n\n  Indexed: {d} files, {d:.1} MB content\n", .{ indexed, total_mb });
    std.debug.print("  Time:    {d:.2}s ({d:.0} files/s)\n", .{ index_s, index_rate });

    // Trigram index stats
    const tri_count = col.tri.index.count();
    const file_count = col.tri.file_trigrams.count();
    std.debug.print("  Trigram index: {d} unique trigrams, {d} file entries\n", .{ tri_count, file_count });

    // ── Phase 2: Search latency at scale ────────────────────────────────
    std.debug.print("\nPhase 2: Search latency ({d} indexed files)\n\n", .{indexed});

    const queries = [_][]const u8{
        "function",       // very common
        "import",         // very common
        "WebSocket",      // medium
        "handleRequest",  // specific
        "authentication", // specific
        "database",       // rare
        "middleware",     // rare
        "createAgent",    // very specific
        "GatewayServer",  // unique
        "export default class", // multi-word
    };

    var total_us: f64 = 0;
    var total_hits: u64 = 0;
    var total_cand: u64 = 0;

    std.debug.print("  {s:<25} {s:>5} {s:>7} {s:>8} {s:>10}\n", .{
        "Query", "Hits", "Cands", "Scan%", "Time",
    });
    std.debug.print("  {s} {s} {s} {s} {s}\n", .{
        "-" ** 25, "-" ** 5, "-" ** 7, "-" ** 8, "-" ** 10,
    });

    for (queries) |q| {
        // Run 3 times, take median
        var times: [3]f64 = undefined;
        var last_result: ?Collection.TextSearchResult = null;

        var run: u32 = 0;
        while (run < 3) : (run += 1) {
            if (last_result) |r| r.deinit();
            const t0 = std.time.nanoTimestamp();
            last_result = try col.searchText(q, 50, alloc);
            const elapsed_ns = std.time.nanoTimestamp() - t0;
            times[run] = @as(f64, @floatFromInt(elapsed_ns)) / 1e3;
        }

        // Sort for median
        std.mem.sort(f64, &times, {}, std.sort.asc(f64));
        const median_us = times[1];

        const result = last_result.?;
        const n_cand = result.candidate_paths.len;
        const total_files = result.total_files;
        const selectivity = if (total_files > 0)
            @as(f64, @floatFromInt(n_cand)) / @as(f64, @floatFromInt(total_files)) * 100
        else
            @as(f64, 0);

        total_us += median_us;
        total_hits += result.docs.len;
        total_cand += n_cand;

        // Format time
        var time_buf: [16]u8 = undefined;
        const time_str = if (median_us >= 1000)
            std.fmt.bufPrint(&time_buf, "{d:.1}ms", .{median_us / 1000}) catch "?"
        else
            std.fmt.bufPrint(&time_buf, "{d:.0}µs", .{median_us}) catch "?";

        std.debug.print("  \"{s}\"{s} {d:>5} {d:>7} {d:>7.1}%% {s:>10}\n", .{
            q,
            // Pad to 25 chars
            (" " ** 24)[0..@min(24, 24 -| q.len)],
            result.docs.len,
            n_cand,
            selectivity,
            time_str,
        });
        result.deinit();
    }

    const avg_us = total_us / @as(f64, queries.len);
    const qps = @as(f64, queries.len) / (total_us / 1e6);

    std.debug.print("\n  {s}\n", .{"─" ** 60});
    std.debug.print("  Avg latency: {d:.0}µs/query  ({d:.0} queries/sec)\n", .{ avg_us, qps });
    std.debug.print("  Total hits: {d}  Total candidates: {d}\n", .{ total_hits, total_cand });

    // ── Phase 3: Throughput burst ───────────────────────────────────────
    std.debug.print("\nPhase 3: Sustained throughput (1000 random queries)\n", .{});

    const burst_queries = [_][]const u8{
        "function", "import", "const", "return", "export",
        "class", "async", "await", "error", "config",
        "WebSocket", "handleRequest", "authentication",
        "database", "middleware", "createAgent", "require",
        "Promise", "setTimeout", "addEventListener",
    };

    const t_burst_start = std.time.nanoTimestamp();
    var burst_count: u32 = 0;
    var rng = std.Random.DefaultPrng.init(@intCast(std.time.nanoTimestamp()));
    const random = rng.random();

    while (burst_count < 1000) : (burst_count += 1) {
        const q = burst_queries[random.intRangeAtMost(usize, 0, burst_queries.len - 1)];
        const result = try col.searchText(q, 10, alloc);
        result.deinit();
    }

    const t_burst_end = std.time.nanoTimestamp();
    const burst_s = @as(f64, @floatFromInt(t_burst_end - t_burst_start)) / 1e9;
    const burst_qps = 1000.0 / burst_s;

    std.debug.print("  1000 queries in {d:.2}s = {d:.0} queries/sec\n", .{ burst_s, burst_qps });

    // ── Summary ─────────────────────────────────────────────────────────
    std.debug.print("\n{s}\n", .{"=" ** 70});
    std.debug.print("  SCALE SUMMARY\n", .{});
    std.debug.print("{s}\n", .{"=" ** 70});
    std.debug.print("  Files indexed:     {d}\n", .{indexed});
    std.debug.print("  Content size:      {d:.1} MB\n", .{total_mb});
    std.debug.print("  Unique trigrams:   {d}\n", .{tri_count});
    std.debug.print("  Index time:        {d:.2}s ({d:.0} files/s)\n", .{ index_s, index_rate });
    std.debug.print("  Median search:     {d:.0}µs\n", .{avg_us});
    std.debug.print("  Burst throughput:  {d:.0} queries/sec\n", .{burst_qps});
    std.debug.print("  Selectivity:       best {d:.1}%%, worst {d:.1}%%\n", .{
        if (total_cand > 0) @as(f64, 1) / @as(f64, @floatFromInt(indexed)) * 100 else @as(f64, 0),
        @as(f64, @floatFromInt(total_cand)) / @as(f64, queries.len) / @as(f64, @floatFromInt(indexed)) * 100,
    });
    std.debug.print("{s}\n\n", .{"=" ** 70});
}
