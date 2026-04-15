/// tdb — TurboDB native CLI
/// =========================
/// Direct engine access, zero network overhead. Indexes codebases and searches them.
///
/// Usage:
///   tdb index <dir>                    index a codebase directory
///   tdb search <query>                 trigram search
///   tdb bench <dir>                    index + benchmark search
///   tdb get <key>                      get a document by key
///   tdb insert <key> <value>           insert a document
///   tdb --data <dir>                   data directory (default: ./turbodb_data)
///
const std = @import("std");
const collection_mod = @import("collection.zig");
const codeindex = @import("codeindex.zig");
const Database = collection_mod.Database;
const Collection = collection_mod.Collection;

const EXTS = [_][]const u8{
    ".ts", ".js", ".tsx", ".jsx", ".py", ".zig", ".go", ".rs",
    ".c", ".h", ".cpp", ".hpp", ".java", ".rb", ".lua", ".sh",
    ".json", ".toml", ".yaml", ".yml", ".md",
};

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const alloc = gpa.allocator();

    const args = try std.process.argsAlloc(alloc);
    defer std.process.argsFree(alloc, args);

    var data_dir: []const u8 = "./turbodb_data";
    var col_name: []const u8 = "code";
    var cmd: ?[]const u8 = null;
    var cmd_args: [8][]const u8 = undefined;
    var cmd_argc: usize = 0;

    // Parse args
    var i: usize = 1;
    while (i < args.len) : (i += 1) {
        if (std.mem.eql(u8, args[i], "--data") and i + 1 < args.len) {
            i += 1;
            data_dir = args[i];
        } else if (std.mem.eql(u8, args[i], "--col") and i + 1 < args.len) {
            i += 1;
            col_name = args[i];
        } else if (std.mem.eql(u8, args[i], "--help")) {
            printHelp();
            return;
        } else if (cmd == null) {
            cmd = args[i];
        } else if (cmd_argc < 8) {
            cmd_args[cmd_argc] = args[i];
            cmd_argc += 1;
        }
    }

    if (cmd == null) {
        printHelp();
        return;
    }

    // Ensure data dir
    std.fs.cwd().makeDir(data_dir) catch |e| switch (e) {
        error.PathAlreadyExists => {},
        else => return e,
    };

    const db = try Database.open(alloc, data_dir);
    defer db.close();

    const command = cmd.?;

    if (std.mem.eql(u8, command, "index")) {
        if (cmd_argc < 1) {
            std.debug.print("Usage: tdb index <directory>\n", .{});
            return;
        }
        try cmdIndex(db, col_name, cmd_args[0], alloc);
    } else if (std.mem.eql(u8, command, "search")) {
        if (cmd_argc < 1) {
            std.debug.print("Usage: tdb search <query>\n", .{});
            return;
        }
        try cmdSearch(db, col_name, cmd_args[0], alloc);
    } else if (std.mem.eql(u8, command, "bench")) {
        if (cmd_argc < 1) {
            std.debug.print("Usage: tdb bench <directory>\n", .{});
            return;
        }
        try cmdBench(db, col_name, cmd_args[0], alloc);
    } else if (std.mem.eql(u8, command, "get")) {
        if (cmd_argc < 1) {
            std.debug.print("Usage: tdb get <key>\n", .{});
            return;
        }
        try cmdGet(db, col_name, cmd_args[0]);
    } else if (std.mem.eql(u8, command, "word")) {
        if (cmd_argc < 1) {
            std.debug.print("Usage: tdb word <identifier>\n", .{});
            return;
        }
        try cmdWord(db, col_name, cmd_args[0], alloc);
    } else if (std.mem.eql(u8, command, "insert")) {
        if (cmd_argc < 2) {
            std.debug.print("Usage: tdb insert <key> <value>\n", .{});
            return;
        }
        try cmdInsert(db, col_name, cmd_args[0], cmd_args[1]);
    } else {
        std.debug.print("Unknown command: {s}\n", .{command});
        printHelp();
    }
}

// ─── Commands ────────────────────────────────────────────────────────────────

fn cmdIndex(db: *Database, col_name: []const u8, dir_path: []const u8, alloc: std.mem.Allocator) !void {
    const col = try db.collection(col_name);

    var dir = std.fs.cwd().openDir(dir_path, .{ .iterate = true }) catch |e| {
        std.debug.print("Cannot open directory: {any}\n", .{e});
        return;
    };
    defer dir.close();

    var indexed: u64 = 0;
    var skipped: u64 = 0;
    const t0 = std.time.nanoTimestamp();

    var walker = try dir.walk(alloc);
    defer walker.deinit();

    while (try walker.next()) |entry| {
        if (entry.kind != .file) continue;
        if (!hasValidExt(entry.basename)) continue;

        // Read file content (first 8KB for indexing)
        var file = dir.openFile(entry.path, .{}) catch {
            skipped += 1;
            continue;
        };
        defer file.close();

        var buf: [8192]u8 = undefined;
        const n = file.read(&buf) catch {
            skipped += 1;
            continue;
        };
        if (n < 3) {
            skipped += 1;
            continue;
        }
        // Dupe path — walker buffer is reused, but trigram index stores pointers
        const stable_path = alloc.dupe(u8, entry.path) catch {
            skipped += 1;
            continue;
        };
        _ = col.insert(stable_path, buf[0..n]) catch {
            skipped += 1;
            continue;
        };
        indexed += 1;

        if (indexed % 1000 == 0) {
            const elapsed_ns = std.time.nanoTimestamp() - t0;
            const elapsed_s = @as(f64, @floatFromInt(elapsed_ns)) / 1e9;
            const rate = @as(f64, @floatFromInt(indexed)) / elapsed_s;
            std.debug.print("\r  Indexed {d} files ({d:.0} files/s)...", .{ indexed, rate });
        }
    }

    const elapsed_ns = std.time.nanoTimestamp() - t0;
    const elapsed_s = @as(f64, @floatFromInt(elapsed_ns)) / 1e9;
    const rate = @as(f64, @floatFromInt(indexed)) / elapsed_s;

    std.debug.print("\r  Indexed {d} files in {d:.2}s ({d:.0} files/s)  skipped {d}\n", .{
        indexed, elapsed_s, rate, skipped,
    });
}

fn cmdSearch(db: *Database, col_name: []const u8, query: []const u8, alloc: std.mem.Allocator) !void {
    const col = try db.collection(col_name);

    const t0 = std.time.nanoTimestamp();
    const result = try col.searchText(query, 20, alloc);
    defer result.deinit();
    const elapsed_ns = std.time.nanoTimestamp() - t0;
    const elapsed_us = @as(f64, @floatFromInt(elapsed_ns)) / 1e3;

    const n_cand = result.candidate_paths.len;
    const total = result.total_files;
    const selectivity = if (total > 0)
        @as(f64, @floatFromInt(n_cand)) / @as(f64, @floatFromInt(total)) * 100
    else
        @as(f64, 0);

    std.debug.print("\n  Query:      \"{s}\"\n", .{query});
    std.debug.print("  Candidates: {d} / {d} ({d:.1}%% scanned)\n", .{ n_cand, total, selectivity });
    std.debug.print("  Hits:       {d}\n", .{result.docs.len});
    std.debug.print("  Time:       {d:.0} µs\n\n", .{elapsed_us});

    for (result.docs, 0..) |d, idx| {
        const path = d.key;
        const snippet = findSnippet(d.value, query);
        std.debug.print("  {d:>3}. {s}\n", .{ idx + 1, path });
        if (snippet.len > 0) {
            std.debug.print("       {s}\n", .{snippet});
        }
    }
    if (result.docs.len == 0) {
        std.debug.print("  (no results)\n", .{});
    }
}

fn cmdWord(db: *Database, col_name: []const u8, word: []const u8, alloc: std.mem.Allocator) !void {
    const col = try db.collection(col_name);
    const hits = try col.searchWord(word, alloc);
    defer if (hits.len > 0) alloc.free(hits);
    if (hits.len == 0) {
        std.debug.print("  No hits for \"{s}\"\n", .{word});
        return;
    }
    std.debug.print("\n  Word: \"{s}\"  ({d} hits)\n\n", .{ word, hits.len });
    for (hits) |hit| {
        std.debug.print("  {s}:{d}\n", .{ hit.path, hit.line_num });
    }
}

fn cmdBench(db: *Database, col_name: []const u8, dir_path: []const u8, alloc: std.mem.Allocator) !void {
    std.debug.print("\n=== TurboDB Native Bench ===\n\n", .{});

    // Phase 1: Index
    std.debug.print("Phase 1: Index\n", .{});
    try cmdIndex(db, col_name, dir_path, alloc);

    const col = try db.collection(col_name);

    // Phase 2: Search benchmark
    std.debug.print("\nPhase 2: Search benchmark\n", .{});
    const queries = [_][]const u8{
        "function", "import", "const", "return", "export",
        "class", "async", "await", "error", "config",
        "WebSocket", "handleRequest", "authentication",
        "database", "middleware",
    };

    var total_us: f64 = 0;
    var total_hits: u64 = 0;
    var total_candidates: u64 = 0;

    for (queries) |q| {
        const t0 = std.time.nanoTimestamp();
        const result = try col.searchText(q, 50, alloc);
        const elapsed_ns = std.time.nanoTimestamp() - t0;
        const elapsed_us = @as(f64, @floatFromInt(elapsed_ns)) / 1e3;
        total_us += elapsed_us;
        total_hits += result.docs.len;
        const n_cand = result.candidate_paths.len;
        total_candidates += n_cand;

        const total_files = result.total_files;
        const selectivity = if (total_files > 0)
            @as(f64, @floatFromInt(n_cand)) / @as(f64, @floatFromInt(total_files)) * 100
        else
            @as(f64, 0);

        std.debug.print("  \"{s:<20}\"  {d:>4} hits  {d:>5} candidates  {d:>5.1}%% scan  {d:>8.0} µs\n", .{
            q, result.docs.len, n_cand, selectivity, elapsed_us,
        });
        result.deinit();
    }

    const avg_us = total_us / @as(f64, queries.len);
    const ops_sec = @as(f64, queries.len) / (total_us / 1e6);
    std.debug.print("\n  Total: {d} queries in {d:.0} µs\n", .{ queries.len, total_us });
    std.debug.print("  Avg:   {d:.0} µs/query  ({d:.0} queries/sec)\n", .{ avg_us, ops_sec });
    std.debug.print("  Hits:  {d}  Candidates: {d}\n\n", .{ total_hits, total_candidates });
}

fn cmdGet(db: *Database, col_name: []const u8, key: []const u8) !void {
    const col = try db.collection(col_name);
    const d = col.get(key) orelse {
        std.debug.print("not found: {s}\n", .{key});
        return;
    };
    std.debug.print("{s}\n", .{d.value});
}

fn cmdInsert(db: *Database, col_name: []const u8, key: []const u8, value: []const u8) !void {
    const col = try db.collection(col_name);
    const doc_id = try col.insert(key, value);
    std.debug.print("inserted doc_id={d} key={s}\n", .{ doc_id, key });
}

// ─── Helpers ─────────────────────────────────────────────────────────────────

fn hasValidExt(name: []const u8) bool {
    inline for (EXTS) |ext| {
        if (name.len >= ext.len and std.mem.eql(u8, name[name.len - ext.len ..], ext))
            return true;
    }
    return false;
}

fn findSnippet(value: []const u8, query: []const u8) []const u8 {
    if (query.len == 0 or value.len < query.len) return "";
    // Simple case-insensitive search
    var vi: usize = 0;
    while (vi + query.len <= value.len) : (vi += 1) {
        var match = true;
        var qi: usize = 0;
        while (qi < query.len) : (qi += 1) {
            const vc = value[vi + qi];
            const qc = query[qi];
            const vl = if (vc >= 'A' and vc <= 'Z') vc + 32 else vc;
            const ql = if (qc >= 'A' and qc <= 'Z') qc + 32 else qc;
            if (vl != ql) {
                match = false;
                break;
            }
        }
        if (match) {
            // Return context around match
            const start = if (vi > 40) vi - 40 else 0;
            const end = @min(vi + query.len + 40, value.len);
            // Find line boundaries
            var ls = start;
            while (ls < vi and value[ls] != '\n') ls += 1;
            if (value[ls] == '\n' and ls < vi) ls += 1;
            var le = end;
            while (le < value.len and value[le] != '\n') le += 1;
            return std.mem.trimRight(u8, value[ls..le], " \t\r\n");
        }
    }
    return "";
}

fn printHelp() void {
    std.debug.print(
        \\tdb — TurboDB native CLI (zero network overhead)
        \\
        \\Usage:
        \\  tdb index <dir>              Index all source files in directory
        \\  tdb search <query>           Trigram-indexed full-text search
        \\  tdb bench <dir>              Index + benchmark search performance
        \\  tdb get <key>                Get a document by key
        \\  tdb insert <key> <value>     Insert a document
        \\
        \\Options:
        \\  --data <dir>    Data directory (default: ./turbodb_data)
        \\  --col <name>    Collection name (default: code)
        \\  --help          Show this help
        \\
        \\Examples:
        \\  tdb index ~/projects/openclaw
        \\  tdb search "WebSocket"
        \\  tdb search "handleRequest"
        \\  tdb bench ~/projects/openclaw
        \\
    , .{});
}
