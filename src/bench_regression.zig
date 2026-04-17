const std = @import("std");
const client = @import("client.zig");
const compression = @import("compression.zig");
const art_mod = @import("art.zig");
const query_mod = @import("query.zig");
const lsm_mod = @import("lsm.zig");
const columnar_mod = @import("columnar.zig");
const mvcc_mod = @import("mvcc.zig");
const btree_mod = @import("btree.zig");
const runtime = @import("runtime");
const compat = @import("compat");

// ═══════════════════════════════════════════════════════════════════════════════
// TurboDB Regression Benchmark
//
// Comprehensive benchmark covering all subsystems.
// Outputs a human-readable table during execution and JSON at the end.
// ═══════════════════════════════════════════════════════════════════════════════

const alloc = std.heap.c_allocator;
const DATA_DIR = "/tmp/turbodb_regression_bench";
const LSM_DIR = "/tmp/turbodb_regression_bench_lsm";

// ─── Result accumulator ─────────────────────────────────────────────────────

const MAX_RESULTS = 32;

const BenchResult = struct {
    name: []const u8,
    json_key: []const u8,
    ops: usize,
    elapsed_ns: i128,

    fn opsPerSec(self: BenchResult) f64 {
        const ns_f: f64 = @floatFromInt(self.elapsed_ns);
        return @as(f64, @floatFromInt(self.ops)) / (ns_f / 1e9);
    }

    fn usPerOp(self: BenchResult) f64 {
        const ns_f: f64 = @floatFromInt(self.elapsed_ns);
        return ns_f / @as(f64, @floatFromInt(self.ops)) / 1000.0;
    }
};

var results: [MAX_RESULTS]BenchResult = undefined;
var result_count: usize = 0;

fn record(name: []const u8, json_key: []const u8, ops: usize, elapsed_ns: i128) void {
    const r = BenchResult{ .name = name, .json_key = json_key, .ops = ops, .elapsed_ns = elapsed_ns };
    results[result_count] = r;
    result_count += 1;

    // Print immediately
    const ops_s = r.opsPerSec();
    const us_op = r.usPerOp();
    std.debug.print("  {s:<24}: {d:>12.0} ops/s  ({d:.1} us/op)\n", .{ name, ops_s, us_op });
}

// ─── Timing helper ──────────────────────────────────────────────────────────

inline fn now() i128 {
    return std.time.nanoTimestamp();
}

// ─── Benchmark: Core Path (INSERT / GET / UPDATE / DELETE) ──────────────────

fn benchCorePath() !void {
    compat.fs.cwdDeleteTree(DATA_DIR) catch {};
    var db = try client.Db.open(alloc, DATA_DIR);
    defer db.close();

    const N: usize = 50_000;
    var key_buf: [32]u8 = undefined;
    const value = "{\"x\":1}";

    // INSERT
    {
        const t0 = now();
        for (0..N) |i| {
            const k = std.fmt.bufPrint(&key_buf, "k{d:0>9}", .{i}) catch continue;
            _ = db.insert("bench", k, value) catch continue;
        }
        record("Core INSERT", "core_insert_ops_sec", N, now() - t0);
    }

    // GET (random)
    {
        var rng = std.Random.DefaultPrng.init(42);
        const t0 = now();
        for (0..N) |_| {
            const idx = rng.random().intRangeAtMost(usize, 0, N - 1);
            const k = std.fmt.bufPrint(&key_buf, "k{d:0>9}", .{idx}) catch continue;
            _ = db.get("bench", k) catch null;
        }
        record("Core GET", "core_get_ops_sec", N, now() - t0);
    }

    // UPDATE
    {
        const new_value = "{\"x\":2}";
        const t0 = now();
        for (0..N) |i| {
            const k = std.fmt.bufPrint(&key_buf, "k{d:0>9}", .{i}) catch continue;
            _ = db.update("bench", k, new_value) catch continue;
        }
        record("Core UPDATE", "core_update_ops_sec", N, now() - t0);
    }

    // DELETE
    {
        const t0 = now();
        for (0..N) |i| {
            const k = std.fmt.bufPrint(&key_buf, "k{d:0>9}", .{i}) catch continue;
            _ = db.delete("bench", k) catch continue;
        }
        record("Core DELETE", "core_delete_ops_sec", N, now() - t0);
    }

    compat.fs.cwdDeleteTree(DATA_DIR) catch {};
}

// ─── Benchmark: Compression ─────────────────────────────────────────────────

fn benchCompression() void {
    const N: usize = 100_000;

    // Build a 4KB block with semi-realistic data (repeated JSON patterns)
    var src_buf: [4096]u8 = undefined;
    const pattern = "{\"id\":12345,\"name\":\"benchmark_test_value\",\"active\":true},";
    var off: usize = 0;
    while (off + pattern.len <= src_buf.len) {
        @memcpy(src_buf[off..][0..pattern.len], pattern);
        off += pattern.len;
    }
    // Fill remaining with zeros
    @memset(src_buf[off..], 0);

    const src: []const u8 = &src_buf;
    var compressed_buf: [8192]u8 = undefined; // 2x for worst case

    // Compress once to get the compressed size
    const comp_size = compression.compress(src, &compressed_buf) catch 0;

    // Compress throughput
    {
        const t0 = now();
        for (0..N) |_| {
            _ = compression.compress(src, &compressed_buf) catch {};
        }
        record("LZ4 Compress 4KB", "lz4_compress_ops_sec", N, now() - t0);
    }

    // Decompress throughput
    if (comp_size > 0) {
        var decomp_buf: [4096]u8 = undefined;
        const compressed: []const u8 = compressed_buf[0..comp_size];
        const t0 = now();
        for (0..N) |_| {
            _ = compression.decompress(compressed, &decomp_buf, src.len) catch {};
        }
        record("LZ4 Decompress 4KB", "lz4_decompress_ops_sec", N, now() - t0);
    }
}

// ─── Benchmark: ART Index ───────────────────────────────────────────────────

fn benchART() !void {
    const N: usize = 100_000;
    var tree = art_mod.ART.init(alloc);
    defer tree.deinit();

    var key_buf: [32]u8 = undefined;

    // Insert
    {
        const t0 = now();
        for (0..N) |i| {
            const k_len = std.fmt.bufPrint(&key_buf, "key:{d:0>9}", .{i}) catch continue;
            const entry = btree_mod.BTreeEntry{
                .key_hash = @as(u64, @intCast(i)) *% 0x9E3779B97F4A7C15,
                .doc_id = @intCast(i),
                .page_no = @intCast(i & 0xFFFF),
                .page_off = 0,
            };
            tree.insert(k_len, entry) catch continue;
        }
        record("ART Insert", "art_insert_ops_sec", N, now() - t0);
    }

    // Search
    {
        const t0 = now();
        for (0..N) |i| {
            const k_len = std.fmt.bufPrint(&key_buf, "key:{d:0>9}", .{i}) catch continue;
            _ = tree.search(k_len);
        }
        record("ART Search", "art_search_ops_sec", N, now() - t0);
    }

    // Prefix scan
    {
        const SCANS: usize = 10_000;
        const t0 = now();
        for (0..SCANS) |i| {
            const prefix_len = std.fmt.bufPrint(&key_buf, "key:{d:0>5}", .{i % 1000}) catch continue;
            const res = tree.prefixScan(prefix_len, alloc) catch continue;
            alloc.free(res);
        }
        record("ART Prefix Scan", "art_prefix_scan_ops_sec", SCANS, now() - t0);
    }
}

// ─── Benchmark: Query Engine ────────────────────────────────────────────────

fn benchQuery() void {
    const N: usize = 100_000;
    const filter_json = "{\"age\":{\"$gt\":25}}";
    const doc_json = "{\"name\":\"alice\",\"age\":30,\"city\":\"NYC\"}";

    // Filter parse
    {
        const t0 = now();
        for (0..N) |_| {
            var pf = query_mod.parseFilter(filter_json, alloc) catch continue;
            pf.deinit();
        }
        record("Query Parse", "query_parse_ops_sec", N, now() - t0);
    }

    // Filter match
    {
        var pf = query_mod.parseFilter(filter_json, alloc) catch return;
        defer pf.deinit();
        const t0 = now();
        for (0..N) |_| {
            _ = query_mod.matches(doc_json, &pf.filter);
        }
        record("Query Match", "query_match_ops_sec", N, now() - t0);
    }

    // Field extraction
    {
        const t0 = now();
        for (0..N) |_| {
            _ = query_mod.extractField(doc_json, "age");
        }
        record("Field Extract", "field_extract_ops_sec", N, now() - t0);
    }
}

// ─── Benchmark: LSM Tree ────────────────────────────────────────────────────

fn benchLSM() !void {
    const N: usize = 100_000;

    compat.fs.cwdDeleteTree(LSM_DIR) catch {};
    var lsm = try lsm_mod.LSMTree.init(alloc, LSM_DIR);
    defer {
        lsm.deinit();
        compat.fs.cwdDeleteTree(LSM_DIR) catch {};
    }

    // Put
    {
        const t0 = now();
        for (0..N) |i| {
            const key_hash: u64 = @as(u64, @intCast(i)) *% 0x517CC1B727220A95;
            const entry = btree_mod.BTreeEntry{
                .key_hash = key_hash,
                .doc_id = @intCast(i),
                .page_no = @intCast(i & 0xFFFF),
                .page_off = 0,
            };
            lsm.put(key_hash, entry) catch continue;
        }
        record("LSM Put", "lsm_put_ops_sec", N, now() - t0);
    }

    // Get
    {
        const t0 = now();
        for (0..N) |i| {
            const key_hash: u64 = @as(u64, @intCast(i)) *% 0x517CC1B727220A95;
            _ = lsm.get(key_hash);
        }
        record("LSM Get", "lsm_get_ops_sec", N, now() - t0);
    }

    // Flush
    {
        // Re-init with fresh data for flush timing
        lsm.deinit();
        compat.fs.cwdDeleteTree(LSM_DIR) catch {};
        lsm = try lsm_mod.LSMTree.init(alloc, LSM_DIR);

        // Fill memtable to ~4MB
        const fill_count: usize = lsm_mod.LSMTree.MEMTABLE_SIZE / @sizeOf(lsm_mod.KVEntry);
        for (0..fill_count) |i| {
            const key_hash: u64 = @as(u64, @intCast(i)) *% 0x517CC1B727220A95;
            lsm.put(key_hash, .{
                .key_hash = key_hash,
                .doc_id = @intCast(i),
                .page_no = @intCast(i & 0xFFFF),
                .page_off = 0,
            }) catch break;
        }

        const t0 = now();
        lsm.flush() catch {};
        record("LSM Flush", "lsm_flush_ops_sec", 1, now() - t0);
    }
}

// ─── Benchmark: Columnar ────────────────────────────────────────────────────

fn benchColumnar() !void {
    const N: usize = 1_000_000;

    var col = columnar_mod.Column.init("bench_col", .integer);
    defer col.deinit(alloc);

    // Append
    {
        const t0 = now();
        for (0..N) |i| {
            col.append(alloc, .{ .integer = @as(i64, @intCast(i)) }) catch continue;
        }
        record("Column Append", "column_append_ops_sec", N, now() - t0);
    }

    // Scan (sequential reads)
    {
        var checksum: i64 = 0;
        const t0 = now();
        for (0..N) |i| {
            if (col.get(@intCast(i))) |v| {
                switch (v) {
                    .integer => |val| checksum +%= val,
                    else => {},
                }
            }
        }
        const elapsed = now() - t0;
        record("Column Scan", "column_scan_ops_sec", N, elapsed);
        // Use checksum to prevent dead-code elimination
        if (checksum == 0) std.debug.print("", .{});
    }

    // filterColumn (vectorized predicate)
    {
        const bitmap_size = (N + 7) / 8;
        const bitmap = try alloc.alloc(u8, bitmap_size);
        defer alloc.free(bitmap);

        const t0 = now();
        columnar_mod.filterColumn(&col, .gt, .{ .integer = 500_000 }, bitmap);
        const elapsed = now() - t0;
        record("Column Filter", "column_filter_ops_sec", N, elapsed);
    }
}

// ─── Benchmark: MVCC ────────────────────────────────────────────────────────

fn benchMVCC() !void {
    const N: usize = 100_000;

    var chain = mvcc_mod.VersionChain.init(alloc);
    defer chain.deinit(alloc);

    // Version append
    {
        const t0 = now();
        for (0..N) |i| {
            const epoch = chain.advanceEpoch();
            chain.appendVersion(alloc, @intCast(i % 10_000), @intCast(i & 0xFFFFFFFF), @intCast(i & 0xFFFF), epoch) catch continue;
        }
        record("MVCC Append", "mvcc_append_ops_sec", N, now() - t0);
    }

    // Read transaction throughput
    {
        const t0 = now();
        for (0..N) |i| {
            var txn = chain.beginRead();
            _ = txn.get(@intCast(i % 10_000));
            txn.end();
        }
        record("MVCC Read Txn", "mvcc_read_txn_ops_sec", N, now() - t0);
    }

    // GC
    {
        // Advance epoch far forward so most versions are collectible
        for (0..100) |_| {
            _ = chain.advanceEpoch();
        }
        // Update min_active_epoch
        const cur = chain.current_epoch.load(.acquire);
        chain.min_active_epoch.store(cur - 1, .release);

        const gc_count: usize = 50_000;
        // We already have ~100K versions; GC will collect old ones
        const t0 = now();
        const freed = chain.gc(alloc);
        const elapsed = now() - t0;
        _ = gc_count;
        record("MVCC GC", "mvcc_gc_ops_sec", @intCast(freed), elapsed);
    }
}

fn emitJSON() void {
    // Timestamp
    const ts = std.time.timestamp();
    const epoch_secs: u64 = @intCast(ts);
    const secs_in_day: u64 = 86400;
    const days = epoch_secs / secs_in_day;
    const year: u64 = 1970 + days / 365;
    const month: u64 = (days % 365) / 30 + 1;
    const day: u64 = (days % 365) % 30 + 1;
    const hour: u64 = (epoch_secs % secs_in_day) / 3600;
    const minute: u64 = (epoch_secs % 3600) / 60;
    const second: u64 = epoch_secs % 60;

    // Print JSON to stderr for immediate visibility
    std.debug.print("\n{{\n", .{});
    std.debug.print("  \"timestamp\": \"{d:0>4}-{d:0>2}-{d:0>2}T{d:0>2}:{d:0>2}:{d:0>2}Z\",\n", .{ year, month, day, hour, minute, second });
    std.debug.print("  \"benchmarks\": {{\n", .{});

    for (0..result_count) |i| {
        const r = results[i];
        const ops_s = r.opsPerSec();
        const trailing: []const u8 = if (i + 1 < result_count) "," else "";
        std.debug.print("    \"{s}\": {d:.0}{s}\n", .{ r.json_key, ops_s, trailing });
    }

    std.debug.print("  }}\n", .{});
    std.debug.print("}}\n", .{});

    // Also write to a JSON file for CI consumption using fmt.bufPrint + writeAll
    const json_path = "/tmp/turbodb_regression_bench.json";
    const file = compat.fs.cwdCreateFile(json_path, .{}) catch return;
    defer file.close();

    var buf: [8192]u8 = undefined;
    var pos: usize = 0;

    pos += (std.fmt.bufPrint(buf[pos..], "{{\n  \"timestamp\": \"{d:0>4}-{d:0>2}-{d:0>2}T{d:0>2}:{d:0>2}:{d:0>2}Z\",\n  \"benchmarks\": {{\n", .{ year, month, day, hour, minute, second }) catch return).len;

    for (0..result_count) |i| {
        const r = results[i];
        const ops_s = r.opsPerSec();
        const trailing: []const u8 = if (i + 1 < result_count) "," else "";
        pos += (std.fmt.bufPrint(buf[pos..], "    \"{s}\": {d:.0}{s}\n", .{ r.json_key, ops_s, trailing }) catch return).len;
    }

    pos += (std.fmt.bufPrint(buf[pos..], "  }}\n}}\n", .{}) catch return).len;

    file.writeAll(buf[0..pos]) catch return;
    std.debug.print("\nJSON results written to: {s}\n", .{json_path});
}
// ─── Main ───────────────────────────────────────────────────────────────────

pub fn main() !void {
    runtime.init(std.heap.c_allocator);
    defer runtime.deinit();

    std.debug.print("\n", .{});
    std.debug.print("TurboDB Regression Benchmark\n", .{});
    std.debug.print("{s}", .{"\xe2\x95\x90" ** 24 ++ "\n"});

    std.debug.print("\n[Core Path]\n", .{});
    benchCorePath() catch |e| {
        std.debug.print("  (core path skipped: {any})\n", .{e});
    };

    std.debug.print("\n[Compression]\n", .{});
    benchCompression();

    std.debug.print("\n[ART Index]\n", .{});
    benchART() catch |e| {
        std.debug.print("  (ART skipped: {any})\n", .{e});
    };

    std.debug.print("\n[Query Engine]\n", .{});
    benchQuery();

    std.debug.print("\n[LSM Tree]\n", .{});
    benchLSM() catch |e| {
        std.debug.print("  (LSM skipped: {any})\n", .{e});
    };

    std.debug.print("\n[Columnar]\n", .{});
    benchColumnar() catch |e| {
        std.debug.print("  (columnar skipped: {any})\n", .{e});
    };

    std.debug.print("\n[MVCC]\n", .{});
    benchMVCC() catch |e| {
        std.debug.print("  (MVCC skipped: {any})\n", .{e});
    };

    std.debug.print("\n{s}\n", .{"\xe2\x95\x90" ** 24});
    std.debug.print("  {d} benchmarks complete\n\n", .{result_count});

    // Emit structured JSON to stdout
    emitJSON();

    // Cleanup
    compat.fs.cwdDeleteTree(DATA_DIR) catch {};
    compat.fs.cwdDeleteTree(LSM_DIR) catch {};
}
