const std = @import("std");
const collection_mod = @import("collection.zig");
const partition_mod = @import("partition.zig");
const doc_mod = @import("doc.zig");
const wal_mod = @import("wal");
const epoch_mod = @import("epoch");
const cdc_mod = @import("cdc.zig");
const runtime = @import("runtime");
const compat = @import("compat");

const Collection = collection_mod.Collection;
const Database = collection_mod.Database;
const PartitionedCollection = partition_mod.PartitionedCollection;
const WAL = wal_mod.WAL;
const EpochManager = epoch_mod.EpochManager;

// ═══════════════════════════════════════════════════════════════════════════════
// TurboDB Partition Scaling Benchmark
//
// Exercises PartitionedCollection directly (in-process, no network),
// sweeping across partition counts [1, 2, 4, 8, 16].
// Outputs a human-readable table during execution and JSON at the end.
// ═══════════════════════════════════════════════════════════════════════════════

const alloc = std.heap.c_allocator;

// ─── Result accumulator ─────────────────────────────────────────────────────

const MAX_RESULTS = 64;

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
var name_bufs: [MAX_RESULTS][64]u8 = undefined;
var json_bufs: [MAX_RESULTS][64]u8 = undefined;

fn record(name_src: []const u8, json_src: []const u8, ops: usize, elapsed_ns: i128) void {
    const idx = result_count;

    // Copy name into stable buffer
    const name_len = @min(name_src.len, 64);
    @memcpy(name_bufs[idx][0..name_len], name_src[0..name_len]);
    const stable_name: []const u8 = name_bufs[idx][0..name_len];

    // Copy json key into stable buffer
    const json_len = @min(json_src.len, 64);
    @memcpy(json_bufs[idx][0..json_len], json_src[0..json_len]);
    const stable_json: []const u8 = json_bufs[idx][0..json_len];

    const r = BenchResult{ .name = stable_name, .json_key = stable_json, .ops = ops, .elapsed_ns = elapsed_ns };
    results[idx] = r;
    result_count += 1;

    // Print immediately
    const ops_s = r.opsPerSec();
    const us_op = r.usPerOp();
    std.debug.print("  {s:<24}: {d:>12.0} ops/s  ({d:.1} us/op)\n", .{ stable_name, ops_s, us_op });
}

// ─── Timing helper ──────────────────────────────────────────────────────────

inline fn now() i128 {
    return std.time.nanoTimestamp();
}

// ─── Partition benchmark for a given partition count ─────────────────────────

fn benchPartitionCount(n_partitions: u16) !void {
    var dir_buf: [128]u8 = undefined;
    const data_dir = std.fmt.bufPrint(&dir_buf, "/tmp/turbodb_partition_bench_{d}", .{n_partitions}) catch return;

    // Cleanup any previous run
    compat.fs.cwdDeleteTree(data_dir) catch {};

    // Create data dir
    compat.fs.cwdMakeDir(data_dir) catch |e| switch (e) {
        error.PathAlreadyExists => {},
        else => return e,
    };

    // Open WAL
    var wal_path_buf: [512]u8 = undefined;
    const wal_path = try std.fmt.bufPrintZ(&wal_path_buf, "{s}/doc.wal", .{data_dir});
    var wal_log = try WAL.open(wal_path, alloc);
    try wal_log.startFlusher();

    // Init epoch manager
    var epochs = try EpochManager.init(alloc);
    var cdc = cdc_mod.CDCManager.init(alloc);
    defer cdc.deinit();
    try cdc.start();

    // Open partitioned collection
    const pc = try PartitionedCollection.open(alloc, data_dir, "bench", n_partitions, .hash, &wal_log, &epochs, &cdc);

    const INSERT_OPS: usize = 100_000;
    const GET_OPS: usize = 100_000;
    const SCAN_OPS: usize = 10_000;
    const PAR_SCAN_OPS: usize = 10_000;

    var key_buf: [32]u8 = undefined;
    const value = "{\"x\":1}";

    // Format names using temp buffers, record() copies into stable storage
    var name_buf: [64]u8 = undefined;
    var json_buf: [64]u8 = undefined;

    // ─── INSERT ──────────────────────────────────────────────────────
    {
        const t0 = now();
        for (0..INSERT_OPS) |i| {
            const k = std.fmt.bufPrint(&key_buf, "k{d:0>9}", .{i}) catch continue;
            _ = pc.insert(k, value) catch continue;
        }
        const elapsed = now() - t0;

        const n = std.fmt.bufPrint(&name_buf, "INSERT ({d} parts)", .{n_partitions}) catch "INSERT";
        const j = std.fmt.bufPrint(&json_buf, "insert_{d}p_ops_sec", .{n_partitions}) catch "insert_ops_sec";
        record(n, j, INSERT_OPS, elapsed);
    }

    // ─── GET (random) ────────────────────────────────────────────────
    {
        var rng = std.Random.DefaultPrng.init(42);
        const t0 = now();
        for (0..GET_OPS) |_| {
            const idx = rng.random().intRangeAtMost(usize, 0, INSERT_OPS - 1);
            const k = std.fmt.bufPrint(&key_buf, "k{d:0>9}", .{idx}) catch continue;
            _ = pc.get(k);
        }
        const elapsed = now() - t0;

        const n = std.fmt.bufPrint(&name_buf, "GET ({d} parts)", .{n_partitions}) catch "GET";
        const j = std.fmt.bufPrint(&json_buf, "get_{d}p_ops_sec", .{n_partitions}) catch "get_ops_sec";
        record(n, j, GET_OPS, elapsed);
    }

    // ─── SCAN ────────────────────────────────────────────────────────
    {
        const t0 = now();
        for (0..SCAN_OPS) |_| {
            const sr = pc.scan(10, 0, alloc) catch continue;
            sr.deinit();
        }
        const elapsed = now() - t0;

        const n = std.fmt.bufPrint(&name_buf, "SCAN ({d} parts)", .{n_partitions}) catch "SCAN";
        const j = std.fmt.bufPrint(&json_buf, "scan_{d}p_ops_sec", .{n_partitions}) catch "scan_ops_sec";
        record(n, j, SCAN_OPS, elapsed);
    }

    // ─── PARALLEL SCAN ───────────────────────────────────────────────
    {
        const t0 = now();
        for (0..PAR_SCAN_OPS) |_| {
            const sr = pc.parallelScan(10, alloc) catch continue;
            sr.deinit();
        }
        const elapsed = now() - t0;

        const n = std.fmt.bufPrint(&name_buf, "PAR_SCAN ({d} parts)", .{n_partitions}) catch "PAR_SCAN";
        const j = std.fmt.bufPrint(&json_buf, "par_scan_{d}p_ops_sec", .{n_partitions}) catch "par_scan_ops_sec";
        record(n, j, PAR_SCAN_OPS, elapsed);
    }

    // ─── Cleanup ─────────────────────────────────────────────────────
    pc.close();
    wal_log.close();
    epochs.deinit();
    compat.fs.cwdDeleteTree(data_dir) catch {};
}

// ─── JSON emitter ───────────────────────────────────────────────────────────

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

    const partition_counts = [_]u16{ 1, 2, 4, 8, 16 };

    // Print JSON to stderr for immediate visibility
    std.debug.print("\n{{\n", .{});
    std.debug.print("  \"timestamp\": \"{d:0>4}-{d:0>2}-{d:0>2}T{d:0>2}:{d:0>2}:{d:0>2}Z\",\n", .{ year, month, day, hour, minute, second });

    std.debug.print("  \"partition_counts\": [", .{});
    for (partition_counts, 0..) |pc, i| {
        if (i > 0) std.debug.print(", ", .{});
        std.debug.print("{d}", .{pc});
    }
    std.debug.print("],\n", .{});

    std.debug.print("  \"benchmarks\": {{\n", .{});
    for (0..result_count) |i| {
        const r = results[i];
        const ops_s = r.opsPerSec();
        const trailing: []const u8 = if (i + 1 < result_count) "," else "";
        std.debug.print("    \"{s}\": {d:.0}{s}\n", .{ r.json_key, ops_s, trailing });
    }
    std.debug.print("  }}\n", .{});
    std.debug.print("}}\n", .{});

    // Also write to a JSON file for CI consumption
    const json_path = "/tmp/turbodb_shard_bench.json";
    const file = compat.fs.cwdCreateFile(json_path, .{}) catch return;
    defer file.close();

    var buf: [16384]u8 = undefined;
    var pos: usize = 0;

    pos += (std.fmt.bufPrint(buf[pos..], "{{\n  \"timestamp\": \"{d:0>4}-{d:0>2}-{d:0>2}T{d:0>2}:{d:0>2}:{d:0>2}Z\",\n", .{ year, month, day, hour, minute, second }) catch return).len;

    pos += (std.fmt.bufPrint(buf[pos..], "  \"partition_counts\": [", .{}) catch return).len;
    for (partition_counts, 0..) |pc, i| {
        if (i > 0) pos += (std.fmt.bufPrint(buf[pos..], ", ", .{}) catch return).len;
        pos += (std.fmt.bufPrint(buf[pos..], "{d}", .{pc}) catch return).len;
    }
    pos += (std.fmt.bufPrint(buf[pos..], "],\n  \"benchmarks\": {{\n", .{}) catch return).len;

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
    std.debug.print("TurboDB Partition Scaling Benchmark\n", .{});
    std.debug.print("{s}", .{"\xe2\x95\x90" ** 24 ++ "\n"});

    const partition_counts = [_]u16{ 1, 2, 4, 8, 16 };

    for (partition_counts) |n| {
        if (n == 1) {
            std.debug.print("\n[{d} Partition]\n", .{n});
        } else {
            std.debug.print("\n[{d} Partitions]\n", .{n});
        }
        benchPartitionCount(n) catch |e| {
            std.debug.print("  (partition {d} skipped: {any})\n", .{ n, e });
        };
    }

    std.debug.print("\n{s}\n", .{"\xe2\x95\x90" ** 24});
    std.debug.print("  {d} benchmarks complete\n\n", .{result_count});

    // Emit structured JSON
    emitJSON();

    // Final cleanup
    for (partition_counts) |n| {
        var dir_buf: [128]u8 = undefined;
        const data_dir = std.fmt.bufPrint(&dir_buf, "/tmp/turbodb_partition_bench_{d}", .{n}) catch continue;
        compat.fs.cwdDeleteTree(data_dir) catch {};
    }
}
