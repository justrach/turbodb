const std = @import("std");
const client = @import("client.zig");
const runtime = @import("runtime");
const compat = @import("compat");

pub fn main() !void {
    const alloc = std.heap.c_allocator;
    runtime.init(alloc);
    defer runtime.deinit();
    const data_dir = "/tmp/turbodb_native_bench";
    std.fs.cwd().deleteTree(data_dir) catch {};

    var db = try client.Db.open(alloc, data_dir);
    defer db.close();

    const N: usize = 50_000;
    var key_buf: [32]u8 = undefined;

    // Use a short value (< 3 bytes) so the trigram index is not populated.
    // This benchmarks the core insert path (WAL + B-tree + page file).
    const value = "{}";

    // ── INSERT benchmark ──
    const t0 = std.time.nanoTimestamp();
    var i: usize = 0;
    while (i < N) : (i += 1) {
        const k = std.fmt.bufPrint(&key_buf, "k{d:0>9}", .{i}) catch continue;
        _ = db.insert("bench", k, value) catch continue;
    }
    const t1 = std.time.nanoTimestamp();
    const insert_ns: f64 = @floatFromInt(@as(i128, t1 - t0));
    const insert_ops: f64 = @as(f64, @floatFromInt(N)) / (insert_ns / 1e9);

    // ── GET benchmark (random lookups) ──
    var rng = std.Random.DefaultPrng.init(42);
    const t2 = std.time.nanoTimestamp();
    var g: usize = 0;
    while (g < N) : (g += 1) {
        const idx = rng.random().intRangeAtMost(usize, 0, N - 1);
        const k = std.fmt.bufPrint(&key_buf, "k{d:0>9}", .{idx}) catch continue;
        _ = db.get("bench", k) catch null;
    }
    const t3 = std.time.nanoTimestamp();
    const get_ns: f64 = @floatFromInt(@as(i128, t3 - t2));
    const get_ops: f64 = @as(f64, @floatFromInt(N)) / (get_ns / 1e9);

    // ── SEARCH benchmark (brute-force path since trigram index is empty) ──
    const t4 = std.time.nanoTimestamp();
    var s: usize = 0;
    while (s < 1000) : (s += 1) {
        var sr = db.search("bench", "k00000", 10, alloc) catch continue;
        sr.deinit();
    }
    const t5 = std.time.nanoTimestamp();
    const search_ns: f64 = @floatFromInt(@as(i128, t5 - t4));
    const search_ops: f64 = 1000.0 / (search_ns / 1e9);

    std.debug.print("\n", .{});
    std.debug.print("TurboDB Native Zig Benchmark ({d} ops)\n", .{N});
    std.debug.print("═══════════════════════════════════════\n", .{});
    std.debug.print("  INSERT : {d:.0} ops/s  ({d:.1}us/op)\n", .{ insert_ops, insert_ns / @as(f64, @floatFromInt(N)) / 1000.0 });
    std.debug.print("  GET    : {d:.0} ops/s  ({d:.1}us/op)\n", .{ get_ops, get_ns / @as(f64, @floatFromInt(N)) / 1000.0 });
    std.debug.print("  SEARCH : {d:.0} ops/s  ({d:.1}us/op)\n", .{ search_ops, search_ns / 1000.0 / 1000.0 });
    std.debug.print("\n", .{});

    std.fs.cwd().deleteTree(data_dir) catch {};
}
