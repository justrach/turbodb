const std = @import("std");
const compat = @import("compat");
const wal_mod = @import("wal");

const BASE_DIR = "/tmp/turbodb_wal_bench";
const WRITE_ONLY_PATH: [:0]const u8 = "/tmp/turbodb_wal_bench/write_only.wal";
const COMMIT_PATH: [:0]const u8 = "/tmp/turbodb_wal_bench/commit.wal";

const WRITE_ONLY_ITERS: usize = 50_000;
const COMMIT_ITERS: usize = 1_000;
const PAYLOAD_SIZE: usize = 64;

const Result = struct {
    name: []const u8,
    iterations: usize,
    bytes: usize,
    elapsed_ns: u64,
};

fn paddedEntrySize(payload_len: usize) usize {
    const raw = wal_mod.HEADER_SIZE + payload_len;
    return raw + ((8 - (raw % 8)) % 8);
}

fn elapsedSince(start_ns: i128) u64 {
    return @intCast(compat.nanoTimestamp() - start_ns);
}

fn printResult(result: Result) void {
    const ns_f: f64 = @floatFromInt(result.elapsed_ns);
    const ops_f: f64 = @floatFromInt(result.iterations);
    const ops_s = ops_f / (ns_f / 1e9);
    const us_op = ns_f / ops_f / 1000.0;
    const mb_s = (@as(f64, @floatFromInt(result.bytes)) / (1024.0 * 1024.0)) / (ns_f / 1e9);

    std.debug.print("  {s:<32} {d:>12.0} ops/s  {d:>8.2} us/op  {d:>8.1} MiB/s\n", .{
        result.name,
        ops_s,
        us_op,
        mb_s,
    });
}

fn benchWriteOnly(allocator: std.mem.Allocator, payload: []const u8) !Result {
    var wal = try wal_mod.WAL.open(WRITE_ONLY_PATH, allocator);
    defer wal.close();

    const start_ns = compat.nanoTimestamp();
    var i: usize = 0;
    while (i < WRITE_ONLY_ITERS) : (i += 1) {
        _ = try wal.write(
            @intCast(i + 1),
            .doc_insert,
            wal_mod.DB_TAG_DOC,
            wal_mod.FLAG_COMMIT,
            payload,
        );
    }

    return .{
        .name = "buffered committed append",
        .iterations = WRITE_ONLY_ITERS,
        .bytes = WRITE_ONLY_ITERS * paddedEntrySize(payload.len),
        .elapsed_ns = elapsedSince(start_ns),
    };
}

fn benchCommit(allocator: std.mem.Allocator, payload: []const u8) !Result {
    var wal = try wal_mod.WAL.open(COMMIT_PATH, allocator);
    defer wal.close();

    const bytes_per_txn = paddedEntrySize(payload.len) + paddedEntrySize(16);
    const start_ns = compat.nanoTimestamp();
    var i: usize = 0;
    while (i < COMMIT_ITERS) : (i += 1) {
        const txn_id: u64 = @intCast(i + 1);
        _ = try wal.write(txn_id, .doc_insert, wal_mod.DB_TAG_DOC, 0, payload);
        try wal.commit(txn_id, wal_mod.DB_TAG_DOC);
    }

    return .{
        .name = "commit plus fsync",
        .iterations = COMMIT_ITERS,
        .bytes = COMMIT_ITERS * bytes_per_txn,
        .elapsed_ns = elapsedSince(start_ns),
    };
}

pub fn main() !void {
    const allocator = std.heap.smp_allocator;

    compat.cwd().deleteTree(BASE_DIR) catch {};
    try compat.cwd().makePath(BASE_DIR);
    defer compat.cwd().deleteTree(BASE_DIR) catch {};

    var payload: [PAYLOAD_SIZE]u8 = undefined;
    @memset(payload[0..], 0xA5);

    const write_only = try benchWriteOnly(allocator, payload[0..]);
    const commit = try benchCommit(allocator, payload[0..]);

    std.debug.print("\nTurboDB WAL Microbenchmark\n", .{});
    std.debug.print("  payload: {d} bytes\n", .{PAYLOAD_SIZE});
    std.debug.print("  write-only iterations: {d}\n", .{WRITE_ONLY_ITERS});
    std.debug.print("  commit iterations: {d}\n\n", .{COMMIT_ITERS});
    printResult(write_only);
    printResult(commit);
    std.debug.print("\n", .{});
}
