/// TurboDB — entry point
const std = @import("std");
const collection = @import("collection.zig");
const server = @import("server.zig");

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const alloc = gpa.allocator();

    // ── CLI args ──────────────────────────────────────────────────────────
    const args = try std.process.argsAlloc(alloc);
    defer std.process.argsFree(alloc, args);

    var data_dir: []const u8 = "./turbodb_data";
    var port: u16 = 27017; // same default as MongoDB
    var i: usize = 1;
    while (i < args.len) : (i += 1) {
        if (std.mem.eql(u8, args[i], "--data") and i + 1 < args.len) {
            i += 1; data_dir = args[i];
        } else if (std.mem.eql(u8, args[i], "--port") and i + 1 < args.len) {
            i += 1; port = try std.fmt.parseInt(u16, args[i], 10);
        } else if (std.mem.eql(u8, args[i], "--help")) {
            std.debug.print(
                \\TurboDB — a fast document database in Zig
                \\
                \\  --data <dir>   data directory (default: ./turbodb_data)
                \\  --port <port>  HTTP listen port (default: 27017)
                \\
                \\API (MongoDB-inspired):
                \\  POST   /db/:col              insert document
                \\  GET    /db/:col/:key          get document by key
                \\  PUT    /db/:col/:key          update document
                \\  DELETE /db/:col/:key          delete document
                \\  GET    /db/:col?limit=N&offset=M   scan collection
                \\  DELETE /db/:col              drop collection
                \\  GET    /collections           list collections
                \\  GET    /health               health check
                \\  GET    /metrics              request/error counters
                \\
                \\Design highlights vs MongoDB:
                \\  * FNV-1a 8-byte key hash (vs 12-byte ObjectId)
                \\  * Compact binary page format (no BSON overhead)
                \\  * 4KB page B-tree, branching factor 184, 3 levels = 6.2M docs
                \\  * WAL group commit - single fsync per batch (same as PostgreSQL)
                \\  * MVCC version chains - zero read locks, concurrent readers
                \\  * Zero-alloc JSON field scanner (no serde per read)
                \\
            , .{});
            return;
        }
    }

    // ── ensure data directory ─────────────────────────────────────────────
    std.fs.cwd().makeDir(data_dir) catch |e| switch (e) {
        error.PathAlreadyExists => {},
        else => return e,
    };

    // ── open database ─────────────────────────────────────────────────────
    std.log.info("Opening TurboDB at {s}", .{data_dir});
    const db = try collection.Database.open(alloc, data_dir);
    defer db.close();

    // ── signal handler ────────────────────────────────────────────────────
    var srv = server.Server.init(alloc, db, port);

    const S = struct {
        var g_srv: ?*server.Server = null;
        fn handler(_: c_int) callconv(.c) void {
            if (g_srv) |s| s.stop();
        }
    };
    S.g_srv = &srv;
    const sa = std.posix.Sigaction{
        .handler = .{ .handler = S.handler },
        .mask = std.mem.zeroes(std.posix.sigset_t),
        .flags = 0,
    };
    std.posix.sigaction(std.posix.SIG.INT,  &sa, null);
    std.posix.sigaction(std.posix.SIG.TERM, &sa, null);

    // ── run ───────────────────────────────────────────────────────────────
    try srv.run();
    std.log.info("TurboDB stopped.", .{});
}
