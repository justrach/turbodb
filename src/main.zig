/// TurboDB — entry point
const std = @import("std");
const collection = @import("collection.zig");
const server = @import("server.zig");
const wire = @import("wire.zig");

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const alloc = gpa.allocator();

    // ── CLI args ──────────────────────────────────────────────────────────
    const args = try std.process.argsAlloc(alloc);
    defer std.process.argsFree(alloc, args);

    var data_dir: []const u8 = "./turbodb_data";
    var port: u16 = 27017;
    var use_wire: bool = true; // wire protocol by default
    var use_http: bool = false;
    var unix_path: ?[]const u8 = null;
    var i: usize = 1;
    while (i < args.len) : (i += 1) {
        if (std.mem.eql(u8, args[i], "--data") and i + 1 < args.len) {
            i += 1; data_dir = args[i];
        } else if (std.mem.eql(u8, args[i], "--port") and i + 1 < args.len) {
            i += 1; port = try std.fmt.parseInt(u16, args[i], 10);
        } else if (std.mem.eql(u8, args[i], "--wire")) {
            use_wire = true;
            use_http = false;
        } else if (std.mem.eql(u8, args[i], "--http")) {
            use_http = true;
            use_wire = false;
        } else if (std.mem.eql(u8, args[i], "--both")) {
            use_wire = true;
            use_http = true;
        } else if (std.mem.eql(u8, args[i], "--unix") and i + 1 < args.len) {
            i += 1;
            unix_path = args[i];
        } else if (std.mem.eql(u8, args[i], "--help")) {
            std.debug.print(
                \\TurboDB — a fast NoSQL document database in Zig
                \\
                \\  --data <dir>   data directory (default: ./turbodb_data)
                \\  --port <port>  listen port (default: 27017)
                \\  --wire         binary wire protocol (default)
                \\  --http         HTTP REST API
                \\  --both         run wire + HTTP (wire on port, HTTP on port+1)
                \\  --unix <path>  also listen on a Unix domain socket
                \\
                \\Wire protocol (default, fastest):
                \\  Binary frame protocol with pipelining and batch support.
                \\  Use Python/JS client: from turbodb import Database
                \\
                \\HTTP API (--http):
                \\  POST   /db/:col              insert document
                \\  GET    /db/:col/:key         get document by key
                \\  PUT    /db/:col/:key         update document
                \\  DELETE /db/:col/:key         delete document
                \\  GET    /db/:col?limit=N      scan collection
                \\  GET    /health               health check
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
    var http_srv = server.Server.init(alloc, db, if (use_http and use_wire) port + 1 else port);
    var wire_srv = wire.WireServer.init(db, port);

    const S = struct {
        var g_http: ?*server.Server = null;
        var g_wire: ?*wire.WireServer = null;
        fn handler(_: c_int) callconv(.c) void {
            if (g_http) |s| s.stop();
            if (g_wire) |w| w.stop();
        }
    };
    if (use_http) S.g_http = &http_srv;
    if (use_wire) S.g_wire = &wire_srv;

    const sa = std.posix.Sigaction{
        .handler = .{ .handler = S.handler },
        .mask = std.mem.zeroes(std.posix.sigset_t),
        .flags = 0,
    };
    std.posix.sigaction(std.posix.SIG.INT, &sa, null);
    std.posix.sigaction(std.posix.SIG.TERM, &sa, null);

    // ── run ───────────────────────────────────────────────────────────────
    // Spawn Unix socket listener in a background thread if requested
    if (unix_path) |upath| {
        if (use_wire) {
            const unix_wire_t = try std.Thread.spawn(.{}, wire.WireServer.runUnix, .{ &wire_srv, upath });
            _ = unix_wire_t;
        }
        if (use_http) {
            const unix_http_t = try std.Thread.spawn(.{}, server.Server.runUnix, .{ &http_srv, upath });
            _ = unix_http_t;
        }
    }

    if (use_http and use_wire) {
        // Run HTTP in a background thread, wire in foreground
        const http_thread = try std.Thread.spawn(.{}, server.Server.run, .{&http_srv});
        _ = http_thread;
        try wire_srv.run();
    } else if (use_wire) {
        try wire_srv.run();
    } else {
        try http_srv.run();
    }
    std.log.info("TurboDB stopped.", .{});
    std.log.info("TurboDB stopped.", .{});
}
