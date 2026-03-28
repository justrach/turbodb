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

    // Replication flags
    var repl_enabled: bool = false;
    var repl_node_id: u16 = 0;
    var repl_is_leader: bool = false;
    var repl_port: u16 = 27117;
    var repl_peers_raw: ?[]const u8 = null;

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
        } else if (std.mem.eql(u8, args[i], "--replicate")) {
            repl_enabled = true;
        } else if (std.mem.eql(u8, args[i], "--node-id") and i + 1 < args.len) {
            i += 1;
            repl_node_id = try std.fmt.parseInt(u16, args[i], 10);
        } else if (std.mem.eql(u8, args[i], "--leader")) {
            repl_is_leader = true;
        } else if (std.mem.eql(u8, args[i], "--repl-port") and i + 1 < args.len) {
            i += 1;
            repl_port = try std.fmt.parseInt(u16, args[i], 10);
        } else if (std.mem.eql(u8, args[i], "--peers") and i + 1 < args.len) {
            i += 1;
            repl_peers_raw = args[i];
        } else if (std.mem.eql(u8, args[i], "--help")) {
            std.debug.print(
                \\TurboDB — a fast NoSQL document database in Zig
                \\
                \\  --data <dir>       data directory (default: ./turbodb_data)
                \\  --port <port>      listen port (default: 27017)
                \\  --wire             binary wire protocol (default)
                \\  --http             HTTP REST API
                \\  --both             run wire + HTTP (wire on port, HTTP on port+1)
                \\  --unix <path>      also listen on a Unix domain socket
                \\
                \\Replication (Calvin deterministic):
                \\  --replicate        enable Calvin replication
                \\  --node-id <id>     this node's ID (0, 1, 2, ...)
                \\  --leader           this node is the sequencer/leader
                \\  --repl-port <port> inter-node communication port (default: 27117)
                \\  --peers <addrs>    comma-separated peer addresses (host:port,...)
                \\
                \\Example cluster:
                \\  # Node 0 (leader):
                \\  turbodb --replicate --node-id 0 --leader --peers 10.0.0.2:27117,10.0.0.3:27117
                \\
                \\  # Node 1 (replica):
                \\  turbodb --replicate --node-id 1 --repl-port 27117
                \\
                \\  # Node 2 (replica):
                \\  turbodb --replicate --node-id 2 --repl-port 27117
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

    // ── replication setup ─────────────────────────────────────────────────
    if (repl_enabled) {
        std.log.info("Calvin replication: node={d} leader={} repl_port={d}", .{
            repl_node_id, repl_is_leader, repl_port,
        });
        if (repl_peers_raw) |peers_str| {
            std.log.info("Peers: {s}", .{peers_str});
        }
    }

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
}
