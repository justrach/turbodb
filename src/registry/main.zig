/// ZagDB — Registry server entry point
///
/// Usage: zagdb [--port PORT] [--data DIR]
///
/// Starts the ZagDB package registry HTTP server backed by TurboDB.
const std = @import("std");
const api = @import("api.zig");
const registry_mod = @import("registry.zig");
const runtime = @import("runtime");
const compat = @import("compat");

pub fn main(init: std.process.Init) !void {
    const alloc = init.gpa;
    runtime.setIo(init.io);

    var port: u16 = 8080;
    var data_dir: []const u8 = "./zagdb-data";

    // Parse CLI args
    const args = try compat.argsAlloc(alloc, init.minimal.args);
    defer compat.argsFree(alloc, args);

    var i: usize = 1;
    while (i < args.len) : (i += 1) {
        if (std.mem.eql(u8, args[i], "--port") and i + 1 < args.len) {
            i += 1;
            port = std.fmt.parseInt(u16, args[i], 10) catch {
                std.log.err("invalid port: {s}", .{args[i]});
                return;
            };
        } else if (std.mem.eql(u8, args[i], "--data") and i + 1 < args.len) {
            i += 1;
            data_dir = args[i];
        } else if (std.mem.eql(u8, args[i], "--help") or std.mem.eql(u8, args[i], "-h")) {
            std.debug.print(
                \\ZagDB — Content-addressed package registry
                \\
                \\Usage: zagdb [OPTIONS]
                \\
                \\Options:
                \\  --port PORT    Listen port (default: 8080)
                \\  --data DIR     Data directory (default: ./zagdb-data)
                \\  --help         Show this help
                \\
                \\API Routes:
                \\  GET    /health                              Health check
                \\  GET    /api/v1/search?q=...&limit=N         Search packages
                \\  GET    /api/v1/packages/:name               Package metadata
                \\  GET    /api/v1/packages/:name/:version      Version detail
                \\  POST   /api/v1/packages                     Publish package
                \\  PATCH  /api/v1/packages/:name/:ver/yank     Yank version
                \\  GET    /api/v1/blobs/:hash                  Download blob
                \\
            , .{});
            return;
        }
    }

    // Ensure data directory exists
    compat.fs.cwdMakeDir(data_dir) catch |e| switch (e) {
        error.PathAlreadyExists => {},
        else => return e,
    };

    std.log.info("ZagDB starting — data={s} port={d}", .{ data_dir, port });

    var registry = try registry_mod.Registry.init(alloc, data_dir);
    defer registry.deinit();

    var server = api.RegistryServer.init(&registry, port);
    try server.run();
}
