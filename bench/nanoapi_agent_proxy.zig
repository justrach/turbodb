const std = @import("std");
const nano = @import("nanoapi");

extern "c" fn socket(domain: c_uint, sock_type: c_uint, protocol: c_uint) c_int;
extern "c" fn close(fd: std.c.fd_t) c_int;

const Config = struct {
    proxy_port: u16 = 28080,
    turbodb_host: []const u8 = "127.0.0.1",
    turbodb_host_owned: bool = false,
    turbodb_port: u16 = 27017,
    runtime: nano.server.Runtime = .auto,
    worker_threads: usize = 0,
    check_only: bool = false,

    fn deinit(self: *Config, allocator: std.mem.Allocator) void {
        if (self.turbodb_host_owned) allocator.free(self.turbodb_host);
    }
};

var g_config: *const Config = undefined;

pub fn main(init: std.process.Init) !void {
    const allocator = std.heap.smp_allocator;
    var config = try configFromArgs(init.minimal.args, allocator);
    defer config.deinit(allocator);
    g_config = &config;

    var api = try nano.NanoAPI.init(allocator, .{
        .title = "TurboDB Agentic NanoAPI Proxy",
        .docs_url = null,
        .redoc_url = null,
        .openapi_url = null,
    });
    defer api.deinit();

    try api.getStaticJson("/health", "{\"status\":\"ok\",\"gateway\":\"nanoapi-agent-proxy\"}", .{});
    try api.get("/turbodb/health", turboHealth, .{});
    try api.get("/turbodb/metrics", turboMetrics, .{});
    try api.post("/agent/event/{key}", writeEvent, .{});
    try api.post("/agent/tool/{key}", writeTool, .{});
    try api.get("/agent/event/{key}", readEvent, .{});
    try api.post("/agent/batch", batchEvents, .{});
    try api.get("/agent/context", contextScan, .{});

    if (config.check_only) return;

    std.debug.print(
        "nanoapi agent proxy listening on :{d}; turbodb={s}:{d}; runtime={t}; workers={d}\n",
        .{ config.proxy_port, config.turbodb_host, config.turbodb_port, config.runtime, config.worker_threads },
    );
    try nano.server.serve(&api, allocator, .{
        .host = .{ 0, 0, 0, 0 },
        .port = config.proxy_port,
        .runtime = config.runtime,
        .worker_threads = config.worker_threads,
        .read_buffer_size = 128 * 1024,
        .write_buffer_size = 128 * 1024,
    });
}

fn turboHealth(req: *nano.Request) anyerror!nano.Response {
    return turboJson(req, "GET", "/health", "");
}

fn turboMetrics(req: *nano.Request) anyerror!nano.Response {
    return turboJson(req, "GET", "/metrics", "");
}

fn writeEvent(req: *nano.Request) anyerror!nano.Response {
    const key = req.pathParam("key") orelse return jsonError(req, 400, "missing key");
    const path = try std.fmt.allocPrint(req.allocator, "/db/agent_events/{s}", .{key});
    return turboJson(req, "PUT", path, req.body);
}

fn writeTool(req: *nano.Request) anyerror!nano.Response {
    const key = req.pathParam("key") orelse return jsonError(req, 400, "missing key");
    const path = try std.fmt.allocPrint(req.allocator, "/db/agent_tools/{s}", .{key});
    return turboJson(req, "PUT", path, req.body);
}

fn readEvent(req: *nano.Request) anyerror!nano.Response {
    const key = req.pathParam("key") orelse return jsonError(req, 400, "missing key");
    const path = try std.fmt.allocPrint(req.allocator, "/db/agent_events/{s}", .{key});
    return turboJson(req, "GET", path, "");
}

fn batchEvents(req: *nano.Request) anyerror!nano.Response {
    return turboJson(req, "POST", "/db/agent_events/bulk", req.body);
}

fn contextScan(req: *nano.Request) anyerror!nano.Response {
    const limit_raw = req.queryParam("limit") orelse "20";
    const path = try std.fmt.allocPrint(req.allocator, "/db/agent_events?limit={s}", .{limit_raw});
    return turboJson(req, "GET", path, "");
}

fn turboJson(req: *nano.Request, method: []const u8, path: []const u8, body: []const u8) anyerror!nano.Response {
    const bytes = callTurbo(req.allocator, method, path, body) catch |err| {
        const msg = try std.fmt.allocPrint(req.allocator, "{{\"error\":\"turbodb proxy failed\",\"detail\":\"{t}\"}}", .{err});
        return nano.Response.fromOwnedBody(req.allocator, msg, .{ .status_code = 502, .media_type = "application/json" });
    };
    return nano.Response.fromOwnedBody(req.allocator, bytes, .{ .media_type = "application/json" });
}

fn jsonError(req: *nano.Request, code: u16, msg: []const u8) !nano.Response {
    const bytes = try std.fmt.allocPrint(req.allocator, "{{\"error\":\"{s}\"}}", .{msg});
    return nano.Response.fromOwnedBody(req.allocator, bytes, .{ .status_code = code, .media_type = "application/json" });
}

fn callTurbo(allocator: std.mem.Allocator, method: []const u8, path: []const u8, body: []const u8) ![]u8 {
    const cfg = g_config;
    const fd = try connectTcp(cfg.turbodb_host, cfg.turbodb_port);
    defer _ = close(fd);

    var req_bytes: std.ArrayList(u8) = .empty;
    defer req_bytes.deinit(allocator);
    try req_bytes.print(
        allocator,
        "{s} {s} HTTP/1.1\r\nHost: turbodb\r\nContent-Type: application/json\r\nContent-Length: {d}\r\nConnection: close\r\n\r\n",
        .{ method, path, body.len },
    );
    try req_bytes.appendSlice(allocator, body);
    try sendAll(fd, req_bytes.items);

    var response: std.ArrayList(u8) = .empty;
    defer response.deinit(allocator);
    var scratch: [8192]u8 = undefined;
    while (response.items.len < 1024 * 1024) {
        const n = recvOnce(fd, &scratch) catch |err| switch (err) {
            error.ConnectionClosed => break,
            else => return err,
        };
        if (n == 0) break;
        try response.appendSlice(allocator, scratch[0..n]);
        if (responseBodyComplete(response.items)) break;
    }

    const body_bytes = responseBody(response.items) orelse return error.BadHttpResponse;
    return try allocator.dupe(u8, body_bytes);
}

fn responseBodyComplete(bytes: []const u8) bool {
    const frame = inspectResponse(bytes) orelse return false;
    return bytes.len >= frame.header_end + frame.content_length;
}

fn responseBody(bytes: []const u8) ?[]const u8 {
    const frame = inspectResponse(bytes) orelse return null;
    if (bytes.len < frame.header_end + frame.content_length) return null;
    return bytes[frame.header_end .. frame.header_end + frame.content_length];
}

const HttpFrame = struct {
    header_end: usize,
    content_length: usize,
};

fn inspectResponse(bytes: []const u8) ?HttpFrame {
    const header_end = if (std.mem.indexOf(u8, bytes, "\r\n\r\n")) |p| p + 4 else return null;
    return .{ .header_end = header_end, .content_length = extractContentLength(bytes[0..header_end]) };
}

fn extractContentLength(headers: []const u8) usize {
    var lines = std.mem.splitSequence(u8, headers, "\r\n");
    while (lines.next()) |line_raw| {
        const line = std.mem.trim(u8, line_raw, " \t\r\n");
        const colon = std.mem.indexOfScalar(u8, line, ':') orelse continue;
        const name = std.mem.trim(u8, line[0..colon], " \t");
        if (!std.ascii.eqlIgnoreCase(name, "content-length")) continue;
        const value = std.mem.trim(u8, line[colon + 1 ..], " \t");
        return std.fmt.parseInt(usize, value, 10) catch 0;
    }
    return 0;
}

fn connectTcp(host: []const u8, port: u16) !std.c.fd_t {
    const fd = socket(std.c.AF.INET, std.c.SOCK.STREAM, 0);
    switch (std.c.errno(fd)) {
        .SUCCESS => {},
        else => |err| return errnoError(err),
    }
    errdefer _ = close(fd);

    var addr: std.c.sockaddr.in = .{
        .port = std.mem.nativeToBig(u16, port),
        .addr = @bitCast(try parseIpv4(host)),
    };
    const rc = std.c.connect(fd, @ptrCast(&addr), @sizeOf(std.c.sockaddr.in));
    switch (std.c.errno(rc)) {
        .SUCCESS => {},
        else => |err| return errnoError(err),
    }
    return fd;
}

fn parseIpv4(ip: []const u8) ![4]u8 {
    var out: [4]u8 = undefined;
    var it = std.mem.splitScalar(u8, ip, '.');
    var i: usize = 0;
    while (it.next()) |part| {
        if (i >= out.len or part.len == 0) return error.InvalidIpAddress;
        out[i] = try std.fmt.parseInt(u8, part, 10);
        i += 1;
    }
    if (i != out.len) return error.InvalidIpAddress;
    return out;
}

fn recvOnce(fd: std.c.fd_t, buf: []u8) !usize {
    while (true) {
        const n = std.c.recv(fd, buf.ptr, buf.len, 0);
        switch (std.c.errno(n)) {
            .SUCCESS => return @intCast(n),
            .INTR => continue,
            .AGAIN => continue,
            .CONNRESET => return error.ConnectionClosed,
            else => |err| return errnoError(err),
        }
    }
}

fn sendAll(fd: std.c.fd_t, bytes: []const u8) !void {
    var written: usize = 0;
    while (written < bytes.len) {
        const flags = if (@hasDecl(std.c.MSG, "NOSIGNAL")) std.c.MSG.NOSIGNAL else 0;
        const n = std.c.send(fd, bytes[written..].ptr, bytes.len - written, flags);
        switch (std.c.errno(n)) {
            .SUCCESS => {
                if (n == 0) return error.ConnectionClosed;
                written += @intCast(n);
            },
            .INTR => continue,
            .AGAIN => continue,
            else => |err| return errnoError(err),
        }
    }
}

fn errnoError(err: std.c.E) error{
    BadHttpResponse,
    ConnectionClosed,
    ConnectFailed,
    InvalidIpAddress,
    Unexpected,
} {
    switch (err) {
        .CONNREFUSED, .HOSTUNREACH, .NETUNREACH, .TIMEDOUT => return error.ConnectFailed,
        .CONNRESET, .PIPE, .NOTCONN => return error.ConnectionClosed,
        else => return error.Unexpected,
    }
}

fn configFromArgs(args_state: std.process.Args, allocator: std.mem.Allocator) !Config {
    var args = std.process.Args.Iterator.init(args_state);
    defer args.deinit();
    _ = args.next();

    var config: Config = .{};
    while (args.next()) |arg| {
        if (std.mem.eql(u8, arg, "--check")) {
            config.check_only = true;
        } else if (std.mem.eql(u8, arg, "--port")) {
            config.proxy_port = try std.fmt.parseInt(u16, args.next() orelse return error.InvalidArgument, 10);
        } else if (std.mem.eql(u8, arg, "--turbodb-host")) {
            config.turbodb_host = try allocator.dupe(u8, args.next() orelse return error.InvalidArgument);
            config.turbodb_host_owned = true;
        } else if (std.mem.eql(u8, arg, "--turbodb-port")) {
            config.turbodb_port = try std.fmt.parseInt(u16, args.next() orelse return error.InvalidArgument, 10);
        } else if (std.mem.eql(u8, arg, "--runtime")) {
            config.runtime = try parseRuntime(args.next() orelse return error.InvalidArgument);
        } else if (std.mem.eql(u8, arg, "--workers")) {
            config.worker_threads = try std.fmt.parseInt(usize, args.next() orelse return error.InvalidArgument, 10);
        } else {
            return error.InvalidArgument;
        }
    }
    return config;
}

fn parseRuntime(raw: []const u8) !nano.server.Runtime {
    if (std.mem.eql(u8, raw, "auto")) return .auto;
    if (std.mem.eql(u8, raw, "io_uring")) return .io_uring;
    if (std.mem.eql(u8, raw, "thread_per_connection")) return .thread_per_connection;
    if (std.mem.eql(u8, raw, "event_loop")) return .event_loop;
    return error.InvalidRuntime;
}

test "parse response body" {
    const resp = "HTTP/1.1 200 OK\r\nContent-Length: 7\r\n\r\n{\"x\":1}";
    try std.testing.expectEqualStrings("{\"x\":1}", responseBody(resp).?);
}
