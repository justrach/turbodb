/// ZagDB — Registry REST API server
///
/// Routes:
///   GET    /api/v1/packages/:name                  package metadata
///   GET    /api/v1/packages/:name/versions         list versions
///   GET    /api/v1/packages/:name/:version         version detail
///   POST   /api/v1/packages                        publish (body = tarball JSON)
///   PATCH  /api/v1/packages/:name/:version/yank    yank version
///   GET    /api/v1/blobs/:hash                     download source tarball
///   GET    /api/v1/search?q=...&limit=N            search packages
///   POST   /api/v1/identities                      register identity
///   GET    /api/v1/identities/:pubkey              lookup identity
///   GET    /health                                 health check
///   GET    /metrics                                request/error counters
///
/// Follows the same pattern as TurboDB's src/server.zig:
///   - Heap-allocated per-connection buffers (threadlocal)
///   - Simple dispatch routing
///   - JSON responses
const std = @import("std");
const compat = @import("compat");
const registry_mod = @import("registry.zig");
const sign_mod = @import("sign.zig");
const hash_mod = @import("hash.zig");
const auth_mod = @import("auth.zig");
const AuthContext = auth_mod.AuthContext;
const Registry = registry_mod.Registry;

const MAX_REQ = 262144; // 256 KiB (larger than TurboDB for package uploads)
const MAX_RESP = 262144; // 256 KiB
const MAX_BODY = 262144; // 256 KiB

const ConnBufs = struct {
    req: [MAX_REQ]u8,
    resp: [MAX_RESP]u8,
    body: [MAX_BODY]u8,
};
threadlocal var tl_bufs: ?*ConnBufs = null;

fn getRespBuf() *[MAX_RESP]u8 {
    return &tl_bufs.?.resp;
}
fn getBodyBuf() *[MAX_BODY]u8 {
    return &tl_bufs.?.body;
}

pub const RegistryServer = struct {
    registry: *Registry,
    port: u16,
    running: std.atomic.Value(bool),
    req_count: std.atomic.Value(u64),
    err_count: std.atomic.Value(u64),

    pub fn init(registry: *Registry, port: u16) RegistryServer {
        return .{
            .registry = registry,
            .port = port,
            .running = std.atomic.Value(bool).init(false),
            .req_count = std.atomic.Value(u64).init(0),
            .err_count = std.atomic.Value(u64).init(0),
        };
    }

    pub fn run(self: *RegistryServer) !void {
        const addr = try compat.net.Address.parseIp("0.0.0.0", self.port);
        var listener = try addr.listen(.{
            .reuse_address = true,
            .kernel_backlog = 256,
        });
        defer listener.deinit();

        self.running.store(true, .release);
        std.log.info("ZagDB Registry listening on :{d}", .{self.port});

        while (self.running.load(.acquire)) {
            const conn = listener.accept() catch |e| {
                if (e == error.WouldBlock) continue;
                std.log.err("accept: {}", .{e});
                continue;
            };
            const t = std.Thread.spawn(.{}, handleConn, .{ self, conn }) catch continue;
            t.detach();
        }
    }

    pub fn stop(self: *RegistryServer) void {
        self.running.store(false, .release);
    }
};

fn handleConn(srv: *RegistryServer, conn: compat.net.Server.Connection) void {
    defer conn.stream.close();

    const bufs = std.heap.page_allocator.create(ConnBufs) catch return;
    defer std.heap.page_allocator.destroy(bufs);
    tl_bufs = bufs;
    defer tl_bufs = null;

    while (true) {
        const n = conn.stream.read(&bufs.req) catch return;
        if (n == 0) return;
        _ = srv.req_count.fetchAdd(1, .monotonic);

        const resp_len = dispatch(srv, bufs.req[0..n]);
        conn.stream.writeAll(bufs.resp[0..resp_len]) catch return;
    }
}

fn dispatch(srv: *RegistryServer, raw: []const u8) usize {
    const nl = std.mem.indexOfScalar(u8, raw, '\n') orelse return err(400, "bad request");
    const req_line = std.mem.trimEnd(u8, raw[0..nl], "\r");
    var parts = std.mem.splitScalar(u8, req_line, ' ');
    const method = parts.next() orelse return err(400, "bad request");
    const full_path = parts.next() orelse return err(400, "bad request");

    var path = full_path;
    var query: []const u8 = "";
    if (std.mem.indexOfScalar(u8, full_path, '?')) |qi| {
        path = full_path[0..qi];
        query = full_path[qi + 1 ..];
    }

    const body = if (std.mem.indexOf(u8, raw, "\r\n\r\n")) |p| raw[p + 4 ..] else if (std.mem.indexOf(u8, raw, "\n\n")) |p| raw[p + 2 ..] else @as([]const u8, "");

    // Parse auth context from headers
    const auth = auth_mod.parseAuth(raw, method, path);

    // ─── /health ────────────────────────────────────────────────────────
    if (std.mem.eql(u8, path, "/health")) {
        return ok("{\"status\":\"ok\",\"engine\":\"ZagDB\",\"backend\":\"TurboDB\"}");
    }

    // ─── /metrics ───────────────────────────────────────────────────────
    if (std.mem.eql(u8, path, "/metrics")) {
        var fbs = compat.fixedBufferStream(getBodyBuf());
        compat.format(fbs.writer(), "{{\"requests\":{d},\"errors\":{d}}}", .{
            srv.req_count.load(.acquire),
            srv.err_count.load(.acquire),
        }) catch {};
        return ok(getBodyBuf()[0..fbs.pos]);
    }

    // ─── /api/v1/search?q=...&limit=N ──────────────────────────────────
    if (std.mem.eql(u8, path, "/api/v1/search") and std.mem.eql(u8, method, "GET")) {
        return handleSearch(srv, query, auth);
    }

    // ─── /api/v1/packages ──────────────────────────────────────────────
    if (std.mem.startsWith(u8, path, "/api/v1/packages")) {
        const rest = path[16..]; // after "/api/v1/packages"

        // POST /api/v1/packages — publish
        if (rest.len == 0 and std.mem.eql(u8, method, "POST")) {
            return handlePublish(srv, body);
        }

        // Must start with /
        if (rest.len > 0 and rest[0] == '/') {
            const after_slash = rest[1..];

            // Check for /versions suffix
            if (std.mem.endsWith(u8, after_slash, "/versions") and std.mem.eql(u8, method, "GET")) {
                const pkg_name = after_slash[0 .. after_slash.len - 9]; // strip "/versions"
                return handleListVersions(srv, pkg_name);
            }

            // Check for /yank suffix
            if (std.mem.endsWith(u8, after_slash, "/yank") and std.mem.eql(u8, method, "PATCH")) {
                return handleYank(srv, after_slash, body);
            }

            // Check for name/version pattern
            if (std.mem.indexOfScalar(u8, after_slash, '/')) |sep| {
                const pkg_name = after_slash[0..sep];
                const ver = after_slash[sep + 1 ..];
                if (std.mem.eql(u8, method, "GET")) {
                    return handleGetVersion(srv, pkg_name, ver, auth);
                }
            } else {
                // GET /api/v1/packages/:name
                if (std.mem.eql(u8, method, "GET")) {
                    return handleGetPackage(srv, after_slash, auth);
                }
            }
        }
    }

    // ─── /api/v1/blobs/:hash ───────────────────────────────────────────
    if (std.mem.startsWith(u8, path, "/api/v1/blobs/") and std.mem.eql(u8, method, "GET")) {
        const hash_hex = path[14..];
        return handleDownload(srv, hash_hex);
    }

    // ─── /api/v1/identities ────────────────────────────────────────────
    if (std.mem.startsWith(u8, path, "/api/v1/identities")) {
        const rest = path[18..];
        if (rest.len == 0 and std.mem.eql(u8, method, "POST")) {
            return handleRegisterIdentity(srv, body);
        }
        if (rest.len > 1 and rest[0] == '/' and std.mem.eql(u8, method, "GET")) {
            return handleGetIdentity(srv, rest[1..]);
        }
    }

    _ = srv.err_count.fetchAdd(1, .monotonic);
    return err(404, "not found");
}

// ─── Handlers ───────────────────────────────────────────────────────────────

fn handleSearch(srv: *RegistryServer, query_str: []const u8, auth: AuthContext) usize {
    const q = qparam(query_str, "q") orelse return err(400, "missing q parameter");
    const limit: u32 = qparamInt(query_str, "limit") orelse 20;
    const capped = @min(limit, 50);

    var results: [50]registry_mod.PackageInfo = undefined;
    const count = srv.registry.searchAuth(q, capped, &results, auth) catch return err(500, "search failed");

    var fbs = compat.fixedBufferStream(getBodyBuf());
    const w = fbs.writer();
    w.writeAll("{\"results\":[") catch {};
    for (0..count) |i| {
        if (i > 0) w.writeAll(",") catch {};
        compat.format(w, "{{\"name\":\"{s}\",\"description\":\"{s}\",\"version\":\"{s}\"}}", .{
            results[i].name,
            results[i].description,
            results[i].version,
        }) catch {};
    }
    compat.format(w, "],\"count\":{d}}}", .{count}) catch {};
    return ok(getBodyBuf()[0..fbs.pos]);
}

fn handlePublish(srv: *RegistryServer, body: []const u8) usize {
    // For MVP: body is the package JSON directly.
    // Signature and pubkey are passed as hex in the JSON body fields:
    //   {"tarball": "...", "signature": "<128hex>", "pubkey": "<64hex>"}
    // For simplicity in MVP, the body IS the tarball, and we use header-based auth.
    // But for now, let's accept unsigned publishes for testing.
    if (body.len == 0) return err(400, "empty body");

    // For MVP: treat body as tarball, use a test keypair (no auth)
    // TODO: Extract signature and pubkey from headers
    const kp = sign_mod.KeyPair.generate();
    const content_hash = hash_mod.hashBytes(body);
    var hash_hex: [64]u8 = undefined;
    hash_mod.hexEncode(content_hash, &hash_hex);
    const sig = sign_mod.sign(&hash_hex, kp.secret_key);

    const result = srv.registry.publish(body, sig, kp.public_key) catch |e| {
        return switch (e) {
            error.VersionAlreadyExists => err(409, "version already exists"),
            error.InvalidManifest => err(400, "invalid manifest"),
            error.InvalidSignature => err(403, "invalid signature"),
            else => err(500, "publish failed"),
        };
    };

    var fbs = compat.fixedBufferStream(getBodyBuf());
    compat.format(fbs.writer(), "{{\"ok\":true,\"name\":\"{s}\",\"version\":\"{s}\",\"hash\":\"{s}\"}}", .{
        result.package_name,
        result.version,
        result.source_hash_hex,
    }) catch {};
    return ok(getBodyBuf()[0..fbs.pos]);
}

fn handleGetPackage(srv: *RegistryServer, name: []const u8, auth: AuthContext) usize {
    const pkg = srv.registry.getPackageAuth(name, auth) orelse return err(404, "package not found");
    return ok(pkg);
}

fn handleGetVersion(srv: *RegistryServer, name: []const u8, version: []const u8, auth: AuthContext) usize {
    const ver = srv.registry.getVersionAuth(name, version, auth) orelse return err(404, "version not found");
    return ok(ver);
}

fn handleListVersions(_: *RegistryServer, _: []const u8) usize {
    // TODO: iterate versions collection for this package
    return ok("{\"versions\":[]}");
}

fn handleYank(srv: *RegistryServer, path: []const u8, body: []const u8) usize {
    _ = body;
    // Path format: "name/version/yank"
    // Parse name and version from path
    const sep1 = std.mem.indexOfScalar(u8, path, '/') orelse return err(400, "invalid path");
    const name = path[0..sep1];
    const after = path[sep1 + 1 ..];
    const sep2 = std.mem.indexOfScalar(u8, after, '/') orelse return err(400, "invalid path");
    const version = after[0..sep2];

    // For MVP: generate a yank signature (TODO: require auth headers)
    const kp = sign_mod.KeyPair.generate();
    var msg_buf: [512]u8 = undefined;
    const msg = std.fmt.bufPrint(&msg_buf, "yank:{s}@{s}", .{ name, version }) catch return err(500, "internal");
    const sig = sign_mod.sign(msg, kp.secret_key);

    const yanked = srv.registry.yank(name, version, sig, kp.public_key) catch |e| {
        return switch (e) {
            error.Unauthorized => err(403, "unauthorized"),
            error.InvalidSignature => err(403, "invalid signature"),
            else => err(500, "yank failed"),
        };
    };

    if (yanked) {
        return ok("{\"ok\":true,\"yanked\":true}");
    }
    return err(404, "version not found or already yanked");
}

fn handleDownload(srv: *RegistryServer, hash_hex: []const u8) usize {
    if (hash_hex.len != 64) return err(400, "invalid hash length");

    const data = srv.registry.download(hash_hex) catch return err(404, "blob not found");
    defer srv.registry.alloc.free(data);

    // For blobs, return raw data (not JSON)
    if (data.len > MAX_RESP - 256) return err(413, "blob too large for response buffer");

    var fbs = compat.fixedBufferStream(getRespBuf());
    compat.format(fbs.writer(), "HTTP/1.1 200 OK\r\nContent-Type: application/octet-stream\r\nContent-Length: {d}\r\nConnection: keep-alive\r\n\r\n", .{data.len}) catch {};
    const header_len = fbs.pos;
    if (header_len + data.len <= MAX_RESP) {
        @memcpy(getRespBuf()[header_len..][0..data.len], data);
        return header_len + data.len;
    }
    return err(500, "response too large");
}

fn handleRegisterIdentity(srv: *RegistryServer, body: []const u8) usize {
    _ = srv;
    _ = body;
    // TODO: parse identity from body, register
    return ok("{\"ok\":true}");
}

fn handleGetIdentity(srv: *RegistryServer, pubkey_hex: []const u8) usize {
    const identity = srv.registry.getIdentity(pubkey_hex) orelse return err(404, "identity not found");
    return ok(identity);
}

// ─── Response helpers ───────────────────────────────────────────────────────

fn ok(body: []const u8) usize {
    return respond(200, "OK", body);
}

fn err(code: u16, msg: []const u8) usize {
    var scratch: [256]u8 = undefined;
    const body = std.fmt.bufPrint(&scratch, "{{\"error\":\"{s}\"}}", .{msg}) catch msg;
    const status = switch (code) {
        400 => "Bad Request",
        403 => "Forbidden",
        404 => "Not Found",
        409 => "Conflict",
        413 => "Payload Too Large",
        else => "Internal Server Error",
    };
    return respond(code, status, body);
}

fn respond(code: u16, status: []const u8, body: []const u8) usize {
    var fbs = compat.fixedBufferStream(getRespBuf());
    compat.format(fbs.writer(), "HTTP/1.1 {d} {s}\r\nContent-Type: application/json\r\nContent-Length: {d}\r\nConnection: keep-alive\r\n\r\n{s}", .{ code, status, body.len, body }) catch {};
    return fbs.pos;
}

// ─── Query parameter parsers ────────────────────────────────────────────────

fn qparam(query: []const u8, key: []const u8) ?[]const u8 {
    var kbuf: [64]u8 = undefined;
    const needle = std.fmt.bufPrint(&kbuf, "{s}=", .{key}) catch return null;
    const pos = std.mem.indexOf(u8, query, needle) orelse return null;
    const start = pos + needle.len;
    var end = start;
    while (end < query.len and query[end] != '&') end += 1;
    if (end == start) return null;
    return query[start..end];
}

fn qparamInt(query: []const u8, key: []const u8) ?u32 {
    var kbuf: [64]u8 = undefined;
    const needle = std.fmt.bufPrint(&kbuf, "{s}=", .{key}) catch return null;
    const pos = std.mem.indexOf(u8, query, needle) orelse return null;
    const start = pos + needle.len;
    var end = start;
    while (end < query.len and query[end] >= '0' and query[end] <= '9') end += 1;
    if (end == start) return null;
    return std.fmt.parseInt(u32, query[start..end], 10) catch null;
}
