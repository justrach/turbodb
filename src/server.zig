/// TurboDB HTTP server — MongoDB-compatible-ish JSON REST API
/// Routes:
///   POST   /db/:col              insert document
///   GET    /db/:col/:key         get document by key
///   PUT    /db/:col/:key         update document
///   DELETE /db/:col/:key         delete document
///   GET    /db/:col              scan collection (limit, offset query params)
///   DELETE /db/:col              drop collection
///   GET    /collections          list collections
///   GET    /health               health check
///   GET    /metrics              server metrics
const std = @import("std");
const collection = @import("collection.zig");
const Database = collection.Database;

const MAX_REQ  = 65536;  // 64 KiB
const MAX_RESP = 131072; // 128 KiB
const MAX_BODY = 65536;  // 64 KiB

// Heap-allocated per-connection buffers (threadlocal pointers set in handleConn).
// This avoids large threadlocal TLS segments that break in Release mode on macOS.
const ConnBufs = struct {
    req:  [MAX_REQ]u8,
    resp: [MAX_RESP]u8,
    body: [MAX_BODY]u8,
};
threadlocal var tl_bufs: ?*ConnBufs = null;

fn getRespBuf() *[MAX_RESP]u8 { return &tl_bufs.?.resp; }
fn getBodyBuf() *[MAX_BODY]u8 { return &tl_bufs.?.body; }

pub const Server = struct {
    db: *Database,
    port: u16,
    running: std.atomic.Value(bool),
    alloc: std.mem.Allocator,

    // metrics
    req_count: std.atomic.Value(u64),
    err_count: std.atomic.Value(u64),

    pub fn init(alloc: std.mem.Allocator, db: *Database, port: u16) Server {
        return .{
            .db      = db,
            .port    = port,
            .running = std.atomic.Value(bool).init(false),
            .alloc   = alloc,
            .req_count = std.atomic.Value(u64).init(0),
            .err_count = std.atomic.Value(u64).init(0),
        };
    }

    pub fn run(self: *Server) !void {
        const addr = try std.net.Address.parseIp("0.0.0.0", self.port);
        var listener = try addr.listen(.{
            .reuse_address = true,
            .kernel_backlog = 256,
        });
        defer listener.deinit();

        self.running.store(true, .release);
        std.log.info("TurboDB listening on :{d}", .{self.port});

        while (self.running.load(.acquire)) {
            const conn = listener.accept() catch |e| {
                if (e == error.WouldBlock) continue;
                std.log.err("accept: {}", .{e});
                continue;
            };
            const t = try std.Thread.spawn(.{}, handleConn, .{self, conn});
            t.detach();
        }
    }

    pub fn stop(self: *Server) void {
        self.running.store(false, .release);
    }
};

fn handleConn(srv: *Server, conn: std.net.Server.Connection) void {
    defer conn.stream.close();

    const bufs = std.heap.page_allocator.create(ConnBufs) catch {
        return;
    };
    defer std.heap.page_allocator.destroy(bufs);
    tl_bufs = bufs;
    defer tl_bufs = null;

    while (true) {
        const n = conn.stream.read(&bufs.req) catch return;
        if (n == 0) return;
        _ = srv.req_count.fetchAdd(1, .monotonic);

        const resp_len = dispatch(srv, bufs.req[0..n], std.heap.page_allocator);
        conn.stream.writeAll(bufs.resp[0..resp_len]) catch return;
    }
}
fn dispatch(srv: *Server, raw: []const u8, alloc: std.mem.Allocator) usize {
    // Parse request line.
    const nl = std.mem.indexOfScalar(u8, raw, '\n') orelse return err(400, "bad request");
    const req_line = std.mem.trimRight(u8, raw[0..nl], "\r");
    var parts = std.mem.splitScalar(u8, req_line, ' ');
    const method = parts.next() orelse return err(400, "bad request");
    const full_path = parts.next() orelse return err(400, "bad request");

    // Split path from query string.
    var path = full_path;
    var query: []const u8 = "";
    if (std.mem.indexOfScalar(u8, full_path, '?')) |qi| {
        path = full_path[0..qi];
        query = full_path[qi + 1 ..];
    }

    // Body.
    const body = if (std.mem.indexOf(u8, raw, "\r\n\r\n")) |p| raw[p + 4 ..]
                 else if (std.mem.indexOf(u8, raw, "\n\n")) |p| raw[p + 2 ..]
                 else @as([]const u8, "");

    // Route: /health
    if (std.mem.eql(u8, path, "/health")) {
        var hb: [64]u8 = undefined;
        const health_json = std.fmt.bufPrint(&hb, "{{\"status\":\"ok\",\"engine\":\"TurboDB\"}}", .{}) catch "{}";
        return ok(health_json);
    }

    // Route: /metrics
    if (std.mem.eql(u8, path, "/metrics")) {
        var fbs = std.io.fixedBufferStream(getBodyBuf());
        std.fmt.format(fbs.writer(),
            "{{\"requests\":{d},\"errors\":{d}}}",
            .{ srv.req_count.load(.acquire), srv.err_count.load(.acquire) }) catch {};
        return ok(getBodyBuf()[0..fbs.pos]);
    }

    // Route: /collections
    if (std.mem.eql(u8, path, "/collections") and std.mem.eql(u8, method, "GET"))
        return handleListCollections(srv, alloc);

    // Route: /search/:col?q=...
    if (std.mem.startsWith(u8, path, "/search/") and std.mem.eql(u8, method, "GET")) {
        const col_name = path[8..];
        const q = qparam(query, "q") orelse return err(400, "missing q parameter");
        const limit_val: u32 = qparamInt(query, "limit") orelse 50;
        return handleSearch(srv, col_name, q, limit_val, alloc);
    }

    // Routes under /db/:col
    if (std.mem.startsWith(u8, path, "/db/")) {
        const rest = path[4..];
        // /db/:col/:key
        if (std.mem.indexOfScalar(u8, rest, '/')) |sep| {
            const col_name = rest[0..sep];
            const key = rest[sep + 1 ..];
            if (std.mem.eql(u8, method, "GET"))    return handleGet(srv, col_name, key);
            if (std.mem.eql(u8, method, "PUT"))    return handleUpdate(srv, col_name, key, body, alloc);
            if (std.mem.eql(u8, method, "DELETE")) return handleDelete(srv, col_name, key);
        } else {
            const col_name = rest;
            if (std.mem.eql(u8, method, "POST"))   return handleInsert(srv, col_name, body, alloc);
            if (std.mem.eql(u8, method, "GET"))    return handleScan(srv, col_name, query, alloc);
            if (std.mem.eql(u8, method, "DELETE")) return handleDrop(srv, col_name);
        }
    }

    _ = srv.err_count.fetchAdd(1, .monotonic);
    return err(404, "not found");
}

// ─── handlers ────────────────────────────────────────────────────────────

fn handleInsert(srv: *Server, col_name: []const u8, body: []const u8, alloc: std.mem.Allocator) usize {
    _ = alloc;
    // Expect body: {"key":"...","value":{...}}  OR  use auto-generated key.
    const key_raw = jsonStr(body, "key") orelse {
        // Auto-generate key from timestamp + counter.
        var kb: [32]u8 = undefined;
        const k = std.fmt.bufPrint(&kb, "doc_{d}", .{std.time.milliTimestamp()}) catch
            return err(400, "bad key");
        return doInsert(srv, col_name, k, body);
    };
    return doInsert(srv, col_name, key_raw, body);
}

fn doInsert(srv: *Server, col_name: []const u8, key: []const u8, value: []const u8) usize {
    const col = srv.db.collection(col_name) catch return err(500, "open collection failed");
    const doc_id = col.insert(key, value) catch return err(500, "insert failed");
    var fbs = std.io.fixedBufferStream(getBodyBuf());
    std.fmt.format(fbs.writer(),
        "{{\"doc_id\":{d},\"key\":\"{s}\",\"collection\":\"{s}\"}}",
        .{ doc_id, key, col_name }) catch {};
    return ok(getBodyBuf()[0..fbs.pos]);
}

fn handleGet(srv: *Server, col_name: []const u8, key: []const u8) usize {
    const col = srv.db.collection(col_name) catch return err(500, "open collection failed");
    const d = col.get(key) orelse return err(404, "not found");
    var fbs = std.io.fixedBufferStream(getBodyBuf());
    std.fmt.format(fbs.writer(),
        "{{\"doc_id\":{d},\"key\":\"{s}\",\"version\":{d},\"value\":{s}}}",
        .{ d.header.doc_id, d.key, d.header.version,
           if (d.value.len > 0) d.value else "{}" }) catch {};
    return ok(getBodyBuf()[0..fbs.pos]);
}

fn handleUpdate(srv: *Server, col_name: []const u8, key: []const u8, body: []const u8, alloc: std.mem.Allocator) usize {
    _ = alloc;
    const col = srv.db.collection(col_name) catch return err(500, "open collection failed");
    const updated = col.update(key, body) catch return err(500, "update failed");
    if (!updated) return err(404, "not found");
    return ok("{\"updated\":true}");
}

fn handleDelete(srv: *Server, col_name: []const u8, key: []const u8) usize {
    const col = srv.db.collection(col_name) catch return err(500, "open collection failed");
    const deleted = col.delete(key) catch return err(500, "delete failed");
    if (!deleted) return err(404, "not found");
    return ok("{\"deleted\":true}");
}

fn handleScan(srv: *Server, col_name: []const u8, query_str: []const u8, alloc: std.mem.Allocator) usize {
    const limit: u32 = qparamInt(query_str, "limit") orelse 20;
    const offset: u32 = qparamInt(query_str, "offset") orelse 0;
    const col = srv.db.collection(col_name) catch return err(500, "open collection failed");
    const result = col.scan(limit, offset, alloc) catch return err(500, "scan failed");
    defer result.deinit();

    var fbs = std.io.fixedBufferStream(getBodyBuf());
    const w = fbs.writer();
    std.fmt.format(w, "{{\"collection\":\"{s}\",\"count\":{d},\"docs\":[",
        .{ col_name, result.docs.len }) catch {};
    for (result.docs, 0..) |d, i| {
        if (i > 0) w.writeByte(',') catch {};
        std.fmt.format(w,
            "{{\"doc_id\":{d},\"key\":\"{s}\",\"version\":{d},\"value\":{s}}}",
            .{ d.header.doc_id, d.key, d.header.version,
               if (d.value.len > 0) d.value else "{}" }) catch {};
    }
    w.writeAll("]}") catch {};
    return ok(getBodyBuf()[0..fbs.pos]);
}

fn handleDrop(srv: *Server, col_name: []const u8) usize {
    srv.db.dropCollection(col_name);
    return ok("{\"dropped\":true}");
}

fn handleSearch(srv: *Server, col_name: []const u8, query: []const u8, limit: u32, alloc: std.mem.Allocator) usize {
    const col = srv.db.collection(col_name) catch return err(500, "open collection failed");
    const result = col.searchText(query, limit, alloc) catch return err(500, "search failed");
    defer result.deinit();

    var fbs = std.io.fixedBufferStream(getBodyBuf());
    const w = fbs.writer();
    std.fmt.format(w,
        "{{\"query\":\"{s}\",\"hits\":{d},\"candidates\":{d},\"total_files\":{d},\"results\":[",
        .{ query, result.docs.len, result.candidate_paths.len, result.total_files }) catch {};
    for (result.docs, 0..) |d, i| {
        if (i > 0) w.writeByte(',') catch {};
        std.fmt.format(w,
            "{{\"doc_id\":{d},\"key\":\"{s}\",\"value\":{s}}}",
            .{ d.header.doc_id, d.key,
               if (d.value.len > 0) d.value else "{}" }) catch {};
    }
    w.writeAll("]}") catch {};
    return ok(getBodyBuf()[0..fbs.pos]);
}

fn handleListCollections(srv: *Server, alloc: std.mem.Allocator) usize {
    _ = alloc;
    var fbs = std.io.fixedBufferStream(getBodyBuf());
    const w = fbs.writer();
    w.writeAll("{\"collections\":[") catch {};
    var it = srv.db.collections.keyIterator();
    var first = true;
    while (it.next()) |k| {
        if (!first) w.writeByte(',') catch {};
        first = false;
        std.fmt.format(w, "\"{s}\"", .{k.*}) catch {};
    }
    w.writeAll("]}") catch {};
    return ok(getBodyBuf()[0..fbs.pos]);
}

// ─── response helpers ────────────────────────────────────────────────────
// ─── response helpers ────────────────────────────────────────────────────

fn ok(body: []const u8) usize {
    return respond(200, "OK", body);
}

fn err(code: u16, msg: []const u8) usize {
    var scratch: [256]u8 = undefined;
    const body = std.fmt.bufPrint(&scratch, "{{\"error\":\"{s}\"}}", .{msg}) catch msg;
    const status = switch (code) {
        400 => "Bad Request",
        404 => "Not Found",
        else => "Internal Server Error",
    };
    return respond(code, status, body);
}

fn respond(code: u16, status: []const u8, body: []const u8) usize {
    var fbs = std.io.fixedBufferStream(getRespBuf());
    std.fmt.format(fbs.writer(),
        "HTTP/1.1 {d} {s}\r\nContent-Type: application/json\r\nContent-Length: {d}\r\nConnection: keep-alive\r\n\r\n{s}",
        .{ code, status, body.len, body }) catch {};
    return fbs.pos;
}

// ─── mini parsers ────────────────────────────────────────────────────────

fn jsonStr(json: []const u8, key: []const u8) ?[]const u8 {
    var kbuf: [64]u8 = undefined;
    const needle = std.fmt.bufPrint(&kbuf, "\"{s}\":\"", .{key}) catch return null;
    const pos = std.mem.indexOf(u8, json, needle) orelse return null;
    const start = pos + needle.len;
    const end = std.mem.indexOfScalarPos(u8, json, start, '"') orelse return null;
    return json[start..end];
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
