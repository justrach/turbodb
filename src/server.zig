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
///   GET    /context/:col         smart context discovery (q, limit query params)
const std = @import("std");
const activity = @import("activity.zig");
const auth = @import("auth.zig");
const collection = @import("collection.zig");
const Database = collection.Database;

const MAX_REQ  = 65536;  // 64 KiB (initial read)
const MAX_RESP = 131072; // 128 KiB
const MAX_BODY = 65536;  // 64 KiB
const MAX_BULK = 16 * 1024 * 1024; // 16 MiB for bulk inserts

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
    pub const QueryCost = struct {
        tenant_id: [64]u8,
        tenant_id_len: u8,
        op: [16]u8,
        op_len: u8,
        rows_scanned: u64,
        bytes_read: u64,
        cpu_us: u64,
        cost_nanos_usd: u64,
    };

    const MAX_CONNECTIONS: u32 = 512;
    const BILLING_LOG_CAP: usize = 1024;

    db: *Database,
    port: u16,
    running: std.atomic.Value(bool),
    alloc: std.mem.Allocator,

    // metrics
    req_count: std.atomic.Value(u64),
    err_count: std.atomic.Value(u64),
    query_count: std.atomic.Value(u64),
    query_rows_scanned: std.atomic.Value(u64),
    query_bytes_read: std.atomic.Value(u64),
    query_cpu_us: std.atomic.Value(u64),
    query_cost_nanos_usd: std.atomic.Value(u64),
    // Ring buffer for billing log — avoids O(n) orderedRemove(0).
    billing_ring: [BILLING_LOG_CAP]QueryCost,
    billing_ring_head: usize,  // next write position
    billing_ring_count: usize, // entries currently stored
    billing_mu: std.Thread.Mutex,
    activity: activity.ActivityTracker,
    // Connection limiter — prevents unbounded thread spawning under flood.
    active_conns: std.atomic.Value(u32),

    pub fn init(alloc: std.mem.Allocator, db: *Database, port: u16) Server {
        return .{
            .db      = db,
            .port    = port,
            .running = std.atomic.Value(bool).init(false),
            .alloc   = alloc,
            .req_count = std.atomic.Value(u64).init(0),
            .err_count = std.atomic.Value(u64).init(0),
            .query_count = std.atomic.Value(u64).init(0),
            .query_rows_scanned = std.atomic.Value(u64).init(0),
            .query_bytes_read = std.atomic.Value(u64).init(0),
            .query_cpu_us = std.atomic.Value(u64).init(0),
            .query_cost_nanos_usd = std.atomic.Value(u64).init(0),
            .billing_ring = undefined,
            .billing_ring_head = 0,
            .billing_ring_count = 0,
            .billing_mu = .{},
            .activity = activity.ActivityTracker.init(),
            .active_conns = std.atomic.Value(u32).init(0),
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
            // Reject if at connection limit to prevent OOM from thread exhaustion.
            if (self.active_conns.load(.monotonic) >= MAX_CONNECTIONS) {
                // Return HTTP 503 instead of silent RST so clients can retry.
                const reject = "HTTP/1.1 503 Service Unavailable\r\nContent-Length: 31\r\nConnection: close\r\n\r\n{\"error\":\"too many connections\"}";
                conn.stream.writeAll(reject) catch {};
                conn.stream.close();
                _ = self.err_count.fetchAdd(1, .monotonic);
                continue;
            }
            const t = std.Thread.spawn(.{ .stack_size = 256 * 1024 }, handleConnWrapped, .{self, conn}) catch {
                conn.stream.close();
                _ = self.err_count.fetchAdd(1, .monotonic);
                continue;
            };
            t.detach();
        }
    }

    pub fn runUnix(self: *Server, path: []const u8) !void {
        // Remove any existing socket file
        std.posix.unlink(path) catch {};
        const fd = try std.posix.socket(std.posix.AF.UNIX, std.posix.SOCK.STREAM, 0);
        defer std.posix.close(fd);

        // Construct sockaddr_un
        var addr: std.posix.sockaddr.un = .{ .family = std.posix.AF.UNIX, .path = undefined };
        @memset(&addr.path, 0);
        if (path.len >= addr.path.len) return error.PathTooLong;
        @memcpy(addr.path[0..path.len], path);

        try std.posix.bind(fd, @ptrCast(&addr), @sizeOf(std.posix.sockaddr.un));
        try std.posix.listen(fd, 256);

        self.running.store(true, .release);
        std.log.info("TurboDB HTTP on unix:{s}", .{path});

        while (self.running.load(.acquire)) {
            var client_addr: std.posix.sockaddr.un = undefined;
            var addr_len: std.posix.socklen_t = @sizeOf(std.posix.sockaddr.un);
            const client_fd = std.posix.accept(fd, @ptrCast(&client_addr), &addr_len, 0) catch continue;
            const stream = std.net.Stream{ .handle = client_fd };
            const conn = std.net.Server.Connection{ .stream = stream, .address = std.net.Address.initUnix(path) catch continue };
            if (self.active_conns.load(.monotonic) >= MAX_CONNECTIONS) {
                const reject = "HTTP/1.1 503 Service Unavailable\r\nContent-Length: 31\r\nConnection: close\r\n\r\n{\"error\":\"too many connections\"}";
                conn.stream.writeAll(reject) catch {};
                conn.stream.close();
                _ = self.err_count.fetchAdd(1, .monotonic);
                continue;
            }
            const t = std.Thread.spawn(.{ .stack_size = 256 * 1024 }, handleConnWrapped, .{ self, conn }) catch {
                conn.stream.close();
                continue;
            };
            t.detach();
        }
    }

    pub fn stop(self: *Server) void {
        self.running.store(false, .release);
    }

    fn recordQueryCost(self: *Server, tenant_id: []const u8, op: []const u8, rows_scanned: usize, bytes_read: usize, start_ns: i128) void {
        const elapsed_ns = std.time.nanoTimestamp() - start_ns;
        const cpu_us: u64 = @intCast(@max(@divTrunc(elapsed_ns, 1000), 0));
        const cost_nanos_usd: u64 = cpu_us * 5 + @as(u64, @intCast(rows_scanned)) + @as(u64, @intCast(bytes_read / 1024));

        _ = self.query_count.fetchAdd(1, .monotonic);
        _ = self.query_rows_scanned.fetchAdd(rows_scanned, .monotonic);
        _ = self.query_bytes_read.fetchAdd(bytes_read, .monotonic);
        _ = self.query_cpu_us.fetchAdd(cpu_us, .monotonic);
        _ = self.query_cost_nanos_usd.fetchAdd(cost_nanos_usd, .monotonic);
        self.activity.recordQuery();

        var entry = QueryCost{
            .tenant_id = [_]u8{0} ** 64,
            .tenant_id_len = @intCast(@min(tenant_id.len, 64)),
            .op = [_]u8{0} ** 16,
            .op_len = @intCast(@min(op.len, 16)),
            .rows_scanned = rows_scanned,
            .bytes_read = bytes_read,
            .cpu_us = cpu_us,
            .cost_nanos_usd = cost_nanos_usd,
        };
        @memcpy(entry.tenant_id[0..entry.tenant_id_len], tenant_id[0..entry.tenant_id_len]);
        @memcpy(entry.op[0..entry.op_len], op[0..entry.op_len]);

        // O(1) ring buffer append — overwrites oldest entry when full.
        self.billing_mu.lock();
        defer self.billing_mu.unlock();
        self.billing_ring[self.billing_ring_head] = entry;
        self.billing_ring_head = (self.billing_ring_head + 1) % BILLING_LOG_CAP;
        if (self.billing_ring_count < BILLING_LOG_CAP) {
            self.billing_ring_count += 1;
        }
    }
};

/// Wrapper that tracks active connection count around handleConn.
fn handleConnWrapped(srv: *Server, conn: std.net.Server.Connection) void {
    _ = srv.active_conns.fetchAdd(1, .monotonic);
    defer _ = srv.active_conns.fetchSub(1, .monotonic);
    handleConn(srv, conn);
}

fn handleConn(srv: *Server, conn: std.net.Server.Connection) void {
    defer conn.stream.close();

    const bufs = std.heap.page_allocator.create(ConnBufs) catch {
        return;
    };
    defer std.heap.page_allocator.destroy(bufs);
    tl_bufs = bufs;
    defer tl_bufs = null;

    while (true) {
        var n = conn.stream.read(&bufs.req) catch return;
        if (n == 0) return;
        _ = srv.req_count.fetchAdd(1, .monotonic);

        const initial = bufs.req[0..n];

        // WebSocket upgrade: switch to persistent framed mode.
        if (headerContains(initial, "upgrade", "websocket")) {
            handleWebSocket(srv, conn, initial) catch {};
            return; // WS handler owns the connection until close
        }

        // For bulk inserts: read the full body based on Content-Length.
        // The initial read may only contain part of a large body.
        const content_length = extractContentLength(initial);
        const is_bulk = std.mem.indexOf(u8, initial[0..@min(n, 256)], "/bulk") != null;

        if (content_length > MAX_REQ and content_length <= MAX_BULK and is_bulk) {
            // Large bulk: allocate a big buffer and read the full body.
            const header_end = if (std.mem.indexOf(u8, initial, "\r\n\r\n")) |p| p + 4
                else if (std.mem.indexOf(u8, initial, "\n\n")) |p| p + 2
                else n;
            const total_size = header_end + content_length;
            if (total_size <= MAX_BULK) {
                const big_buf = std.heap.page_allocator.alloc(u8, total_size) catch {
                    const resp_len = dispatch(srv, initial, std.heap.page_allocator);
                    conn.stream.writeAll(bufs.resp[0..resp_len]) catch return;
                    continue;
                };
                defer std.heap.page_allocator.free(big_buf);
                @memcpy(big_buf[0..n], initial);
                // Read remaining bytes
                while (n < total_size) {
                    const r = conn.stream.read(big_buf[n..total_size]) catch break;
                    if (r == 0) break;
                    n += r;
                }
                const resp_len = dispatch(srv, big_buf[0..n], std.heap.page_allocator);
                conn.stream.writeAll(bufs.resp[0..resp_len]) catch return;
                continue;
            }
        } else if (content_length > 0 and content_length <= MAX_REQ and is_bulk) {
            // Small bulk: body fits in the req buffer but the initial read may
            // not have received it all.  Keep reading until we have the full body.
            const header_end = if (std.mem.indexOf(u8, initial, "\r\n\r\n")) |p| p + 4
                else if (std.mem.indexOf(u8, initial, "\n\n")) |p| p + 2
                else n;
            const total_size = @min(header_end + content_length, bufs.req.len);
            while (n < total_size) {
                const r = conn.stream.read(bufs.req[n..total_size]) catch break;
                if (r == 0) break;
                n += r;
            }
            const resp_len = dispatch(srv, bufs.req[0..n], std.heap.page_allocator);
            conn.stream.writeAll(bufs.resp[0..resp_len]) catch return;
            continue;
        }

        const resp_len = dispatch(srv, initial, std.heap.page_allocator);
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
            "{{\"requests\":{d},\"errors\":{d},\"queries\":{d},\"rows_scanned\":{d},\"bytes_read\":{d},\"cpu_us\":{d},\"cost_nanos_usd\":{d}}}",
            .{
                srv.req_count.load(.acquire),
                srv.err_count.load(.acquire),
                srv.query_count.load(.acquire),
                srv.query_rows_scanned.load(.acquire),
                srv.query_bytes_read.load(.acquire),
                srv.query_cpu_us.load(.acquire),
                srv.query_cost_nanos_usd.load(.acquire),
            }) catch {};
        return ok(getBodyBuf()[0..fbs.pos]);
    }

    // ── Auth gate — public endpoints above, protected endpoints below ────
    if (srv.db.auth.isEnabled()) {
        const api_key = auth.AuthStore.extractHttpKey(raw) orelse
            return err(401, "unauthorized — missing X-Api-Key header");
        if (srv.db.auth.verify(api_key) == null)
            return err(401, "unauthorized — invalid API key");
    }

    if (std.mem.eql(u8, path, "/billing") and std.mem.eql(u8, method, "GET"))
        return handleBillingLog(srv);

    if (std.mem.eql(u8, path, "/resource_state") and std.mem.eql(u8, method, "GET"))
        return handleResourceState(srv);

    if (std.mem.eql(u8, path, "/cdc/webhooks") and std.mem.eql(u8, method, "POST"))
        return handleWebhookRegistration(srv, body);

    if (std.mem.eql(u8, path, "/cdc/events") and std.mem.eql(u8, method, "GET"))
        return handleCdcEvents(srv, qparam(query, "tenant"), alloc);

    // Route: /collections
    if (std.mem.eql(u8, path, "/collections") and std.mem.eql(u8, method, "GET"))
        return handleListCollections(srv, requestTenant(raw, query), alloc);

    // Route: /search/:col?q=...
    if (std.mem.startsWith(u8, path, "/search/") and std.mem.eql(u8, method, "GET")) {
        const col_name = path[8..];
        const q_raw = qparam(query, "q") orelse return err(400, "missing q parameter");
        var decode_buf: [4096]u8 = undefined;
        const q = urlDecode(q_raw, &decode_buf) orelse q_raw;
        const limit_val: u32 = @min(qparamInt(query, "limit") orelse 50, 500);
        return handleSearch(srv, requestTenant(raw, query), col_name, q, limit_val, alloc);
    }

    // Route: /context/:col?q=...&limit=20
    if (std.mem.startsWith(u8, path, "/context/") and std.mem.eql(u8, method, "GET")) {
        const col_name = path[9..];
        const q_raw = qparam(query, "q") orelse return err(400, "missing q parameter");
        var decode_buf: [4096]u8 = undefined;
        const q = urlDecode(q_raw, &decode_buf) orelse q_raw;
        const limit_val: u32 = @min(qparamInt(query, "limit") orelse 20, 100);
        return handleDiscoverContext(srv, requestTenant(raw, query), col_name, q, limit_val, alloc);
    }

    // Routes under /branch/:col[/:branch[/:key]]
    if (std.mem.startsWith(u8, path, "/branch/")) {
        const rest = path[8..]; // after "/branch/"
        const tenant_id = requestTenant(raw, query);

        // Parse: rest = col_name[/branch_name[/key...]]
        if (std.mem.indexOfScalar(u8, rest, '/')) |sep1| {
            const col_name = rest[0..sep1];
            const after_col = rest[sep1 + 1 ..];

            if (std.mem.indexOfScalar(u8, after_col, '/')) |sep2| {
                const branch_name = after_col[0..sep2];
                const key_or_action = after_col[sep2 + 1 ..];

                // POST /branch/:col/:branch/merge
                if (std.mem.eql(u8, key_or_action, "merge") and std.mem.eql(u8, method, "POST")) {
                    return handleBranchMerge(srv, tenant_id, col_name, branch_name, alloc);
                }
                // GET /branch/:col/:branch/search?q=...
                if (std.mem.eql(u8, key_or_action, "search") and std.mem.eql(u8, method, "GET")) {
                    const q_raw = qparam(query, "q") orelse return err(400, "missing q parameter");
                    var decode_buf: [4096]u8 = undefined;
                    const q = urlDecode(q_raw, &decode_buf) orelse q_raw;
                    const limit_val: u32 = qparamInt(query, "limit") orelse 50;
                    return handleBranchSearch(srv, tenant_id, col_name, branch_name, q, limit_val, alloc);
                }
                // PUT /branch/:col/:branch/:key — write on branch
                if (std.mem.eql(u8, method, "PUT")) {
                    return handleBranchWrite(srv, tenant_id, col_name, branch_name, key_or_action, body);
                }
                // GET /branch/:col/:branch/:key — read on branch
                if (std.mem.eql(u8, method, "GET")) {
                    return handleBranchRead(srv, tenant_id, col_name, branch_name, key_or_action);
                }
            } else {
                // No second slash — this is /branch/:col/:branch (no key)
                // Could be used for branch-level ops if needed
            }
        } else {
            // No slash after col_name: /branch/:col
            const col_name = rest;
            // POST /branch/:col — create branch (body: {"name":"...","agent_id":"..."})
            if (std.mem.eql(u8, method, "POST")) {
                return handleCreateBranch(srv, tenant_id, col_name, body);
            }
            // GET /branch/:col — list branches
            if (std.mem.eql(u8, method, "GET")) {
                return handleListBranches(srv, tenant_id, col_name, alloc);
            }
        }
    }

    // Routes under /db/:col
    if (std.mem.startsWith(u8, path, "/db/")) {
        const rest = path[4..];
        // /db/:col/:key
        if (std.mem.indexOfScalar(u8, rest, '/')) |sep| {
            const col_name = rest[0..sep];
            const key = rest[sep + 1 ..];
            const tenant_id = requestTenant(raw, query);
            // POST /db/:col/bulk — bulk insert
            if (std.mem.eql(u8, key, "bulk") and std.mem.eql(u8, method, "POST"))
                return handleBulkInsert(srv, tenant_id, col_name, body, alloc);
            if (std.mem.eql(u8, method, "GET"))    return handleGet(srv, tenant_id, col_name, key, requestAsOf(raw, query));
            if (std.mem.eql(u8, method, "PUT"))    return handleUpdate(srv, tenant_id, col_name, key, body, alloc);
            if (std.mem.eql(u8, method, "DELETE")) return handleDelete(srv, tenant_id, col_name, key);
        } else {
            const col_name = rest;
            const tenant_id = requestTenant(raw, query);
            if (std.mem.eql(u8, method, "POST"))   return handleInsert(srv, tenant_id, col_name, body, alloc);
            if (std.mem.eql(u8, method, "GET"))    return handleScan(srv, tenant_id, col_name, query, requestAsOf(raw, query), alloc);
            if (std.mem.eql(u8, method, "DELETE")) return handleDrop(srv, tenant_id, col_name);
        }
    }

    _ = srv.err_count.fetchAdd(1, .monotonic);
    return err(404, "not found");
}

// ─── handlers ────────────────────────────────────────────────────────────

fn handleInsert(srv: *Server, tenant_id: []const u8, col_name: []const u8, body: []const u8, alloc: std.mem.Allocator) usize {
    _ = alloc;
    // Expect body: {"key":"...","value":{...}}  OR  use auto-generated key.
    // Extract the value field; fall back to the full body for backwards compat.
    const value = jsonValue(body, "value") orelse body;
    const key_raw = jsonStr(body, "key") orelse {
        // Auto-generate key from timestamp + counter.
        var kb: [32]u8 = undefined;
        const k = std.fmt.bufPrint(&kb, "doc_{d}", .{std.time.milliTimestamp()}) catch
            return err(400, "bad key");
        return doInsert(srv, tenant_id, col_name, k, value);
    };
    return doInsert(srv, tenant_id, col_name, key_raw, value);
}

fn doInsert(srv: *Server, tenant_id: []const u8, col_name: []const u8, key: []const u8, value: []const u8) usize {
    const start_ns = std.time.nanoTimestamp();
    srv.db.recordTenantOperation(tenant_id) catch return err(429, "tenant ops quota exceeded");
    srv.db.ensureTenantStorageAvailable(tenant_id, value.len) catch return err(429, "tenant storage quota exceeded");
    const col = srv.db.collectionForTenant(tenant_id, col_name) catch return err(500, "open collection failed");
    const doc_id = col.insert(key, value) catch return err(500, "insert failed");
    srv.recordQueryCost(tenant_id, "insert", 1, value.len, start_ns);
    var fbs = std.io.fixedBufferStream(getBodyBuf());
    std.fmt.format(fbs.writer(),
        "{{\"doc_id\":{d},\"key\":\"{s}\",\"collection\":\"{s}\",\"tenant\":\"{s}\"}}",
        .{ doc_id, key, col_name, tenant_id }) catch {};
    return ok(getBodyBuf()[0..fbs.pos]);
}

/// POST /db/:col/bulk — insert multiple documents in one request.
/// Body: NDJSON — one {"key":"...","value":"..."} per line.
/// Response: {"inserted":N,"errors":M,"collection":"...","tenant":"..."}
fn handleBulkInsert(srv: *Server, tenant_id: []const u8, col_name: []const u8, body: []const u8, alloc: std.mem.Allocator) usize {
    _ = alloc;
    const start_ns = std.time.nanoTimestamp();
    srv.db.recordTenantOperation(tenant_id) catch return err(429, "tenant ops quota exceeded");
    srv.db.ensureTenantStorageAvailable(tenant_id, body.len) catch return err(429, "tenant storage quota exceeded");
    const col = srv.db.collectionForTenant(tenant_id, col_name) catch return err(500, "open collection failed");

    var inserted: u32 = 0;
    var errors: u32 = 0;
    var total_bytes: u64 = 0;

    // Parse NDJSON: iterate lines, each is a {"key":"...","value":...} object
    var pos: usize = 0;
    while (pos < body.len) {
        // Find end of line
        const line_end = std.mem.indexOfScalarPos(u8, body, pos, '\n') orelse body.len;
        const line = std.mem.trim(u8, body[pos..line_end], " \t\r");
        pos = line_end + 1;

        if (line.len < 2) continue; // skip empty lines

        // Extract key from this JSON line
        const key = jsonStr(line, "key") orelse continue;
        // Extract value field; fall back to full line for backwards compat.
        const value = jsonValue(line, "value") orelse line;

        _ = col.insert(key, value) catch {
            errors += 1;
            continue;
        };
        inserted += 1;
        total_bytes += line.len;
    }

    srv.recordQueryCost(tenant_id, "bulk_insert", inserted, total_bytes, start_ns);

    var fbs = std.io.fixedBufferStream(getBodyBuf());
    std.fmt.format(fbs.writer(),
        "{{\"inserted\":{d},\"errors\":{d},\"collection\":\"{s}\",\"tenant\":\"{s}\"}}",
        .{ inserted, errors, col_name, tenant_id }) catch {};
    return ok(getBodyBuf()[0..fbs.pos]);
}
fn handleGet(srv: *Server, tenant_id: []const u8, col_name: []const u8, key: []const u8, as_of: ?i64) usize {
        const start_ns = std.time.nanoTimestamp();
        srv.db.recordTenantOperation(tenant_id) catch return err(429, "tenant ops quota exceeded");
        const col = srv.db.collectionForTenant(tenant_id, col_name) catch return err(500, "open collection failed");
        const d = if (as_of) |ts_ms|
            (col.getAsOfTimestamp(key, ts_ms) orelse return err(404, "not found"))
        else
            (col.get(key) orelse return err(404, "not found"));
        srv.recordQueryCost(tenant_id, "get", 1, d.key.len + d.value.len, start_ns);

        // Write JSON body directly into resp_buf at offset 256 (reserve space for headers)
        const HEADER_RESERVE = 256;
        var resp = getRespBuf();
        var fbs = std.io.fixedBufferStream(resp[HEADER_RESERVE..]);
        const val = if (d.value.len > 0) d.value else "{}";
        const is_json = val.len > 0 and (val[0] == '{' or val[0] == '[' or val[0] == '"');
        const w = fbs.writer();
        if (is_json) {
            std.fmt.format(w, "{{\"doc_id\":{d},\"key\":\"{s}\",\"version\":{d},\"value\":{s}}}", .{ d.header.doc_id, d.key, d.header.version, val }) catch {};
        } else {
            std.fmt.format(w, "{{\"doc_id\":{d},\"key\":\"{s}\",\"version\":{d},\"value\":\"", .{ d.header.doc_id, d.key, d.header.version }) catch {};
            for (val) |ch| {
                if (ch == '"' or ch == '\\') w.writeByte('\\') catch {};
                if (ch == '\n') { w.writeAll("\\n") catch {}; continue; }
                w.writeByte(ch) catch {};
            }
            w.writeAll("\"}") catch {};
        }
        const body_len = fbs.pos;

        // Now write headers into the reserved space at the front
        var hdr_fbs = std.io.fixedBufferStream(resp[0..HEADER_RESERVE]);
        std.fmt.format(hdr_fbs.writer(),
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {d}\r\nConnection: keep-alive\r\n\r\n",
            .{body_len}) catch {};
        const hdr_len = hdr_fbs.pos;

        // Move body right after headers (memmove if needed)
        if (hdr_len < HEADER_RESERVE) {
            std.mem.copyForwards(u8, resp[hdr_len .. hdr_len + body_len], resp[HEADER_RESERVE .. HEADER_RESERVE + body_len]);
        }
        return hdr_len + body_len;
    }

fn handleUpdate(srv: *Server, tenant_id: []const u8, col_name: []const u8, key: []const u8, body: []const u8, alloc: std.mem.Allocator) usize {
    const start_ns = std.time.nanoTimestamp();
    _ = alloc;
    srv.db.recordTenantOperation(tenant_id) catch return err(429, "tenant ops quota exceeded");
    srv.db.ensureTenantStorageAvailable(tenant_id, body.len) catch return err(429, "tenant storage quota exceeded");
    const col = srv.db.collectionForTenant(tenant_id, col_name) catch return err(500, "open collection failed");
    const updated = col.update(key, body) catch return err(500, "update failed");
    if (!updated) return err(404, "not found");
    srv.recordQueryCost(tenant_id, "update", 1, body.len, start_ns);
    return ok("{\"updated\":true}");
}

fn handleDelete(srv: *Server, tenant_id: []const u8, col_name: []const u8, key: []const u8) usize {
    const start_ns = std.time.nanoTimestamp();
    srv.db.recordTenantOperation(tenant_id) catch return err(429, "tenant ops quota exceeded");
    const col = srv.db.collectionForTenant(tenant_id, col_name) catch return err(500, "open collection failed");
    const deleted = col.delete(key) catch return err(500, "delete failed");
    if (!deleted) return err(404, "not found");
    srv.recordQueryCost(tenant_id, "delete", 1, 0, start_ns);
    return ok("{\"deleted\":true}");
}

fn handleScan(srv: *Server, tenant_id: []const u8, col_name: []const u8, query_str: []const u8, as_of: ?i64, alloc: std.mem.Allocator) usize {
    const start_ns = std.time.nanoTimestamp();
    const limit: u32 = @min(qparamInt(query_str, "limit") orelse 20, 1000);
    const offset: u32 = qparamInt(query_str, "offset") orelse 0;
    srv.db.recordTenantOperation(tenant_id) catch return err(429, "tenant ops quota exceeded");
    const col = srv.db.collectionForTenant(tenant_id, col_name) catch return err(500, "open collection failed");
    const result = if (as_of) |ts_ms|
        col.scanAsOfTimestamp(ts_ms, limit, offset, alloc) catch return err(500, "scan failed")
    else
        col.scan(limit, offset, alloc) catch return err(500, "scan failed");
    defer result.deinit();
    var bytes_read: u64 = 0;
    for (result.docs) |d| bytes_read += d.key.len + d.value.len;
    srv.recordQueryCost(tenant_id, "scan", result.docs.len, bytes_read, start_ns);

    var fbs = std.io.fixedBufferStream(getBodyBuf());
    const w = fbs.writer();
    std.fmt.format(w, "{{\"tenant\":\"{s}\",\"collection\":\"{s}\",\"count\":{d},\"docs\":[",
        .{ tenant_id, col_name, result.docs.len }) catch {};
    for (result.docs, 0..) |d, i| {
        // Stop writing if buffer is nearly full to avoid truncated JSON.
        if (fbs.pos + 256 >= MAX_BODY) break;
        if (i > 0) w.writeByte(',') catch {};
        const val = if (d.value.len > 0) d.value else "{}";
        const is_json = val.len > 0 and (val[0] == '{' or val[0] == '[' or val[0] == '"');
        if (is_json) {
            std.fmt.format(w, "{{\"doc_id\":{d},\"key\":\"{s}\",\"version\":{d},\"value\":{s}}}", .{ d.header.doc_id, d.key, d.header.version, val }) catch {};
        } else {
            std.fmt.format(w, "{{\"doc_id\":{d},\"key\":\"{s}\",\"version\":{d},\"value\":\"", .{ d.header.doc_id, d.key, d.header.version }) catch {};
            for (val) |ch| {
                if (ch == '"' or ch == '\\') w.writeByte('\\') catch {};
                if (ch == '\n') { w.writeAll("\\n") catch {}; continue; }
                w.writeByte(ch) catch {};
            }
            w.writeAll("\"}") catch {};
        }
    }
    w.writeAll("]}") catch {};
    return ok(getBodyBuf()[0..fbs.pos]);

}
fn handleDrop(srv: *Server, tenant_id: []const u8, col_name: []const u8) usize {
    const start_ns = std.time.nanoTimestamp();
    srv.db.recordTenantOperation(tenant_id) catch return err(429, "tenant ops quota exceeded");
    srv.db.dropCollectionForTenant(tenant_id, col_name);
    srv.recordQueryCost(tenant_id, "drop", 0, 0, start_ns);
    return ok("{\"dropped\":true}");
}

fn handleSearch(srv: *Server, tenant_id: []const u8, col_name: []const u8, query: []const u8, limit: u32, alloc: std.mem.Allocator) usize {
    const start_ns = std.time.nanoTimestamp();
    srv.db.recordTenantOperation(tenant_id) catch return err(429, "tenant ops quota exceeded");
    const col = srv.db.collectionForTenant(tenant_id, col_name) catch return err(500, "open collection failed");
    const result = col.searchText(query, limit, alloc) catch return err(500, "search failed");
    defer result.deinit();
    var bytes_read: u64 = 0;
    for (result.docs) |d| bytes_read += d.key.len + d.value.len;
    srv.recordQueryCost(tenant_id, "search", result.docs.len, bytes_read, start_ns);

    var fbs = std.io.fixedBufferStream(getBodyBuf());
    const w = fbs.writer();
    // Write JSON header with escaped query string
    w.writeAll("{\"query\":\"") catch {};
    for (query) |ch| {
        if (ch == '"' or ch == '\\') { w.writeByte('\\') catch {}; }
        w.writeByte(ch) catch {};
    }
    std.fmt.format(w,
        "\",\"hits\":{d},\"candidates\":{d},\"total_docs\":{d},\"total_files\":{d},\"results\":[",
        .{ result.docs.len, result.candidate_paths.len, col.docCount(), result.total_files }) catch {};
    for (result.docs, 0..) |d, i| {
        if (fbs.pos + 256 >= MAX_BODY) break;
        if (i > 0) w.writeByte(',') catch {};
        // Output value as valid JSON — objects/arrays as-is, strings quoted
        const val = if (d.value.len > 0) d.value else "{}";
        const is_json = val.len > 0 and (val[0] == '{' or val[0] == '[' or val[0] == '"');
        if (is_json) {
            std.fmt.format(w, "{{\"doc_id\":{d},\"key\":\"{s}\",\"value\":{s}}}", .{ d.header.doc_id, d.key, val }) catch {};
        } else {
            w.writeAll("{\"doc_id\":") catch {};
            std.fmt.format(w, "{d},\"key\":\"{s}\",\"value\":\"", .{ d.header.doc_id, d.key }) catch {};
            for (val) |ch| {
                if (ch == '"' or ch == '\\') w.writeByte('\\') catch {};
                if (ch == '\n') { w.writeAll("\\n") catch {}; continue; }
                w.writeByte(ch) catch {};
            }
            w.writeAll("\"}") catch {};
        }
    }
    w.writeAll("]}") catch {};
    return ok(getBodyBuf()[0..fbs.pos]);
}

fn handleDiscoverContext(srv: *Server, tenant_id: []const u8, col_name: []const u8, query: []const u8, limit: u32, alloc: std.mem.Allocator) usize {
    const start_ns = std.time.nanoTimestamp();
    srv.db.recordTenantOperation(tenant_id) catch return err(429, "tenant ops quota exceeded");
    const col = srv.db.collectionForTenant(tenant_id, col_name) catch return err(500, "open collection failed");
    var result = col.discoverContext(query, limit, alloc) catch return err(500, "context discovery failed");
    defer result.deinit();
    var bytes_read: u64 = 0;
    for (result.matching_files) |d| bytes_read += d.key.len + d.value.len;
    srv.recordQueryCost(tenant_id, "context", result.matching_files.len, bytes_read, start_ns);

    var fbs = std.io.fixedBufferStream(getBodyBuf());
    const w = fbs.writer();

    // Write JSON: matching_files
    w.writeAll("{\"query\":\"") catch {};
    for (query) |ch| {
        if (ch == '"' or ch == '\\') { w.writeByte('\\') catch {}; }
        w.writeByte(ch) catch {};
    }
    w.writeAll("\",\"matching_files\":[") catch {};
    for (result.matching_files, 0..) |d, i| {
        if (fbs.pos + 128 >= MAX_BODY) break;
        if (i > 0) w.writeByte(',') catch {};
        std.fmt.format(w, "{{\"key\":\"{s}\",\"size\":{d}}}", .{ d.key, d.value.len }) catch {};
    }
    w.writeAll("],\"related_files\":[") catch {};
    for (result.related_files, 0..) |d, i| {
        if (fbs.pos + 128 >= MAX_BODY) break;
        if (i > 0) w.writeByte(',') catch {};
        std.fmt.format(w, "{{\"key\":\"{s}\"}}", .{d.key}) catch {};
    }
    w.writeAll("],\"test_files\":[") catch {};
    for (result.test_files, 0..) |d, i| {
        if (fbs.pos + 128 >= MAX_BODY) break;
        if (i > 0) w.writeByte(',') catch {};
        std.fmt.format(w, "{{\"key\":\"{s}\"}}", .{d.key}) catch {};
    }
    std.fmt.format(w, "],\"recent_versions\":{d},\"total_files\":{d}}}", .{ result.recent_versions, result.total_files }) catch {};
    return ok(getBodyBuf()[0..fbs.pos]);
}

fn handleListCollections(srv: *Server, tenant_id: []const u8, alloc: std.mem.Allocator) usize {
    const start_ns = std.time.nanoTimestamp();
    srv.db.recordTenantOperation(tenant_id) catch return err(429, "tenant ops quota exceeded");
    var cols = srv.db.listCollectionsForTenant(tenant_id, alloc) catch return err(500, "list collections failed");
    defer {
        for (cols.items) |name| alloc.free(name);
        cols.deinit(alloc);
    }
    var fbs = std.io.fixedBufferStream(getBodyBuf());
    const w = fbs.writer();
    std.fmt.format(w, "{{\"tenant\":\"{s}\",\"collections\":[", .{tenant_id}) catch {};
    for (cols.items, 0..) |name, i| {
        if (i > 0) w.writeByte(',') catch {};
        std.fmt.format(w, "\"{s}\"", .{name}) catch {};
    }
    srv.recordQueryCost(tenant_id, "list_collections", cols.items.len, 0, start_ns);
    w.writeAll("]}") catch {};
    return ok(getBodyBuf()[0..fbs.pos]);
}

fn handleBillingLog(srv: *Server) usize {
    var fbs = std.io.fixedBufferStream(getBodyBuf());
    const w = fbs.writer();
    w.writeAll("{\"queries\":[") catch {};
    srv.billing_mu.lock();
    defer srv.billing_mu.unlock();
    // Read from ring buffer: emit up to 100 most recent entries.
    const count = srv.billing_ring_count;
    const emit = @min(count, 100);
    const cap = Server.BILLING_LOG_CAP;
    // Start index: oldest of the last `emit` entries in the ring.
    const start_idx = if (count >= emit) (srv.billing_ring_head + cap - emit) % cap else 0;
    for (0..emit) |i| {
        const idx = (start_idx + i) % cap;
        const entry = srv.billing_ring[idx];
        if (i > 0) w.writeByte(',') catch {};
        std.fmt.format(w,
            "{{\"tenant\":\"{s}\",\"op\":\"{s}\",\"rows_scanned\":{d},\"bytes_read\":{d},\"cpu_us\":{d},\"cost_nanos_usd\":{d}}}",
            .{
                entry.tenant_id[0..entry.tenant_id_len],
                entry.op[0..entry.op_len],
                entry.rows_scanned,
                entry.bytes_read,
                entry.cpu_us,
                entry.cost_nanos_usd,
            },
        ) catch {};
    }
    w.writeAll("]}") catch {};
    return ok(getBodyBuf()[0..fbs.pos]);
}

fn handleResourceState(srv: *Server) usize {
    var fbs = std.io.fixedBufferStream(getBodyBuf());
    std.fmt.format(
        fbs.writer(),
        "{{\"state\":\"{s}\",\"queries_per_second\":{d},\"last_query_ms\":{d}}}",
        .{ resourceStateName(srv.activity.state()), srv.activity.queriesPerSecond(), srv.activity.lastQueryMs() },
    ) catch {};
    return ok(getBodyBuf()[0..fbs.pos]);
}

fn handleWebhookRegistration(srv: *Server, body: []const u8) usize {
    const tenant = jsonStr(body, "tenant") orelse return err(400, "missing tenant");
    const webhook = jsonStr(body, "webhook_url") orelse return err(400, "missing webhook_url");
    const secret = jsonStr(body, "secret") orelse return err(400, "missing secret");
    const collection_name = jsonStr(body, "collection") orelse "";
    const id = srv.db.registerWebhook(tenant, collection_name, webhook, secret) catch return err(500, "register webhook failed");
    var fbs = std.io.fixedBufferStream(getBodyBuf());
    std.fmt.format(fbs.writer(), "{{\"subscription_id\":{d}}}", .{id}) catch {};
    return ok(getBodyBuf()[0..fbs.pos]);
}

fn handleCdcEvents(srv: *Server, tenant_filter: ?[]const u8, alloc: std.mem.Allocator) usize {
    const deliveries = srv.db.listWebhookDeliveries(alloc, tenant_filter) catch return err(500, "cdc read failed");
    defer alloc.free(deliveries);
    var fbs = std.io.fixedBufferStream(getBodyBuf());
    const w = fbs.writer();
    w.writeAll("{\"events\":[") catch {};
    for (deliveries, 0..) |entry, i| {
        if (i > 0) w.writeByte(',') catch {};
        std.fmt.format(w,
            "{{\"seq\":{d},\"tenant\":\"{s}\",\"collection\":\"{s}\",\"webhook_url\":\"{s}\",\"signature\":\"{s}\",\"payload\":{s}}}",
            .{
                entry.seq,
                entry.tenant(),
                entry.collectionName(),
                entry.webhook(),
                &entry.signature_hex,
                entry.payloadSlice(),
            },
        ) catch {};
    }
    w.writeAll("]}") catch {};
    return ok(getBodyBuf()[0..fbs.pos]);
}

// ─── Branch handlers ─────────────────────────────────────────────────────

fn handleCreateBranch(srv: *Server, tenant_id: []const u8, col_name: []const u8, body: []const u8) usize {
    srv.db.recordTenantOperation(tenant_id) catch return err(429, "tenant ops quota exceeded");
    const col = srv.db.collectionForTenant(tenant_id, col_name) catch return err(500, "open collection failed");
    const name = jsonStr(body, "name") orelse return err(400, "missing name");
    const agent = jsonStr(body, "agent_id") orelse "default";
    _ = col.createBranch(name, agent) catch return err(500, "create branch failed");
    return ok("{\"created\":true}");
}

fn handleBranchWrite(srv: *Server, tenant_id: []const u8, col_name: []const u8, branch_name: []const u8, key: []const u8, body: []const u8) usize {
    srv.db.recordTenantOperation(tenant_id) catch return err(429, "tenant ops quota exceeded");
    const col = srv.db.collectionForTenant(tenant_id, col_name) catch return err(500, "open collection failed");
    const br = col.getBranch(branch_name) orelse return err(404, "branch not found");
    col.writeOnBranch(br, key, body) catch return err(500, "branch write failed");
    return ok("{\"written\":true}");
}

fn handleBranchRead(srv: *Server, tenant_id: []const u8, col_name: []const u8, branch_name: []const u8, key: []const u8) usize {
    srv.db.recordTenantOperation(tenant_id) catch return err(429, "tenant ops quota exceeded");
    const col = srv.db.collectionForTenant(tenant_id, col_name) catch return err(500, "open collection failed");
    const br = col.getBranch(branch_name) orelse return err(404, "branch not found");
    const val = col.getOnBranch(br, key) orelse return err(404, "not found");
    // Write response with raw value
    var fbs = std.io.fixedBufferStream(getBodyBuf());
    std.fmt.format(fbs.writer(), "{{\"key\":\"{s}\",\"value\":{s}}}", .{
        key, if (val.len > 0) val else "{}",
    }) catch {};
    return ok(getBodyBuf()[0..fbs.pos]);
}

fn handleBranchMerge(srv: *Server, tenant_id: []const u8, col_name: []const u8, branch_name: []const u8, alloc: std.mem.Allocator) usize {
    srv.db.recordTenantOperation(tenant_id) catch return err(429, "tenant ops quota exceeded");
    const col = srv.db.collectionForTenant(tenant_id, col_name) catch return err(500, "open collection failed");
    const br = col.getBranch(branch_name) orelse return err(404, "branch not found");
    var result = col.mergeBranch(br, alloc) catch return err(500, "merge failed");
    defer result.deinit();
    if (result.conflicts.len > 0) {
        // Return conflict count
        var fbs = std.io.fixedBufferStream(getBodyBuf());
        std.fmt.format(fbs.writer(), "{{\"merged\":false,\"conflicts\":{d}}}", .{result.conflicts.len}) catch {};
        return respond(409, "Conflict", getBodyBuf()[0..fbs.pos]);
    }
    var fbs = std.io.fixedBufferStream(getBodyBuf());
    std.fmt.format(fbs.writer(), "{{\"merged\":true,\"applied\":{d}}}", .{result.applied}) catch {};
    return ok(getBodyBuf()[0..fbs.pos]);
}

fn handleBranchSearch(srv: *Server, tenant_id: []const u8, col_name: []const u8, branch_name: []const u8, query_text: []const u8, limit: u32, alloc: std.mem.Allocator) usize {
    srv.db.recordTenantOperation(tenant_id) catch return err(429, "tenant ops quota exceeded");
    const col = srv.db.collectionForTenant(tenant_id, col_name) catch return err(500, "open collection failed");
    const br = col.getBranch(branch_name) orelse return err(404, "branch not found");
    const result = col.searchOnBranch(br, query_text, limit, alloc) catch return err(500, "branch search failed");
    defer result.deinit();

    var fbs = std.io.fixedBufferStream(getBodyBuf());
    const w = fbs.writer();
    std.fmt.format(w, "{{\"branch\":\"{s}\",\"hits\":{d},\"results\":[", .{ branch_name, result.docs.len }) catch {};
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

fn handleListBranches(srv: *Server, tenant_id: []const u8, col_name: []const u8, alloc: std.mem.Allocator) usize {
    srv.db.recordTenantOperation(tenant_id) catch return err(429, "tenant ops quota exceeded");
    const col = srv.db.collectionForTenant(tenant_id, col_name) catch return err(500, "open collection failed");
    const names = col.listBranches(alloc) catch return err(500, "list branches failed");
    defer alloc.free(names);

    var fbs = std.io.fixedBufferStream(getBodyBuf());
    const w = fbs.writer();
    w.writeAll("{\"branches\":[") catch {};
    for (names, 0..) |name, i| {
        if (i > 0) w.writeByte(',') catch {};
        std.fmt.format(w, "\"{s}\"", .{name}) catch {};
    }
    w.writeAll("]}") catch {};
    return ok(getBodyBuf()[0..fbs.pos]);
}

fn requestTenant(raw: []const u8, query: []const u8) []const u8 {
    return header(raw, "X-Tenant-Id: ") orelse qparam(query, "tenant") orelse collection.DEFAULT_TENANT;
}

fn requestAsOf(raw: []const u8, query: []const u8) ?i64 {
    const value = header(raw, "X-As-Of: ") orelse qparam(query, "as_of") orelse return null;
    return parseAsOfTimestamp(value);
}

fn header(raw: []const u8, needle: []const u8) ?[]const u8 {
    const pos = std.mem.indexOf(u8, raw, needle) orelse return null;
    const start = pos + needle.len;
    const end = std.mem.indexOfScalarPos(u8, raw, start, '\r') orelse
        std.mem.indexOfScalarPos(u8, raw, start, '\n') orelse raw.len;
    const value = raw[start..end];
    return if (value.len > 0) value else null;
}

fn parseAsOfTimestamp(value: []const u8) ?i64 {
    const parsed = std.fmt.parseInt(i64, value, 10) catch return null;
    if (parsed < 0) return null;
    return if (parsed < 1_000_000_000_000) parsed * 1000 else parsed;
}

fn resourceStateName(state: activity.ResourceState) []const u8 {
    return switch (state) {
        .deep_sleep => "deep_sleep",
        .light_sleep => "light_sleep",
        .warm => "warm",
        .hot => "hot",
    };
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
        401 => "Unauthorized",
        429 => "Too Many Requests",
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

fn extractContentLength(raw: []const u8) usize {
    // Fully case-insensitive search for Content-Length header.
    const headers = raw[0..@min(raw.len, 2048)]; // only scan headers
    var i: usize = 0;
    while (i + 16 < headers.len) : (i += 1) {
        if ((headers[i] == 'C' or headers[i] == 'c') and
            (headers[i + 1] == 'o' or headers[i + 1] == 'O') and
            (headers[i + 2] == 'n' or headers[i + 2] == 'N') and
            (headers[i + 3] == 't' or headers[i + 3] == 'T') and
            (headers[i + 4] == 'e' or headers[i + 4] == 'E') and
            (headers[i + 5] == 'n' or headers[i + 5] == 'N') and
            (headers[i + 6] == 't' or headers[i + 6] == 'T') and
            headers[i + 7] == '-' and
            (headers[i + 8] == 'L' or headers[i + 8] == 'l') and
            (headers[i + 9] == 'e' or headers[i + 9] == 'E') and
            (headers[i + 10] == 'n' or headers[i + 10] == 'N') and
            (headers[i + 11] == 'g' or headers[i + 11] == 'G') and
            (headers[i + 12] == 't' or headers[i + 12] == 'T') and
            (headers[i + 13] == 'h' or headers[i + 13] == 'H') and
            headers[i + 14] == ':')
        {
            var start = i + 15;
            while (start < headers.len and (headers[start] == ' ' or headers[start] == '\t')) start += 1;
            var end = start;
            while (end < headers.len and headers[end] >= '0' and headers[end] <= '9') : (end += 1) {}
            if (end > start) {
                return std.fmt.parseInt(usize, headers[start..end], 10) catch 0;
            }
        }
    }
    return 0;
}

fn jsonStr(json: []const u8, key: []const u8) ?[]const u8 {
    var kbuf: [64]u8 = undefined;
    const needle = std.fmt.bufPrint(&kbuf, "\"{s}\":", .{key}) catch return null;
    const pos = std.mem.indexOf(u8, json, needle) orelse return null;
    var start = pos + needle.len;
    // Skip optional whitespace after colon (e.g. "key": "value")
    while (start < json.len and (json[start] == ' ' or json[start] == '\t')) start += 1;
    if (start >= json.len or json[start] != '"') return null;
    start += 1; // skip opening quote
    const end = std.mem.indexOfScalarPos(u8, json, start, '"') orelse return null;
    return json[start..end];
}

// Extract a JSON value (string, object, array, number, bool, null) for the given key.
// Returns the raw slice: "hello" for strings (without quotes), {"a":1} for objects, etc.
fn jsonValue(json: []const u8, key: []const u8) ?[]const u8 {
    var kbuf: [64]u8 = undefined;
    const needle = std.fmt.bufPrint(&kbuf, "\"{s}\":", .{key}) catch return null;
    const pos = std.mem.indexOf(u8, json, needle) orelse return null;
    var start = pos + needle.len;
    // Skip whitespace
    while (start < json.len and (json[start] == ' ' or json[start] == '\t')) start += 1;
    if (start >= json.len) return null;

    const ch = json[start];
    if (ch == '"') {
        // String value — return the raw JSON token including quotes so it stays
        // valid when embedded in a JSON response via {s}.  A stringified JSON
        // string like "value":"{\"a\":1}" is stored as "{\"a\":1}" (quotes + escapes)
        // and re-emitted verbatim by handleGet.
        var i = start + 1;
        while (i < json.len) : (i += 1) {
            if (json[i] == '\\' and i + 1 < json.len) { i += 1; continue; }
            if (json[i] == '"') return json[start .. i + 1]; // include both quotes
        }
        return null; // unterminated string — don't read past buffer
    } else if (ch == '{' or ch == '[') {
        // Object or array — find matching close bracket
        const close: u8 = if (ch == '{') '}' else ']';
        var depth: u32 = 1;
        var i = start + 1;
        var in_str = false;
        while (i < json.len and depth > 0) : (i += 1) {
            if (json[i] == '\\' and in_str) { i += 1; continue; }
            if (json[i] == '"') { in_str = !in_str; continue; }
            if (in_str) continue;
            if (json[i] == ch) depth += 1;
            if (json[i] == close) depth -= 1;
        }
        return json[start..i];
    } else {
        // Number, bool, null — read until delimiter
        var i = start;
        while (i < json.len and json[i] != ',' and json[i] != '}' and json[i] != ']' and json[i] != ' ' and json[i] != '\n') : (i += 1) {}
        return json[start..i];
    }
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

/// Decode percent-encoded URL string (%20 → space, + → space, %XX → byte).
/// Returns decoded slice within `buf`, or null if buffer too small.
fn urlDecode(encoded: []const u8, buf: []u8) ?[]const u8 {
    var i: usize = 0;
    var o: usize = 0;
    while (i < encoded.len) {
        if (o >= buf.len) return null;
        if (encoded[i] == '+') {
            buf[o] = ' ';
            i += 1;
        } else if (encoded[i] == '%' and i + 2 < encoded.len) {
            const hi = hexVal(encoded[i + 1]) orelse {
                buf[o] = encoded[i];
                i += 1;
                o += 1;
                continue;
            };
            const lo = hexVal(encoded[i + 2]) orelse {
                buf[o] = encoded[i];
                i += 1;
                o += 1;
                continue;
            };
            buf[o] = @as(u8, hi) << 4 | @as(u8, lo);
            i += 3;
        } else {
            buf[o] = encoded[i];
            i += 1;
        }
        o += 1;
    }
    return buf[0..o];
}

fn hexVal(c: u8) ?u4 {
    if (c >= '0' and c <= '9') return @intCast(c - '0');
    if (c >= 'a' and c <= 'f') return @intCast(c - 'a' + 10);
    if (c >= 'A' and c <= 'F') return @intCast(c - 'A' + 10);
    return null;
}

// ─── WebSocket ───────────────────────────────────────────────────────────

/// Read exactly `buf.len` bytes from the stream, looping as needed.
fn wsReadExact(conn: std.net.Server.Connection, buf: []u8) !void {
    var filled: usize = 0;
    while (filled < buf.len) {
        const n = conn.stream.read(buf[filled..]) catch return error.ReadFailed;
        if (n == 0) return error.ConnectionClosed;
        filled += n;
    }
}
/// Case-insensitive header value check.
fn headerContains(raw: []const u8, name: []const u8, value: []const u8) bool {
    const headers = raw[0..@min(raw.len, 4096)];
    var i: usize = 0;
    while (i + name.len + 2 < headers.len) : (i += 1) {
        if (headers[i] == '\n') {
            const line_start = i + 1;
            if (line_start + name.len + 2 >= headers.len) continue;
            var match = true;
            for (0..name.len) |j| {
                const a = headers[line_start + j];
                const b = name[j];
                const al = if (a >= 'A' and a <= 'Z') a + 32 else a;
                const bl = if (b >= 'A' and b <= 'Z') b + 32 else b;
                if (al != bl) { match = false; break; }
            }
            if (!match) continue;
            if (headers[line_start + name.len] != ':') continue;
            // Found header — check value (case-insensitive)
            var vs = line_start + name.len + 1;
            while (vs < headers.len and (headers[vs] == ' ' or headers[vs] == '\t')) vs += 1;
            const ve = std.mem.indexOfScalarPos(u8, headers, vs, '\r') orelse
                std.mem.indexOfScalarPos(u8, headers, vs, '\n') orelse headers.len;
            const hval = headers[vs..ve];
            if (hval.len < value.len) continue;
            var vmatch = true;
            for (0..value.len) |j| {
                const a = hval[j];
                const b = value[j];
                const al = if (a >= 'A' and a <= 'Z') a + 32 else a;
                const bl = if (b >= 'A' and b <= 'Z') b + 32 else b;
                if (al != bl) { vmatch = false; break; }
            }
            if (vmatch) return true;
        }
    }
    return false;
}

/// Extract a specific header value from raw HTTP request.
fn headerValue(raw: []const u8, name: []const u8) ?[]const u8 {
    const headers = raw[0..@min(raw.len, 4096)];
    var i: usize = 0;
    while (i + name.len + 2 < headers.len) : (i += 1) {
        if (headers[i] == '\n') {
            const ls = i + 1;
            if (ls + name.len + 2 >= headers.len) continue;
            var match = true;
            for (0..name.len) |j| {
                const a = headers[ls + j];
                const b = name[j];
                const al = if (a >= 'A' and a <= 'Z') a + 32 else a;
                const bl = if (b >= 'A' and b <= 'Z') b + 32 else b;
                if (al != bl) { match = false; break; }
            }
            if (!match) continue;
            if (headers[ls + name.len] != ':') continue;
            var vs = ls + name.len + 1;
            while (vs < headers.len and (headers[vs] == ' ' or headers[vs] == '\t')) vs += 1;
            const ve = std.mem.indexOfScalarPos(u8, headers, vs, '\r') orelse
                std.mem.indexOfScalarPos(u8, headers, vs, '\n') orelse headers.len;
            return headers[vs..ve];
        }
    }
    return null;
}

const WS_MAGIC = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";

/// Perform WebSocket upgrade handshake, then loop reading WS frames.
/// Each text message is a JSON request dispatched through the normal
/// HTTP handler; the response body is sent back as a WS text frame.
fn handleWebSocket(srv: *Server, conn: std.net.Server.Connection, initial: []const u8) !void {
    // Extract Sec-WebSocket-Key
    const ws_key = headerValue(initial, "Sec-WebSocket-Key") orelse return error.MissingKey;

    // Compute accept hash: SHA1(key ++ magic), base64-encoded
    var sha = std.crypto.hash.Sha1.init(.{});
    sha.update(ws_key);
    sha.update(WS_MAGIC);
    const digest = sha.finalResult();
    var accept_buf: [28]u8 = undefined;
    const accept = std.base64.standard.Encoder.encode(&accept_buf, &digest);

    // Send upgrade response
    var resp_buf: [256]u8 = undefined;
    const resp_len = std.fmt.bufPrint(&resp_buf,
        "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: {s}\r\n\r\n",
        .{accept}) catch return error.FormatFailed;
    conn.stream.writeAll(resp_len) catch return error.WriteFailed;

    // WebSocket frame loop — heap-allocate frame buffer (too large for stack).
    const frame_buf = std.heap.page_allocator.alloc(u8, MAX_BULK) catch return error.OutOfMemory;
    defer std.heap.page_allocator.free(frame_buf);
    while (true) {
        // Read frame header (2 bytes minimum)
        var hdr: [14]u8 = undefined;
        wsReadExact(conn, hdr[0..2]) catch return;
        const fin = hdr[0] & 0x80 != 0;
        const opcode = hdr[0] & 0x0F;
        const masked = hdr[1] & 0x80 != 0;
        var payload_len: u64 = hdr[1] & 0x7F;

        if (opcode == 0x8) return; // close frame
        if (opcode == 0x9) { // ping → pong
            hdr[0] = 0x8A; // FIN + pong
            conn.stream.writeAll(hdr[0..2]) catch return;
            continue;
        }

        // Extended payload length
        if (payload_len == 126) {
            wsReadExact(conn, hdr[2..4]) catch return;
            payload_len = std.mem.readInt(u16, hdr[2..4], .big);
        } else if (payload_len == 127) {
            wsReadExact(conn, hdr[2..10]) catch return;
            payload_len = std.mem.readInt(u64, hdr[2..10], .big);
        }

        if (payload_len > MAX_BULK) {
            wsWriteClose(conn, 1009); // message too big
            return;
        }

        // Read mask key (4 bytes if masked)
        var mask: [4]u8 = .{0, 0, 0, 0};
        if (masked) wsReadExact(conn, &mask) catch return;

        // Read payload
        const plen: usize = @intCast(payload_len);
        const payload = frame_buf[0..plen];
        wsReadExact(conn, payload) catch return;

        // Unmask
        if (masked) {
            for (payload, 0..) |*b, j| b.* ^= mask[j % 4];
        }

        _ = fin; // TODO: handle fragmented messages

        if (opcode == 0x1) { // text frame — dispatch as request
            _ = srv.req_count.fetchAdd(1, .monotonic);

            // Build a synthetic HTTP request from the WS message.
            // Expected format: {"op":"insert","col":"name","key":"k","value":{...}}
            // Or direct HTTP path: "POST /db/mycol\n{...body...}"
            const resp_body = wsDispatch(srv, payload);
            wsWriteText(conn, resp_body) catch return;
        }
    }
}

/// Dispatch a WebSocket text message. Supports two formats:
///   1. Raw HTTP: "METHOD /path\n{body}" — reuses dispatch()
///   2. JSON ops: {"op":"insert","col":"c","key":"k","value":{}} (future)
fn wsDispatch(srv: *Server, msg: []const u8) []const u8 {
    // Wrap as a minimal HTTP request so we can reuse dispatch().
    // Find first \n to split method+path from body.
    const nl = std.mem.indexOfScalar(u8, msg, '\n') orelse msg.len;
    const req_line = msg[0..nl];
    const body = if (nl < msg.len) msg[nl + 1 ..] else "";

    // Build HTTP/1.1 request with Content-Length.
    // Heap-allocate since WS messages can be up to 16MB.
    const needed = req_line.len + 64 + body.len;
    const http_buf = std.heap.page_allocator.alloc(u8, needed) catch
        return "{\"error\":\"request too large\"}";
    defer std.heap.page_allocator.free(http_buf);
    const http_len = std.fmt.bufPrint(http_buf,
        "{s} HTTP/1.1\r\nContent-Length: {d}\r\n\r\n{s}",
        .{ req_line, body.len, body }) catch return "{\"error\":\"request too large\"}";

    const resp_len = dispatch(srv, http_len, std.heap.page_allocator);
    const resp = getRespBuf()[0..resp_len];

    // Strip HTTP headers from response, return just the body
    if (std.mem.indexOf(u8, resp, "\r\n\r\n")) |p| return resp[p + 4 ..];
    return resp;
}

fn wsWriteText(conn: std.net.Server.Connection, payload: []const u8) !void {
    var hdr: [10]u8 = undefined;
    hdr[0] = 0x81; // FIN + text
    var hdr_len: usize = 2;
    if (payload.len < 126) {
        hdr[1] = @intCast(payload.len);
    } else if (payload.len < 65536) {
        hdr[1] = 126;
        std.mem.writeInt(u16, hdr[2..4], @intCast(payload.len), .big);
        hdr_len = 4;
    } else {
        hdr[1] = 127;
        std.mem.writeInt(u64, hdr[2..10], @intCast(payload.len), .big);
        hdr_len = 10;
    }
    try conn.stream.writeAll(hdr[0..hdr_len]);
    try conn.stream.writeAll(payload);
}

fn wsWriteClose(conn: std.net.Server.Connection, code: u16) void {
    var frame: [4]u8 = undefined;
    frame[0] = 0x88; // FIN + close
    frame[1] = 2;    // payload = 2 bytes (status code)
    std.mem.writeInt(u16, frame[2..4], code, .big);
    conn.stream.writeAll(&frame) catch {};
}

test "parse as_of accepts seconds and milliseconds" {
    try std.testing.expectEqual(@as(?i64, 1_700_000_000_000), parseAsOfTimestamp("1700000000"));
    try std.testing.expectEqual(@as(?i64, 1_700_000_000_123), parseAsOfTimestamp("1700000000123"));
    try std.testing.expectEqual(@as(?i64, null), parseAsOfTimestamp("not-a-timestamp"));
}

test "request tenant prefers header over query" {
    const raw = "GET /db/users?tenant=query-tenant HTTP/1.1\r\nX-Tenant-Id: header-tenant\r\n\r\n";
    try std.testing.expectEqualStrings("header-tenant", requestTenant(raw, "tenant=query-tenant"));
}
