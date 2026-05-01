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
const compat = @import("compat");
const runtime = @import("runtime");
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
    billing_ring: [1024]QueryCost = undefined,
    billing_head: u32 = 0, // next write slot (mod 1024)
    billing_len: u32 = 0,  // items stored, capped at 1024
    billing_mu: std.Io.Mutex,
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
            .billing_mu = .init,
            .activity = activity.ActivityTracker.init(),
            .active_conns = std.atomic.Value(u32).init(0),
        };
    }

    pub fn run(self: *Server) !void {
        const addr = try std.Io.net.IpAddress.parse("0.0.0.0", self.port);
        var listener = try std.Io.net.IpAddress.listen(&addr, runtime.io, .{
            .reuse_address = true,
            .kernel_backlog = 256,
        });
        defer listener.deinit(runtime.io);

        self.running.store(true, .release);
        std.log.info("TurboDB listening on :{d}", .{self.port});

        while (self.running.load(.acquire)) {
            const stream = listener.accept(runtime.io) catch |e| {
                std.log.err("accept: {}", .{e});
                continue;
            };
            // Reject when at connection cap — bounds thread/FD use under flood.
            if (self.active_conns.load(.monotonic) >= MAX_CONNECTIONS) {
                stream.close(runtime.io);
                _ = self.err_count.fetchAdd(1, .monotonic);
                continue;
            }
            const t = std.Thread.spawn(.{}, handleConnWrapped, .{self, stream}) catch |e| {
                std.log.err("thread spawn: {} (dropping connection)", .{e});
                stream.close(runtime.io);
                _ = self.err_count.fetchAdd(1, .monotonic);
                continue;
            };
            t.detach();
        }
    }

    pub fn runUnix(self: *Server, path: []const u8) !void {
        // TODO(0.16): Unix socket support removed during Zig 0.16 migration.
        // Reimplement on std.Io.net.UnixAddress when time permits.
        _ = self;
        _ = path;
        return error.Unimplemented;
    }

    pub fn stop(self: *Server) void {
        self.running.store(false, .release);
    }

    fn recordQueryCost(self: *Server, tenant_id: []const u8, op: []const u8, rows_scanned: usize, bytes_read: usize, start_ns: i128) void {
        const elapsed_ns = compat.nanoTimestamp() - start_ns;
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

        self.billing_mu.lockUncancelable(runtime.io);
        defer self.billing_mu.unlock(runtime.io);
        self.billing_ring[self.billing_head] = entry;
        self.billing_head = (self.billing_head + 1) % 1024;
        if (self.billing_len < 1024) self.billing_len += 1;
    }
};

/// Wrapper that tracks active connection count around handleConn.
fn handleConnWrapped(srv: *Server, stream: std.Io.net.Stream) void {
    _ = srv.active_conns.fetchAdd(1, .monotonic);
    defer _ = srv.active_conns.fetchSub(1, .monotonic);
    handleConn(srv, stream);
}

fn handleConn(srv: *Server, stream: std.Io.net.Stream) void {
    defer stream.close(runtime.io);

    const bufs = std.heap.page_allocator.create(ConnBufs) catch {
        return;
    };
    defer std.heap.page_allocator.destroy(bufs);
    tl_bufs = bufs;
    defer tl_bufs = null;

    while (true) {
        var n = compat.streamRead(stream, &bufs.req) catch return;
        if (n == 0) return;
        _ = srv.req_count.fetchAdd(1, .monotonic);

        // For bulk inserts: read the full body based on Content-Length.
        // The initial read may only contain part of a large body.
        const initial = bufs.req[0..n];
        const content_length = extractContentLength(initial);
        if (content_length > MAX_REQ and content_length <= MAX_BULK) {
            // Check this is actually a bulk request before allocating
            const is_bulk = std.mem.indexOf(u8, initial[0..@min(n, 256)], "/bulk") != null;
            if (is_bulk) {
                // Find where headers end
                const header_end = if (std.mem.indexOf(u8, initial, "\r\n\r\n")) |p| p + 4
                    else if (std.mem.indexOf(u8, initial, "\n\n")) |p| p + 2
                    else n;
                const total_size = header_end + content_length;
                if (total_size <= MAX_BULK) {
                    const big_buf = std.heap.page_allocator.alloc(u8, total_size) catch {
                        const resp_len = dispatch(srv, initial, std.heap.page_allocator);
                        compat.streamWriteAll(stream, bufs.resp[0..resp_len]) catch return;
                        continue;
                    };
                    defer std.heap.page_allocator.free(big_buf);
                    @memcpy(big_buf[0..n], initial);
                    // Read remaining bytes
                    while (n < total_size) {
                        const r = compat.streamRead(stream, big_buf[n..total_size]) catch break;
                        if (r == 0) break;
                        n += r;
                    }
                    const resp_len = dispatch(srv, big_buf[0..n], std.heap.page_allocator);
                    compat.streamWriteAll(stream, bufs.resp[0..resp_len]) catch return;
                    continue;
                }
            }
        }

        const resp_len = dispatch(srv, initial, std.heap.page_allocator);
        compat.streamWriteAll(stream, bufs.resp[0..resp_len]) catch return;
    }
}
fn dispatch(srv: *Server, raw: []const u8, alloc: std.mem.Allocator) usize {
    // Parse request line.
    const nl = std.mem.indexOfScalar(u8, raw, '\n') orelse return err(400, "bad request");
    const req_line = std.mem.trimEnd(u8, raw[0..nl], "\r");
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
        var w = std.Io.Writer.fixed(getBodyBuf());
        w.print(
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
        return ok(getBodyBuf()[0..w.end]);
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
        const limit_val: u32 = qparamInt(query, "limit") orelse 50;
        return handleSearch(srv, requestTenant(raw, query), col_name, q, limit_val, alloc);
    }

    // Route: /context/:col?q=...&limit=20
    if (std.mem.startsWith(u8, path, "/context/") and std.mem.eql(u8, method, "GET")) {
        const col_name = path[9..];
        const q_raw = qparam(query, "q") orelse return err(400, "missing q parameter");
        var decode_buf: [4096]u8 = undefined;
        const q = urlDecode(q_raw, &decode_buf) orelse q_raw;
        const limit_val: u32 = qparamInt(query, "limit") orelse 20;
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
        const k = std.fmt.bufPrint(&kb, "doc_{d}", .{compat.milliTimestamp()}) catch
            return err(400, "bad key");
        return doInsert(srv, tenant_id, col_name, k, value);
    };
    return doInsert(srv, tenant_id, col_name, key_raw, value);
}

fn doInsert(srv: *Server, tenant_id: []const u8, col_name: []const u8, key: []const u8, value: []const u8) usize {
    const start_ns = compat.nanoTimestamp();
    srv.db.recordTenantOperation(tenant_id) catch return err(429, "tenant ops quota exceeded");
    srv.db.ensureTenantStorageAvailable(tenant_id, value.len) catch return err(429, "tenant storage quota exceeded");
    const col = srv.db.collectionForTenant(tenant_id, col_name) catch return err(500, "open collection failed");
    const doc_id = col.insert(key, value) catch return err(500, "insert failed");
    srv.recordQueryCost(tenant_id, "insert", 1, value.len, start_ns);
    var w = std.Io.Writer.fixed(getBodyBuf());
    w.print(
        "{{\"doc_id\":{d},\"key\":\"{s}\",\"collection\":\"{s}\",\"tenant\":\"{s}\"}}",
        .{ doc_id, key, col_name, tenant_id }) catch {};
    return ok(getBodyBuf()[0..w.end]);
}

/// POST /db/:col/bulk — insert multiple documents in one request.
/// Body: NDJSON — one {"key":"...","value":"..."} per line.
/// Response: {"inserted":N,"errors":M,"collection":"...","tenant":"..."}
fn handleBulkInsert(srv: *Server, tenant_id: []const u8, col_name: []const u8, body: []const u8, alloc: std.mem.Allocator) usize {
    const start_ns = compat.nanoTimestamp();
    srv.db.recordTenantOperation(tenant_id) catch return err(429, "tenant ops quota exceeded");
    const col = srv.db.collectionForTenant(tenant_id, col_name) catch return err(500, "open collection failed");

    var inserted: u32 = 0;
    var errors: u32 = 0;
    var total_bytes: u64 = 0;
    var items: std.ArrayList(collection.Collection.BatchItem) = .empty;
    defer items.deinit(alloc);

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

        items.append(alloc, .{ .key = key, .value = value }) catch {
            errors += 1;
            continue;
        };
        total_bytes += line.len;
    }

    const inserted_count = col.insertBatch(items.items, null) catch return err(500, "bulk insert failed");
    inserted = @intCast(inserted_count);

    srv.recordQueryCost(tenant_id, "bulk_insert", inserted, total_bytes, start_ns);

    var w = std.Io.Writer.fixed(getBodyBuf());
    w.print(
        "{{\"inserted\":{d},\"errors\":{d},\"collection\":\"{s}\",\"tenant\":\"{s}\"}}",
        .{ inserted, errors, col_name, tenant_id }) catch {};
    return ok(getBodyBuf()[0..w.end]);
}
fn handleGet(srv: *Server, tenant_id: []const u8, col_name: []const u8, key: []const u8, as_of: ?i64) usize {
        const start_ns = compat.nanoTimestamp();
        srv.db.recordTenantOperation(tenant_id) catch return err(429, "tenant ops quota exceeded");
        const col = srv.db.collectionForTenant(tenant_id, col_name) catch return err(500, "open collection failed");
        const d = if (as_of) |ts_ms|
            (col.getAsOfTimestamp(key, ts_ms) orelse return err(404, "not found"))
        else
            (col.get(key) orelse return err(404, "not found"));
        srv.recordQueryCost(tenant_id, "get", 1, d.key.len + d.value.len, start_ns);

        // Fixed-length header = 103 bytes ("Content-Length" padded to 10 digits).
        const HEADER_LEN = 103;
        var resp = getRespBuf();

        // Write body directly after the header region. No reserved-prefix
        // memcpy dance — the header is always HEADER_LEN bytes.
        var w = std.Io.Writer.fixed(resp[HEADER_LEN..]);
        w.print(
            "{{\"doc_id\":{d},\"key\":\"{s}\",\"version\":{d},\"value\":",
            .{ d.header.doc_id, d.key, d.header.version }) catch {};
        writeJsonValue(&w, d.value);
        w.writeByte('}') catch {};
        const body_len = w.end;

        // Header with zero-padded Content-Length so length is deterministic.
        var hdr_w = std.Io.Writer.fixed(resp[0..HEADER_LEN]);
        hdr_w.print(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {d:0>10}\r\nConnection: keep-alive\r\n\r\n",
            .{body_len}) catch {};
        // hdr_w.end == HEADER_LEN by construction.
        return HEADER_LEN + body_len;
    }

fn handleUpdate(srv: *Server, tenant_id: []const u8, col_name: []const u8, key: []const u8, body: []const u8, alloc: std.mem.Allocator) usize {
    const start_ns = compat.nanoTimestamp();
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
    const start_ns = compat.nanoTimestamp();
    srv.db.recordTenantOperation(tenant_id) catch return err(429, "tenant ops quota exceeded");
    const col = srv.db.collectionForTenant(tenant_id, col_name) catch return err(500, "open collection failed");
    const deleted = col.delete(key) catch return err(500, "delete failed");
    if (!deleted) return err(404, "not found");
    srv.recordQueryCost(tenant_id, "delete", 1, 0, start_ns);
    return ok("{\"deleted\":true}");
}

fn handleScan(srv: *Server, tenant_id: []const u8, col_name: []const u8, query_str: []const u8, as_of: ?i64, alloc: std.mem.Allocator) usize {
    const start_ns = compat.nanoTimestamp();
    const limit: u32 = qparamInt(query_str, "limit") orelse 20;
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

    var w = std.Io.Writer.fixed(getBodyBuf());
    w.print("{{\"tenant\":\"{s}\",\"collection\":\"{s}\",\"count\":{d},\"docs\":[",
        .{ tenant_id, col_name, result.docs.len }) catch {};
    for (result.docs, 0..) |d, i| {
        if (i > 0) w.writeByte(',') catch {};
        w.print("{{\"doc_id\":{d},\"key\":\"{s}\",\"version\":{d},\"value\":",
            .{ d.header.doc_id, d.key, d.header.version }) catch {};
        writeJsonValue(&w, d.value);
        w.writeByte('}') catch {};
    }
    w.writeAll("]}") catch {};
    return ok(getBodyBuf()[0..w.end]);
}

fn handleDrop(srv: *Server, tenant_id: []const u8, col_name: []const u8) usize {
    const start_ns = compat.nanoTimestamp();
    srv.db.recordTenantOperation(tenant_id) catch return err(429, "tenant ops quota exceeded");
    srv.db.dropCollectionForTenant(tenant_id, col_name);
    srv.recordQueryCost(tenant_id, "drop", 0, 0, start_ns);
    return ok("{\"dropped\":true}");
}

fn handleSearch(srv: *Server, tenant_id: []const u8, col_name: []const u8, query: []const u8, limit: u32, alloc: std.mem.Allocator) usize {
    const start_ns = compat.nanoTimestamp();
    srv.db.recordTenantOperation(tenant_id) catch return err(429, "tenant ops quota exceeded");
    const col = srv.db.collectionForTenant(tenant_id, col_name) catch return err(500, "open collection failed");
    const result = col.searchText(query, limit, alloc) catch return err(500, "search failed");
    defer result.deinit();
    var bytes_read: u64 = 0;
    for (result.docs) |d| bytes_read += d.key.len + d.value.len;
    srv.recordQueryCost(tenant_id, "search", result.docs.len, bytes_read, start_ns);

    var w = std.Io.Writer.fixed(getBodyBuf());
    // Write JSON header with escaped query string
    w.writeAll("{\"query\":\"") catch {};
    for (query) |ch| {
        if (ch == '"' or ch == '\\') { w.writeByte('\\') catch {}; }
        w.writeByte(ch) catch {};
    }
    w.print("\",\"hits\":{d},\"candidates\":{d},\"total_docs\":{d},\"total_files\":{d},\"results\":[",
        .{ result.docs.len, result.candidate_paths.len, col.docCount(), result.total_files }) catch {};
    for (result.docs, 0..) |d, i| {
        if (i > 0) w.writeByte(',') catch {};
        w.print("{{\"doc_id\":{d},\"key\":\"{s}\",\"value\":",
            .{ d.header.doc_id, d.key }) catch {};
        writeJsonValue(&w, d.value);
        w.writeByte('}') catch {};
    }
    w.writeAll("]}") catch {};
    return ok(getBodyBuf()[0..w.end]);
}

fn handleDiscoverContext(srv: *Server, tenant_id: []const u8, col_name: []const u8, query: []const u8, limit: u32, alloc: std.mem.Allocator) usize {
    const start_ns = compat.nanoTimestamp();
    srv.db.recordTenantOperation(tenant_id) catch return err(429, "tenant ops quota exceeded");
    const col = srv.db.collectionForTenant(tenant_id, col_name) catch return err(500, "open collection failed");
    var result = col.discoverContext(query, limit, alloc) catch return err(500, "context discovery failed");
    defer result.deinit();
    var bytes_read: u64 = 0;
    for (result.matching_files) |d| bytes_read += d.key.len + d.value.len;
    srv.recordQueryCost(tenant_id, "context", result.matching_files.len, bytes_read, start_ns);

    var w = std.Io.Writer.fixed(getBodyBuf());

    // Write JSON: matching_files
    w.writeAll("{\"query\":\"") catch {};
    for (query) |ch| {
        if (ch == '"' or ch == '\\') { w.writeByte('\\') catch {}; }
        w.writeByte(ch) catch {};
    }
    w.writeAll("\",\"matching_files\":[") catch {};
    for (result.matching_files, 0..) |d, i| {
        if (i > 0) w.writeByte(',') catch {};
        w.print("{{\"key\":\"{s}\",\"size\":{d}}}", .{ d.key, d.value.len }) catch {};
    }
    w.writeAll("],\"related_files\":[") catch {};
    for (result.related_files, 0..) |d, i| {
        if (i > 0) w.writeByte(',') catch {};
        w.print("{{\"key\":\"{s}\"}}", .{d.key}) catch {};
    }
    w.writeAll("],\"test_files\":[") catch {};
    for (result.test_files, 0..) |d, i| {
        if (i > 0) w.writeByte(',') catch {};
        w.print("{{\"key\":\"{s}\"}}", .{d.key}) catch {};
    }
    w.print("],\"recent_versions\":{d},\"total_files\":{d}}}", .{ result.recent_versions, result.total_files }) catch {};
    return ok(getBodyBuf()[0..w.end]);
}

fn handleListCollections(srv: *Server, tenant_id: []const u8, alloc: std.mem.Allocator) usize {
    const start_ns = compat.nanoTimestamp();
    srv.db.recordTenantOperation(tenant_id) catch return err(429, "tenant ops quota exceeded");
    var cols = srv.db.listCollectionsForTenant(tenant_id, alloc) catch return err(500, "list collections failed");
    defer {
        for (cols.items) |name| alloc.free(name);
        cols.deinit(alloc);
    }
    var w = std.Io.Writer.fixed(getBodyBuf());
    w.print("{{\"tenant\":\"{s}\",\"collections\":[", .{tenant_id}) catch {};
    for (cols.items, 0..) |name, i| {
        if (i > 0) w.writeByte(',') catch {};
        w.print("\"{s}\"", .{name}) catch {};
    }
    srv.recordQueryCost(tenant_id, "list_collections", cols.items.len, 0, start_ns);
    w.writeAll("]}") catch {};
    return ok(getBodyBuf()[0..w.end]);
}

fn handleBillingLog(srv: *Server) usize {
    var w = std.Io.Writer.fixed(getBodyBuf());
    w.writeAll("{\"queries\":[") catch {};
    srv.billing_mu.lockUncancelable(runtime.io);
    defer srv.billing_mu.unlock(runtime.io);
    // Iterate ring in logical order: newest entries are the most recent `min(len, 100)`.
    const shown: u32 = @min(srv.billing_len, 100);
    const first_slot: u32 = (srv.billing_head + 1024 - srv.billing_len) % 1024;
    const start_offset: u32 = srv.billing_len - shown;
    var idx: u32 = 0;
    while (idx < shown) : (idx += 1) {
        const slot = (first_slot + start_offset + idx) % 1024;
        const entry = srv.billing_ring[slot];
        const i = idx;
        if (i > 0) w.writeByte(',') catch {};
        w.print("{{\"tenant\":\"{s}\",\"op\":\"{s}\",\"rows_scanned\":{d},\"bytes_read\":{d},\"cpu_us\":{d},\"cost_nanos_usd\":{d}}}",
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
    return ok(getBodyBuf()[0..w.end]);
}

fn handleResourceState(srv: *Server) usize {
    var w = std.Io.Writer.fixed(getBodyBuf());
    w.print(
        "{{\"state\":\"{s}\",\"queries_per_second\":{d},\"last_query_ms\":{d}}}",
        .{ resourceStateName(srv.activity.state()), srv.activity.queriesPerSecond(), srv.activity.lastQueryMs() },
    ) catch {};
    return ok(getBodyBuf()[0..w.end]);
}

fn handleWebhookRegistration(srv: *Server, body: []const u8) usize {
    const tenant = jsonStr(body, "tenant") orelse return err(400, "missing tenant");
    const webhook = jsonStr(body, "webhook_url") orelse return err(400, "missing webhook_url");
    const secret = jsonStr(body, "secret") orelse return err(400, "missing secret");
    const collection_name = jsonStr(body, "collection") orelse "";
    const id = srv.db.registerWebhook(tenant, collection_name, webhook, secret) catch return err(500, "register webhook failed");
    var w = std.Io.Writer.fixed(getBodyBuf());
    w.print("{{\"subscription_id\":{d}}}", .{id}) catch {};
    return ok(getBodyBuf()[0..w.end]);
}

fn handleCdcEvents(srv: *Server, tenant_filter: ?[]const u8, alloc: std.mem.Allocator) usize {
    const deliveries = srv.db.listWebhookDeliveries(alloc, tenant_filter) catch return err(500, "cdc read failed");
    defer alloc.free(deliveries);
    var w = std.Io.Writer.fixed(getBodyBuf());
    w.writeAll("{\"events\":[") catch {};
    for (deliveries, 0..) |entry, i| {
        if (i > 0) w.writeByte(',') catch {};
        w.print("{{\"seq\":{d},\"tenant\":\"{s}\",\"collection\":\"{s}\",\"webhook_url\":\"{s}\",\"signature\":\"{s}\",\"payload\":{s}}}",
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
    return ok(getBodyBuf()[0..w.end]);
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
    var w = std.Io.Writer.fixed(getBodyBuf());
    w.print("{{\"key\":\"{s}\",\"value\":{s}}}", .{
        key, if (val.len > 0) val else "{}",
    }) catch {};
    return ok(getBodyBuf()[0..w.end]);
}

fn handleBranchMerge(srv: *Server, tenant_id: []const u8, col_name: []const u8, branch_name: []const u8, alloc: std.mem.Allocator) usize {
    srv.db.recordTenantOperation(tenant_id) catch return err(429, "tenant ops quota exceeded");
    const col = srv.db.collectionForTenant(tenant_id, col_name) catch return err(500, "open collection failed");
    const br = col.getBranch(branch_name) orelse return err(404, "branch not found");
    var result = col.mergeBranch(br, alloc) catch return err(500, "merge failed");
    defer result.deinit();
    if (result.conflicts.len > 0) {
        // Return conflict count
        var w = std.Io.Writer.fixed(getBodyBuf());
        w.print("{{\"merged\":false,\"conflicts\":{d}}}", .{result.conflicts.len}) catch {};
        return respond(409, "Conflict", getBodyBuf()[0..w.end]);
    }
    var w = std.Io.Writer.fixed(getBodyBuf());
    w.print("{{\"merged\":true,\"applied\":{d}}}", .{result.applied}) catch {};
    return ok(getBodyBuf()[0..w.end]);
}

fn handleBranchSearch(srv: *Server, tenant_id: []const u8, col_name: []const u8, branch_name: []const u8, query_text: []const u8, limit: u32, alloc: std.mem.Allocator) usize {
    srv.db.recordTenantOperation(tenant_id) catch return err(429, "tenant ops quota exceeded");
    const col = srv.db.collectionForTenant(tenant_id, col_name) catch return err(500, "open collection failed");
    const br = col.getBranch(branch_name) orelse return err(404, "branch not found");
    const result = col.searchOnBranch(br, query_text, limit, alloc) catch return err(500, "branch search failed");
    defer result.deinit();

    var w = std.Io.Writer.fixed(getBodyBuf());
    w.print("{{\"branch\":\"{s}\",\"hits\":{d},\"results\":[", .{ branch_name, result.docs.len }) catch {};
    for (result.docs, 0..) |d, i| {
        if (i > 0) w.writeByte(',') catch {};
        w.print("{{\"doc_id\":{d},\"key\":\"{s}\",\"value\":",
            .{ d.header.doc_id, d.key }) catch {};
        writeJsonValue(&w, d.value);
        w.writeByte('}') catch {};
    }
    w.writeAll("]}") catch {};
    return ok(getBodyBuf()[0..w.end]);
}

fn handleListBranches(srv: *Server, tenant_id: []const u8, col_name: []const u8, alloc: std.mem.Allocator) usize {
    srv.db.recordTenantOperation(tenant_id) catch return err(429, "tenant ops quota exceeded");
    const col = srv.db.collectionForTenant(tenant_id, col_name) catch return err(500, "open collection failed");
    const names = col.listBranches(alloc) catch return err(500, "list branches failed");
    defer alloc.free(names);

    var w = std.Io.Writer.fixed(getBodyBuf());
    w.writeAll("{\"branches\":[") catch {};
    for (names, 0..) |name, i| {
        if (i > 0) w.writeByte(',') catch {};
        w.print("\"{s}\"", .{name}) catch {};
    }
    w.writeAll("]}") catch {};
    return ok(getBodyBuf()[0..w.end]);
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

/// Write a document value as a JSON value — pass-through for JSON objects/arrays/strings,
/// quoted + escaped for plain text (with empty → "{}" default).
fn writeJsonValue(w: *std.Io.Writer, val_in: []const u8) void {
    const val = if (val_in.len > 0) val_in else "{}";
    const is_json = val[0] == '{' or val[0] == '[' or val[0] == '"';
    if (is_json) {
        w.writeAll(val) catch {};
        return;
    }
    w.writeByte('"') catch {};
    for (val) |ch| {
        switch (ch) {
            '"', '\\' => { w.writeByte('\\') catch {}; w.writeByte(ch) catch {}; },
            '\n' => w.writeAll("\\n") catch {},
            '\r' => w.writeAll("\\r") catch {},
            '\t' => w.writeAll("\\t") catch {},
            else => w.writeByte(ch) catch {},
        }
    }
    w.writeByte('"') catch {};
}
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
    var w = std.Io.Writer.fixed(getRespBuf());
    w.print(
        "HTTP/1.1 {d} {s}\r\nContent-Type: application/json\r\nContent-Length: {d}\r\nConnection: keep-alive\r\n\r\n{s}",
        .{ code, status, body.len, body }) catch {};
    return w.end;
}

// ─── mini parsers ────────────────────────────────────────────────────────

fn extractContentLength(raw: []const u8) usize {
    // Case-insensitive search for Content-Length header
    const headers = raw[0..@min(raw.len, 2048)]; // only scan headers
    const needle = "ontent-length: "; // skip first char for case insensitivity
    var i: usize = 0;
    while (i + needle.len < headers.len) : (i += 1) {
        if ((headers[i] == 'C' or headers[i] == 'c') and std.mem.eql(u8, headers[i + 1 .. i + 1 + needle.len], needle)) {
            const start = i + 1 + needle.len;
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
            if (json[i] == '"') break;
        }
        return json[start .. i + 1]; // include both quotes
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

test "parse as_of accepts seconds and milliseconds" {
    try std.testing.expectEqual(@as(?i64, 1_700_000_000_000), parseAsOfTimestamp("1700000000"));
    try std.testing.expectEqual(@as(?i64, 1_700_000_000_123), parseAsOfTimestamp("1700000000123"));
    try std.testing.expectEqual(@as(?i64, null), parseAsOfTimestamp("not-a-timestamp"));
}

test "request tenant prefers header over query" {
    const raw = "GET /db/users?tenant=query-tenant HTTP/1.1\r\nX-Tenant-Id: header-tenant\r\n\r\n";
    try std.testing.expectEqualStrings("header-tenant", requestTenant(raw, "tenant=query-tenant"));
}
