/// TurboDB HTTP server — MongoDB-compatible-ish JSON REST API
/// Routes:
///   POST   /db/:col              insert document
///   POST   /db/:col/batch_get    get multiple documents by key
///   POST   /db/:col/batch_update upsert multiple documents by key
///   POST   /db/:col/batch_delete delete multiple documents by key
///   POST   /grpc/:col/BulkInsert gRPC-framed binary bulk insert bridge
///   POST   /grpc/:col/BulkUpdate gRPC-framed binary bulk update/upsert bridge
///   POST   /grpc/:col/BulkDelete gRPC-framed binary bulk delete bridge
///   POST   /db/:edge_col/join    read edge keys and batch-get target docs
///   GET    /db/:col/:key         get document by key
///   PUT    /db/:col/:key         upsert document
///   DELETE /db/:col/:key         delete document
///   GET    /db/:col              scan collection (limit, offset query params)
///   DELETE /db/:col              drop collection
///   GET    /collections          list collections
///   GET    /health               health check
///   GET    /metrics              server metrics
///   GET    /context/:col         smart context discovery (q, limit query params)
const std = @import("std");
const compat = @import("compat");
const activity = @import("activity.zig");
const auth = @import("auth.zig");
const collection = @import("collection.zig");
const doc_mod = @import("doc.zig");
const page_mod = @import("page.zig");
const Database = collection.Database;

const MAX_REQ = 65536; // 64 KiB (initial read)
const MAX_RESP = 131072; // 128 KiB
const MAX_BODY = 65536; // 64 KiB
const MAX_BULK = 64 * 1024 * 1024; // 64 MiB for large bulk inserts
const BULK_INSERT_CHUNK_ROWS: usize = 16384;
const BULK_INSERT_CHUNK_BYTES: usize = 16 * 1024 * 1024;
const DEFAULT_BULK_MEMORY_LIMIT: usize = 256 * 1024 * 1024;
const MAX_AUTO_BULK_MEMORY_LIMIT: usize = 512 * 1024 * 1024;
const BULK_MEMORY_AVAILABLE_FRACTION: usize = 4;
const BULK_TENANT_MEMORY_FRACTION: usize = 2;
const BULK_MEMORY_BASE_OVERHEAD: usize = 1024 * 1024;

// Heap-allocated per-connection buffers (threadlocal pointers set in handleConn).
// This avoids large threadlocal TLS segments that break in Release mode on macOS.
const ConnBufs = struct {
    req: [MAX_REQ]u8,
    resp: [MAX_RESP]u8,
    body: [MAX_BODY]u8,
};

const CONN_THREAD_STACK_SIZE = 1024 * 1024;
threadlocal var tl_bufs: ?*ConnBufs = null;

fn getRespBuf() *[MAX_RESP]u8 {
    return &tl_bufs.?.resp;
}
fn getBodyBuf() *[MAX_BODY]u8 {
    return &tl_bufs.?.body;
}

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

    const BulkTenantUsage = struct {
        bytes: usize = 0,
        requests: u32 = 0,
    };

    pub const BulkMemoryReservation = struct {
        srv: *Server,
        tenant_id: [collection.MAX_TENANT_ID_LEN]u8,
        tenant_id_len: u8,
        bytes: usize,
        active: bool = true,

        pub fn release(self: *BulkMemoryReservation) void {
            if (!self.active) return;
            self.srv.releaseBulkMemory(self.tenant_id[0..self.tenant_id_len], self.bytes);
            self.active = false;
        }
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
    billing_ring_head: usize, // next write position
    billing_ring_count: usize, // entries currently stored
    billing_mu: compat.Mutex,
    activity: activity.ActivityTracker,
    // Connection limiter — prevents unbounded thread spawning under flood.
    active_conns: std.atomic.Value(u32),
    bulk_mu: compat.Mutex,
    bulk_tenant_usage: std.StringHashMap(BulkTenantUsage),
    bulk_inflight_bytes: usize,
    bulk_inflight_requests: u32,
    bulk_rejected: std.atomic.Value(u64),

    pub fn init(alloc: std.mem.Allocator, db: *Database, port: u16) Server {
        return .{
            .db = db,
            .port = port,
            .running = std.atomic.Value(bool).init(false),
            .alloc = alloc,
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
            .bulk_mu = .{},
            .bulk_tenant_usage = std.StringHashMap(BulkTenantUsage).init(alloc),
            .bulk_inflight_bytes = 0,
            .bulk_inflight_requests = 0,
            .bulk_rejected = std.atomic.Value(u64).init(0),
        };
    }

    pub fn deinit(self: *Server) void {
        self.bulk_mu.lock();
        defer self.bulk_mu.unlock();
        var it = self.bulk_tenant_usage.keyIterator();
        while (it.next()) |key| self.alloc.free(key.*);
        self.bulk_tenant_usage.deinit();
    }

    fn acquireBulkMemory(self: *Server, tenant_id: []const u8, estimated_bytes: usize) !BulkMemoryReservation {
        const global_limit = bulkGlobalMemoryLimit();
        const tenant_limit = bulkTenantMemoryLimit(global_limit);
        return self.acquireBulkMemoryWithLimits(tenant_id, estimated_bytes, global_limit, tenant_limit);
    }

    fn acquireBulkMemoryWithLimits(
        self: *Server,
        tenant_id: []const u8,
        estimated_bytes: usize,
        global_limit: usize,
        tenant_limit: usize,
    ) !BulkMemoryReservation {
        if (tenant_id.len == 0 or tenant_id.len > collection.MAX_TENANT_ID_LEN) return error.BulkMemoryLimitExceeded;
        if (estimated_bytes == 0 or estimated_bytes > global_limit or estimated_bytes > tenant_limit) {
            _ = self.bulk_rejected.fetchAdd(1, .monotonic);
            return error.BulkMemoryLimitExceeded;
        }

        self.bulk_mu.lock();
        defer self.bulk_mu.unlock();

        const global_next = std.math.add(usize, self.bulk_inflight_bytes, estimated_bytes) catch {
            _ = self.bulk_rejected.fetchAdd(1, .monotonic);
            return error.BulkMemoryLimitExceeded;
        };
        if (global_next > global_limit) {
            _ = self.bulk_rejected.fetchAdd(1, .monotonic);
            return error.BulkMemoryLimitExceeded;
        }

        const current_tenant_usage = self.bulk_tenant_usage.get(tenant_id) orelse BulkTenantUsage{};
        const tenant_next = std.math.add(usize, current_tenant_usage.bytes, estimated_bytes) catch {
            _ = self.bulk_rejected.fetchAdd(1, .monotonic);
            return error.BulkMemoryLimitExceeded;
        };
        if (tenant_next > tenant_limit) {
            _ = self.bulk_rejected.fetchAdd(1, .monotonic);
            return error.BulkMemoryLimitExceeded;
        }

        if (self.bulk_tenant_usage.getPtr(tenant_id)) |usage| {
            usage.bytes = tenant_next;
            usage.requests += 1;
        } else {
            const owned_key = try self.alloc.dupe(u8, tenant_id);
            errdefer self.alloc.free(owned_key);
            try self.bulk_tenant_usage.put(owned_key, .{
                .bytes = tenant_next,
                .requests = 1,
            });
        }
        self.bulk_inflight_bytes = global_next;
        self.bulk_inflight_requests += 1;

        var tenant_copy = [_]u8{0} ** collection.MAX_TENANT_ID_LEN;
        @memcpy(tenant_copy[0..tenant_id.len], tenant_id);
        return .{
            .srv = self,
            .tenant_id = tenant_copy,
            .tenant_id_len = @intCast(tenant_id.len),
            .bytes = estimated_bytes,
        };
    }

    fn releaseBulkMemory(self: *Server, tenant_id: []const u8, bytes: usize) void {
        self.bulk_mu.lock();
        defer self.bulk_mu.unlock();

        self.bulk_inflight_bytes = if (self.bulk_inflight_bytes >= bytes) self.bulk_inflight_bytes - bytes else 0;
        if (self.bulk_inflight_requests > 0) self.bulk_inflight_requests -= 1;

        if (self.bulk_tenant_usage.getPtr(tenant_id)) |usage| {
            usage.bytes = if (usage.bytes >= bytes) usage.bytes - bytes else 0;
            if (usage.requests > 0) usage.requests -= 1;
            if (usage.bytes == 0 and usage.requests == 0) {
                if (self.bulk_tenant_usage.fetchRemove(tenant_id)) |kv| {
                    self.alloc.free(kv.key);
                }
            }
        }
    }

    pub fn run(self: *Server) !void {
        const addr = try compat.net.Address.parseIp("0.0.0.0", self.port);
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
            const t = std.Thread.spawn(.{ .stack_size = CONN_THREAD_STACK_SIZE }, handleConnWrapped, .{ self, conn }) catch {
                conn.stream.close();
                _ = self.err_count.fetchAdd(1, .monotonic);
                continue;
            };
            t.detach();
        }
    }

    pub fn runUnix(self: *Server, path: []const u8) !void {
        // Remove any existing socket file
        // Remove existing socket
        {
            var zbuf: [256]u8 = undefined;
            @memcpy(zbuf[0..path.len], path);
            zbuf[path.len] = 0;
            _ = std.c.unlink(@ptrCast(&zbuf));
        }
        const fd = std.c.socket(1, 1, 0); // AF_UNIX=1, SOCK_STREAM=1
        if (fd < 0) return error.SocketError;
        defer _ = std.c.close(fd);

        // Construct sockaddr_un (family u16 + path)
        var addr: extern struct { family: u16, path: [104]u8 } = .{ .family = 1, .path = undefined };
        @memset(&addr.path, 0);
        if (path.len >= addr.path.len) return error.PathTooLong;
        @memcpy(addr.path[0..path.len], path);

        if (std.c.bind(fd, @ptrCast(&addr), @sizeOf(@TypeOf(addr))) != 0) return error.BindError;
        if (std.c.listen(fd, 256) != 0) return error.ListenError;

        self.running.store(true, .release);
        std.log.info("TurboDB HTTP on unix:{s}", .{path});

        while (self.running.load(.acquire)) {
            const client_fd = std.c.accept(fd, null, null);
            if (client_fd < 0) continue;
            const stream = compat.net.Stream{ .handle = client_fd };
            const conn = compat.net.Server.Connection{ .stream = stream, .address = compat.net.Address.initUnix(path) catch continue };
            if (self.active_conns.load(.monotonic) >= MAX_CONNECTIONS) {
                const reject = "HTTP/1.1 503 Service Unavailable\r\nContent-Length: 31\r\nConnection: close\r\n\r\n{\"error\":\"too many connections\"}";
                conn.stream.writeAll(reject) catch {};
                conn.stream.close();
                _ = self.err_count.fetchAdd(1, .monotonic);
                continue;
            }
            const t = std.Thread.spawn(.{ .stack_size = CONN_THREAD_STACK_SIZE }, handleConnWrapped, .{ self, conn }) catch {
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

threadlocal var tl_bulk_reservation: ?Server.BulkMemoryReservation = null;

const ParsedRequestTarget = struct {
    method: []const u8,
    path: []const u8,
    query: []const u8,
};

const GrpcTarget = struct {
    col_name: []const u8,
    rpc: []const u8,
};

const BulkPreflight = union(enum) {
    not_bulk,
    reserved: Server.BulkMemoryReservation,
    reject: struct {
        code: u16,
        message: []const u8,
    },
};

fn bulkGlobalMemoryLimit() usize {
    const explicit = compat.envUsize("TURBODB_BULK_MEMORY_LIMIT_BYTES", 0);
    if (explicit > 0) return explicit;
    if (compat.availableMemoryBytes()) |available| {
        return @min(available / BULK_MEMORY_AVAILABLE_FRACTION, MAX_AUTO_BULK_MEMORY_LIMIT);
    }
    return DEFAULT_BULK_MEMORY_LIMIT;
}

fn bulkTenantMemoryLimit(global_limit: usize) usize {
    const explicit = compat.envUsize("TURBODB_BULK_TENANT_MEMORY_LIMIT_BYTES", 0);
    if (explicit > 0) return @min(explicit, global_limit);
    return global_limit / BULK_TENANT_MEMORY_FRACTION;
}

fn estimateBulkMemoryBytes(body_len: usize) usize {
    const chunk_bytes = @min(body_len, BULK_INSERT_CHUNK_BYTES);
    const row_meta = @min(body_len / 4 + 64 * 1024, 2 * 1024 * 1024);
    var total = std.math.add(usize, body_len, BULK_MEMORY_BASE_OVERHEAD) catch return std.math.maxInt(usize);
    total = std.math.add(usize, total, row_meta) catch return std.math.maxInt(usize);
    total = std.math.add(usize, total, chunk_bytes) catch return std.math.maxInt(usize);
    total = std.math.add(usize, total, chunk_bytes) catch return std.math.maxInt(usize);
    return total;
}

fn parseRequestTarget(raw: []const u8) ?ParsedRequestTarget {
    const nl = std.mem.indexOfScalar(u8, raw, '\n') orelse return null;
    const req_line = std.mem.trimEnd(u8, raw[0..nl], "\r");
    var parts = std.mem.splitScalar(u8, req_line, ' ');
    const method = parts.next() orelse return null;
    const full_path = parts.next() orelse return null;

    var path = full_path;
    var query: []const u8 = "";
    if (std.mem.indexOfScalar(u8, full_path, '?')) |qi| {
        path = full_path[0..qi];
        query = full_path[qi + 1 ..];
    }

    return .{ .method = method, .path = path, .query = query };
}

fn isBulkInsertTarget(method: []const u8, path: []const u8) bool {
    if (!std.mem.eql(u8, method, "POST")) return false;
    if (!std.mem.startsWith(u8, path, "/db/")) return false;
    const rest = path[4..];
    const sep = std.mem.indexOfScalar(u8, rest, '/') orelse return false;
    const key = rest[sep + 1 ..];
    return std.mem.eql(u8, key, "bulk") or std.mem.eql(u8, key, "bulk_binary");
}

fn parseGrpcTarget(path: []const u8) ?GrpcTarget {
    if (!std.mem.startsWith(u8, path, "/grpc/")) return null;
    const rest = path["/grpc/".len..];
    const sep = std.mem.indexOfScalar(u8, rest, '/') orelse return null;
    if (sep == 0 or sep + 1 >= rest.len) return null;
    return .{
        .col_name = rest[0..sep],
        .rpc = rest[sep + 1 ..],
    };
}

fn isGrpcBulkMutation(method: []const u8, path: []const u8) bool {
    if (!std.mem.eql(u8, method, "POST")) return false;
    const target = parseGrpcTarget(path) orelse return false;
    return std.mem.eql(u8, target.rpc, "BulkInsert") or
        std.mem.eql(u8, target.rpc, "BulkUpdate") or
        std.mem.eql(u8, target.rpc, "BulkUpsert") or
        std.mem.eql(u8, target.rpc, "BulkDelete");
}

fn isBulkMemoryTarget(method: []const u8, path: []const u8) bool {
    return isBulkInsertTarget(method, path) or isGrpcBulkMutation(method, path);
}

fn preflightBulkAdmission(srv: *Server, initial: []const u8, body_len: usize) BulkPreflight {
    const target = parseRequestTarget(initial) orelse return .not_bulk;
    if (!isBulkMemoryTarget(target.method, target.path)) return .not_bulk;

    var auth_ctx = auth.AuthContext{
        .perm = .admin,
        .tenant_id = [_]u8{0} ** 64,
        .tenant_id_len = 0,
    };
    var auth_ctx_bound = false;
    if (srv.db.auth.isEnabled()) {
        const api_key = auth.AuthStore.extractHttpKey(initial) orelse
            return .{ .reject = .{ .code = 401, .message = "unauthorized — missing X-Api-Key header" } };
        auth_ctx = srv.db.auth.resolve(api_key) orelse
            return .{ .reject = .{ .code = 401, .message = "unauthorized — invalid API key" } };
        auth_ctx_bound = true;
    }

    const auth_ctx_ptr: ?*const auth.AuthContext = if (auth_ctx_bound) &auth_ctx else null;
    if (!canWriteDb(auth_ctx_ptr)) {
        return .{ .reject = .{ .code = 403, .message = "forbidden: API key is read-only" } };
    }

    const tenant_id = requestTenant(initial, target.query, auth_ctx_ptr);
    const estimated_bytes = estimateBulkMemoryBytes(body_len);
    const reservation = srv.acquireBulkMemory(tenant_id, estimated_bytes) catch {
        return .{ .reject = .{ .code = 429, .message = "bulk memory limit exceeded" } };
    };
    return .{ .reserved = reservation };
}

fn releaseThreadBulkReservation() void {
    if (tl_bulk_reservation) |*reservation| reservation.release();
    tl_bulk_reservation = null;
}

/// Wrapper that tracks active connection count around handleConn.
fn handleConnWrapped(srv: *Server, conn: compat.net.Server.Connection) void {
    _ = srv.active_conns.fetchAdd(1, .monotonic);
    defer _ = srv.active_conns.fetchSub(1, .monotonic);
    handleConn(srv, conn);
}

fn handleConn(srv: *Server, conn: compat.net.Server.Connection) void {
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

        // Read the full body based on Content-Length. The initial read may only
        // contain part of a large body.
        const content_length = extractContentLength(initial);
        if (content_length > 0) {
            const header_end = if (std.mem.indexOf(u8, initial, "\r\n\r\n")) |p| p + 4 else if (std.mem.indexOf(u8, initial, "\n\n")) |p| p + 2 else n;
            const total_size = header_end + content_length;
            if (content_length > MAX_BULK) {
                const resp_len = err(413, "request too large");
                conn.stream.writeAll(bufs.resp[0..resp_len]) catch return;
                return;
            }

            switch (preflightBulkAdmission(srv, initial, content_length)) {
                .not_bulk => {},
                .reserved => |reservation| tl_bulk_reservation = reservation,
                .reject => |rejection| {
                    const resp_len = err(rejection.code, rejection.message);
                    conn.stream.writeAll(bufs.resp[0..resp_len]) catch return;
                    return;
                },
            }

            if (total_size > bufs.req.len) {
                const big_buf = std.heap.page_allocator.alloc(u8, total_size) catch {
                    releaseThreadBulkReservation();
                    const resp_len = err(500, "request allocation failed");
                    conn.stream.writeAll(bufs.resp[0..resp_len]) catch return;
                    return;
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
                releaseThreadBulkReservation();
                conn.stream.writeAll(bufs.resp[0..resp_len]) catch return;
                continue;
            } else {
                while (n < total_size) {
                    const r = conn.stream.read(bufs.req[n..total_size]) catch break;
                    if (r == 0) break;
                    n += r;
                }
                const resp_len = dispatch(srv, bufs.req[0..n], std.heap.page_allocator);
                releaseThreadBulkReservation();
                conn.stream.writeAll(bufs.resp[0..resp_len]) catch return;
                continue;
            }
        }

        const resp_len = dispatch(srv, initial, std.heap.page_allocator);
        conn.stream.writeAll(bufs.resp[0..resp_len]) catch return;
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
    const body = if (std.mem.indexOf(u8, raw, "\r\n\r\n")) |p| raw[p + 4 ..] else if (std.mem.indexOf(u8, raw, "\n\n")) |p| raw[p + 2 ..] else @as([]const u8, "");

    // Route: /health
    if (std.mem.eql(u8, path, "/health")) {
        var hb: [64]u8 = undefined;
        const health_json = std.fmt.bufPrint(&hb, "{{\"status\":\"ok\",\"engine\":\"TurboDB\"}}", .{}) catch "{}";
        return ok(health_json);
    }

    // Route: /metrics
    if (std.mem.eql(u8, path, "/metrics")) {
        var fbs = compat.fixedBufferStream(getBodyBuf());
        srv.bulk_mu.lock();
        const bulk_inflight_bytes = srv.bulk_inflight_bytes;
        const bulk_inflight_requests = srv.bulk_inflight_requests;
        srv.bulk_mu.unlock();
        const bulk_global_limit = bulkGlobalMemoryLimit();
        compat.format(fbs.writer(), "{{\"requests\":{d},\"errors\":{d},\"queries\":{d},\"rows_scanned\":{d},\"bytes_read\":{d},\"cpu_us\":{d},\"cost_nanos_usd\":{d},\"bulk_inflight_bytes\":{d},\"bulk_inflight_requests\":{d},\"bulk_rejected\":{d},\"bulk_memory_limit_bytes\":{d},\"bulk_tenant_memory_limit_bytes\":{d}}}", .{
            srv.req_count.load(.acquire),
            srv.err_count.load(.acquire),
            srv.query_count.load(.acquire),
            srv.query_rows_scanned.load(.acquire),
            srv.query_bytes_read.load(.acquire),
            srv.query_cpu_us.load(.acquire),
            srv.query_cost_nanos_usd.load(.acquire),
            bulk_inflight_bytes,
            bulk_inflight_requests,
            srv.bulk_rejected.load(.acquire),
            bulk_global_limit,
            bulkTenantMemoryLimit(bulk_global_limit),
        }) catch {};
        return ok(getBodyBuf()[0..fbs.pos]);
    }

    var auth_ctx = auth.AuthContext{
        .perm = .admin,
        .tenant_id = [_]u8{0} ** 64,
        .tenant_id_len = 0,
    };
    var auth_ctx_bound = false;

    // ── Auth gate — public endpoints above, protected endpoints below ────
    if (srv.db.auth.isEnabled()) {
        const api_key = auth.AuthStore.extractHttpKey(raw) orelse
            return err(401, "unauthorized — missing X-Api-Key header");
        auth_ctx = srv.db.auth.resolve(api_key) orelse
            return err(401, "unauthorized — invalid API key");
        auth_ctx_bound = true;
    }
    const auth_ctx_ptr: ?*const auth.AuthContext = if (auth_ctx_bound) &auth_ctx else null;

    if (std.mem.eql(u8, path, "/billing") and std.mem.eql(u8, method, "GET"))
        return handleBillingLog(srv);

    if (std.mem.eql(u8, path, "/resource_state") and std.mem.eql(u8, method, "GET"))
        return handleResourceState(srv);

    if (std.mem.eql(u8, path, "/cdc/webhooks") and std.mem.eql(u8, method, "POST"))
        return handleWebhookRegistration(srv, body, auth_ctx_ptr);

    if (std.mem.eql(u8, path, "/cdc/events") and std.mem.eql(u8, method, "GET"))
        return handleCdcEvents(srv, requestTenantFilter(query, auth_ctx_ptr), alloc);

    // Route: /collections
    if (std.mem.eql(u8, path, "/collections") and std.mem.eql(u8, method, "GET"))
        return handleListCollections(srv, requestTenant(raw, query, auth_ctx_ptr), alloc);

    // Route: /search/:col?q=...
    if (std.mem.startsWith(u8, path, "/search/") and std.mem.eql(u8, method, "GET")) {
        const col_name = path[8..];
        const q_raw = qparam(query, "q") orelse return err(400, "missing q parameter");
        var decode_buf: [4096]u8 = undefined;
        const q = urlDecode(q_raw, &decode_buf) orelse q_raw;
        const limit_val: u32 = @min(qparamInt(query, "limit") orelse 50, 500);
        return handleSearch(srv, requestTenant(raw, query, auth_ctx_ptr), col_name, q, limit_val, alloc);
    }

    // Route: /context/:col?q=...&limit=20
    if (std.mem.startsWith(u8, path, "/context/") and std.mem.eql(u8, method, "GET")) {
        const col_name = path[9..];
        const q_raw = qparam(query, "q") orelse return err(400, "missing q parameter");
        var decode_buf: [4096]u8 = undefined;
        const q = urlDecode(q_raw, &decode_buf) orelse q_raw;
        const limit_val: u32 = @min(qparamInt(query, "limit") orelse 20, 100);
        return handleDiscoverContext(srv, requestTenant(raw, query, auth_ctx_ptr), col_name, q, limit_val, alloc);
    }

    // Routes under /branch/:col[/:branch[/:key]]
    if (std.mem.startsWith(u8, path, "/branch/")) {
        const rest = path[8..]; // after "/branch/"
        const tenant_id = requestTenant(raw, query, auth_ctx_ptr);

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

    // Routes under /grpc/:col/:rpc.
    // This is a bridge for gRPC-style clients on the current HTTP/1 transport:
    // if Content-Type is application/grpc, the body must be a single uncompressed
    // gRPC frame; otherwise the same raw binary payload is accepted directly.
    if (parseGrpcTarget(path)) |target| {
        if (!std.mem.eql(u8, method, "POST")) return err(404, "not found");
        if (!canWriteDb(auth_ctx_ptr)) return err(403, "forbidden: API key is read-only");
        const payload = grpcPayload(raw, body) orelse return err(400, "bad grpc frame");
        const tenant_id = requestTenant(raw, query, auth_ctx_ptr);
        if (std.mem.eql(u8, target.rpc, "BulkInsert"))
            return handleBulkInsertBinaryWithMode(srv, tenant_id, target.col_name, payload, alloc, "grpc_bulk_insert", "grpc_bridge_binary");
        if (std.mem.eql(u8, target.rpc, "BulkUpdate") or std.mem.eql(u8, target.rpc, "BulkUpsert"))
            return handleBulkUpdateBinary(srv, tenant_id, target.col_name, payload, "grpc_bulk_update", "grpc_bridge_binary");
        if (std.mem.eql(u8, target.rpc, "BulkDelete"))
            return handleBulkDeleteBinary(srv, tenant_id, target.col_name, payload, "grpc_bulk_delete", "grpc_bridge_binary");
        return err(404, "not found");
    }

    // Routes under /db/:col
    if (std.mem.startsWith(u8, path, "/db/")) {
        const rest = path[4..];
        // /db/:col/:key
        if (std.mem.indexOfScalar(u8, rest, '/')) |sep| {
            const col_name = rest[0..sep];
            const key = rest[sep + 1 ..];
            const tenant_id = requestTenant(raw, query, auth_ctx_ptr);
            // POST /db/:col/batch_get — read-only bulk point lookup.
            if (std.mem.eql(u8, key, "batch_get") and std.mem.eql(u8, method, "POST"))
                return handleBatchGet(srv, tenant_id, col_name, body, requestAsOf(raw, query), batchDocsCompact(query));
            // POST /db/:edge_col/join — read-only edge doc lookup + target batch lookup.
            if (std.mem.eql(u8, key, "join") and std.mem.eql(u8, method, "POST"))
                return handleJoin(srv, tenant_id, col_name, body, batchDocsCompact(query));
            if (isBasicDbWriteMethod(method) and !canWriteDb(auth_ctx_ptr))
                return err(403, "forbidden: API key is read-only");
            // POST /db/:col/batch_update — write-gated bulk upsert.
            if (std.mem.eql(u8, key, "batch_update") and std.mem.eql(u8, method, "POST"))
                return handleBatchUpdate(srv, tenant_id, col_name, body, alloc);
            // POST /db/:col/batch_delete — write-gated bulk delete.
            if (std.mem.eql(u8, key, "batch_delete") and std.mem.eql(u8, method, "POST"))
                return handleBatchDelete(srv, tenant_id, col_name, body);
            // POST /db/:col/bulk — bulk insert
            if (std.mem.eql(u8, key, "bulk") and std.mem.eql(u8, method, "POST"))
                return handleBulkInsert(srv, tenant_id, col_name, body, alloc);
            // POST /db/:col/bulk_binary — length-prefixed bulk insert.
            if (std.mem.eql(u8, key, "bulk_binary") and std.mem.eql(u8, method, "POST"))
                return handleBulkInsertBinaryWithMode(srv, tenant_id, col_name, body, alloc, "bulk_insert_bin", "binary");
            if (std.mem.eql(u8, method, "GET")) return handleGet(srv, tenant_id, col_name, key, requestAsOf(raw, query));
            if (std.mem.eql(u8, method, "PUT")) return handleUpdate(srv, tenant_id, col_name, key, body, alloc);
            if (std.mem.eql(u8, method, "DELETE")) return handleDelete(srv, tenant_id, col_name, key);
        } else {
            const col_name = rest;
            const tenant_id = requestTenant(raw, query, auth_ctx_ptr);
            if (isBasicDbWriteMethod(method) and !canWriteDb(auth_ctx_ptr))
                return err(403, "forbidden: API key is read-only");
            if (std.mem.eql(u8, method, "POST")) return handleInsert(srv, tenant_id, col_name, body, alloc);
            if (std.mem.eql(u8, method, "GET")) return handleScan(srv, tenant_id, col_name, query, requestAsOf(raw, query), alloc);
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
    if (!documentFitsLeaf(key, value)) return err(413, "document too large");
    srv.db.recordTenantOperation(tenant_id) catch return err(429, "tenant ops quota exceeded");
    srv.db.ensureTenantStorageAvailable(tenant_id, value.len) catch return err(429, "tenant storage quota exceeded");
    const col = srv.db.collectionForTenant(tenant_id, col_name) catch return err(500, "open collection failed");
    const doc_id = col.insert(key, value) catch return err(500, "insert failed");
    srv.recordQueryCost(tenant_id, "insert", 1, value.len, start_ns);
    var fbs = compat.fixedBufferStream(getBodyBuf());
    compat.format(fbs.writer(), "{{\"doc_id\":{d},\"key\":\"{s}\",\"collection\":\"{s}\",\"tenant\":\"{s}\"}}", .{ doc_id, key, col_name, tenant_id }) catch {};
    return ok(getBodyBuf()[0..fbs.pos]);
}

/// POST /db/:col/bulk — insert multiple documents in one request.
/// Body: NDJSON — one {"key":"...","value":"..."} per line.
/// Response: {"inserted":N,"errors":M,"collection":"...","tenant":"..."}
fn handleBulkInsert(srv: *Server, tenant_id: []const u8, col_name: []const u8, body: []const u8, alloc: std.mem.Allocator) usize {
    const start_ns = compat.nanoTimestamp();
    var local_bulk_reservation: ?Server.BulkMemoryReservation = null;
    if (tl_bulk_reservation == null) {
        local_bulk_reservation = srv.acquireBulkMemory(tenant_id, estimateBulkMemoryBytes(body.len)) catch
            return err(429, "bulk memory limit exceeded");
    }
    defer if (local_bulk_reservation) |*reservation| reservation.release();

    srv.db.recordTenantOperation(tenant_id) catch return err(429, "tenant ops quota exceeded");
    srv.db.ensureTenantStorageAvailable(tenant_id, body.len) catch return err(429, "tenant storage quota exceeded");
    const col = srv.db.collectionForTenant(tenant_id, col_name) catch return err(500, "open collection failed");

    var inserted: u32 = 0;
    var errors: u32 = 0;
    var total_bytes: u64 = 0;
    var chunk_bytes: usize = 0;
    var rows: std.ArrayList(collection.Collection.BulkInsertRow) = .empty;
    defer rows.deinit(alloc);
    rows.ensureTotalCapacity(alloc, @min(BULK_INSERT_CHUNK_ROWS, @max(@as(usize, 1), body.len / 96))) catch
        return err(500, "bulk request too large");

    const BulkFlush = struct {
        fn run(
            col_arg: *collection.Collection,
            rows_arg: *std.ArrayList(collection.Collection.BulkInsertRow),
            chunk_bytes_arg: *usize,
            inserted_arg: *u32,
            errors_arg: *u32,
            bytes_arg: *u64,
        ) !void {
            if (rows_arg.items.len == 0) return;
            const bulk = try col_arg.insertBulk(rows_arg.items);
            inserted_arg.* += bulk.inserted;
            errors_arg.* += bulk.errors;
            bytes_arg.* += bulk.bytes;
            rows_arg.clearRetainingCapacity();
            chunk_bytes_arg.* = 0;
        }
    };

    // Parse NDJSON: iterate lines, each is a {"key":"...","value":...} object
    var pos: usize = 0;
    while (pos < body.len) {
        // Find end of line
        const line_end = std.mem.indexOfScalarPos(u8, body, pos, '\n') orelse body.len;
        const line = std.mem.trim(u8, body[pos..line_end], " \t\r");
        pos = line_end + 1;

        if (line.len < 2) continue; // skip empty lines

        const parsed = parseBulkLine(line) orelse {
            errors += 1;
            continue;
        };
        const key = parsed.key;
        const value = parsed.value;
        const row_bytes = key.len + value.len + 128;
        if (rows.items.len > 0 and chunk_bytes + row_bytes > BULK_INSERT_CHUNK_BYTES) {
            BulkFlush.run(col, &rows, &chunk_bytes, &inserted, &errors, &total_bytes) catch
                return err(500, "bulk insert failed");
        }
        rows.append(alloc, .{ .key = key, .value = value, .line_len = line.len }) catch
            return err(500, "bulk request too large");
        chunk_bytes += row_bytes;

        if (rows.items.len >= BULK_INSERT_CHUNK_ROWS) {
            BulkFlush.run(col, &rows, &chunk_bytes, &inserted, &errors, &total_bytes) catch
                return err(500, "bulk insert failed");
        }
    }
    BulkFlush.run(col, &rows, &chunk_bytes, &inserted, &errors, &total_bytes) catch
        return err(500, "bulk insert failed");

    srv.recordQueryCost(tenant_id, "bulk_insert", inserted, total_bytes, start_ns);

    var fbs = compat.fixedBufferStream(getBodyBuf());
    compat.format(fbs.writer(), "{{\"inserted\":{d},\"errors\":{d},\"collection\":\"{s}\",\"tenant\":\"{s}\"}}", .{ inserted, errors, col_name, tenant_id }) catch {};
    return ok(getBodyBuf()[0..fbs.pos]);
}

/// POST /db/:col/bulk_binary — insert multiple documents in one request.
/// Body: repeated little-endian records:
///   [key_len:u16][value_len:u32][key bytes][raw value bytes]
fn handleBulkInsertBinaryWithMode(
    srv: *Server,
    tenant_id: []const u8,
    col_name: []const u8,
    body: []const u8,
    alloc: std.mem.Allocator,
    op_name: []const u8,
    mode: []const u8,
) usize {
    const start_ns = compat.nanoTimestamp();
    var local_bulk_reservation: ?Server.BulkMemoryReservation = null;
    if (tl_bulk_reservation == null) {
        local_bulk_reservation = srv.acquireBulkMemory(tenant_id, estimateBulkMemoryBytes(body.len)) catch
            return err(429, "bulk memory limit exceeded");
    }
    defer if (local_bulk_reservation) |*reservation| reservation.release();

    srv.db.recordTenantOperation(tenant_id) catch return err(429, "tenant ops quota exceeded");
    srv.db.ensureTenantStorageAvailable(tenant_id, body.len) catch return err(429, "tenant storage quota exceeded");
    const col = srv.db.collectionForTenant(tenant_id, col_name) catch return err(500, "open collection failed");

    var inserted: u32 = 0;
    var errors: u32 = 0;
    var total_bytes: u64 = 0;
    var chunk_bytes: usize = 0;
    var rows: std.ArrayList(collection.Collection.BulkInsertRow) = .empty;
    defer rows.deinit(alloc);
    rows.ensureTotalCapacity(alloc, @min(BULK_INSERT_CHUNK_ROWS, @max(@as(usize, 1), body.len / 80))) catch
        return err(500, "bulk request too large");

    const BulkFlush = struct {
        fn run(
            col_arg: *collection.Collection,
            rows_arg: *std.ArrayList(collection.Collection.BulkInsertRow),
            chunk_bytes_arg: *usize,
            inserted_arg: *u32,
            errors_arg: *u32,
            bytes_arg: *u64,
        ) !void {
            if (rows_arg.items.len == 0) return;
            const bulk = try col_arg.insertBulk(rows_arg.items);
            inserted_arg.* += bulk.inserted;
            errors_arg.* += bulk.errors;
            bytes_arg.* += bulk.bytes;
            rows_arg.clearRetainingCapacity();
            chunk_bytes_arg.* = 0;
        }
    };

    var pos: usize = 0;
    while (pos < body.len) {
        if (pos + 6 > body.len) {
            errors += 1;
            break;
        }
        const key_len: usize = std.mem.readInt(u16, body[pos..][0..2], .little);
        const value_len: usize = std.mem.readInt(u32, body[pos + 2 ..][0..4], .little);
        pos += 6;
        const end = std.math.add(usize, pos, key_len) catch {
            errors += 1;
            break;
        };
        const value_end = std.math.add(usize, end, value_len) catch {
            errors += 1;
            break;
        };
        if (key_len == 0 or end > body.len or value_end > body.len) {
            errors += 1;
            break;
        }

        const key = body[pos..end];
        const value = body[end..value_end];
        pos = value_end;

        const row_bytes = key.len + value.len + 128;
        if (rows.items.len > 0 and chunk_bytes + row_bytes > BULK_INSERT_CHUNK_BYTES) {
            BulkFlush.run(col, &rows, &chunk_bytes, &inserted, &errors, &total_bytes) catch
                return err(500, "bulk insert failed");
        }
        rows.append(alloc, .{ .key = key, .value = value, .line_len = 6 + key.len + value.len }) catch
            return err(500, "bulk request too large");
        chunk_bytes += row_bytes;

        if (rows.items.len >= BULK_INSERT_CHUNK_ROWS) {
            BulkFlush.run(col, &rows, &chunk_bytes, &inserted, &errors, &total_bytes) catch
                return err(500, "bulk insert failed");
        }
    }
    BulkFlush.run(col, &rows, &chunk_bytes, &inserted, &errors, &total_bytes) catch
        return err(500, "bulk insert failed");

    srv.recordQueryCost(tenant_id, op_name, inserted, total_bytes, start_ns);

    var fbs = compat.fixedBufferStream(getBodyBuf());
    compat.format(fbs.writer(), "{{\"inserted\":{d},\"errors\":{d},\"collection\":\"{s}\",\"tenant\":\"{s}\",\"mode\":\"{s}\"}}", .{ inserted, errors, col_name, tenant_id, mode }) catch {};
    return ok(getBodyBuf()[0..fbs.pos]);
}

/// Binary bulk update/upsert used by the gRPC bridge.
/// Body: repeated little-endian records:
///   [key_len:u16][value_len:u32][key bytes][raw value bytes]
fn handleBulkUpdateBinary(srv: *Server, tenant_id: []const u8, col_name: []const u8, body: []const u8, op_name: []const u8, mode: []const u8) usize {
    const start_ns = compat.nanoTimestamp();
    var local_bulk_reservation: ?Server.BulkMemoryReservation = null;
    if (tl_bulk_reservation == null) {
        local_bulk_reservation = srv.acquireBulkMemory(tenant_id, estimateBulkMemoryBytes(body.len)) catch
            return err(429, "bulk memory limit exceeded");
    }
    defer if (local_bulk_reservation) |*reservation| reservation.release();

    srv.db.recordTenantOperation(tenant_id) catch return err(429, "tenant ops quota exceeded");
    srv.db.ensureTenantStorageAvailable(tenant_id, body.len) catch return err(429, "tenant storage quota exceeded");
    const col = srv.db.collectionForTenant(tenant_id, col_name) catch return err(500, "open collection failed");

    var updated: u32 = 0;
    var inserted: u32 = 0;
    var errors: u32 = 0;
    var total_bytes: u64 = 0;
    var pos: usize = 0;
    while (pos < body.len) {
        const record = nextBinaryKeyValue(body, &pos) orelse {
            errors += 1;
            break;
        };
        if (!documentFitsLeaf(record.key, record.value)) {
            errors += 1;
            continue;
        }
        const did_update = col.update(record.key, record.value) catch {
            errors += 1;
            continue;
        };
        if (did_update) {
            updated += 1;
        } else {
            _ = col.insert(record.key, record.value) catch {
                errors += 1;
                continue;
            };
            inserted += 1;
        }
        total_bytes += record.key.len + record.value.len;
    }

    srv.recordQueryCost(tenant_id, op_name, updated + inserted + errors, total_bytes, start_ns);

    var fbs = compat.fixedBufferStream(getBodyBuf());
    compat.format(fbs.writer(), "{{\"updated\":{d},\"inserted\":{d},\"errors\":{d},\"collection\":\"{s}\",\"tenant\":\"{s}\",\"mode\":\"{s}\"}}", .{ updated, inserted, errors, col_name, tenant_id, mode }) catch {};
    return ok(getBodyBuf()[0..fbs.pos]);
}

/// Binary bulk delete used by the gRPC bridge.
/// Body: repeated little-endian records:
///   [key_len:u16][key bytes]
fn handleBulkDeleteBinary(srv: *Server, tenant_id: []const u8, col_name: []const u8, body: []const u8, op_name: []const u8, mode: []const u8) usize {
    const start_ns = compat.nanoTimestamp();
    var local_bulk_reservation: ?Server.BulkMemoryReservation = null;
    if (tl_bulk_reservation == null) {
        local_bulk_reservation = srv.acquireBulkMemory(tenant_id, estimateBulkMemoryBytes(body.len)) catch
            return err(429, "bulk memory limit exceeded");
    }
    defer if (local_bulk_reservation) |*reservation| reservation.release();

    srv.db.recordTenantOperation(tenant_id) catch return err(429, "tenant ops quota exceeded");
    const col = srv.db.collectionForTenant(tenant_id, col_name) catch return err(500, "open collection failed");

    var deleted: u32 = 0;
    var missing: u32 = 0;
    var errors: u32 = 0;
    var pos: usize = 0;
    while (pos < body.len) {
        const record = nextBinaryKey(body, &pos) orelse {
            errors += 1;
            break;
        };
        const did_delete = col.delete(record.key) catch {
            errors += 1;
            continue;
        };
        if (did_delete) deleted += 1 else missing += 1;
    }

    srv.recordQueryCost(tenant_id, op_name, deleted + missing + errors, body.len, start_ns);

    var fbs = compat.fixedBufferStream(getBodyBuf());
    compat.format(fbs.writer(), "{{\"deleted\":{d},\"missing\":{d},\"errors\":{d},\"collection\":\"{s}\",\"tenant\":\"{s}\",\"mode\":\"{s}\"}}", .{ deleted, missing, errors, col_name, tenant_id, mode }) catch {};
    return ok(getBodyBuf()[0..fbs.pos]);
}

/// POST /db/:col/batch_get — read multiple documents in one request.
/// Body can be a JSON string array, {"keys":[...]}, or one key per line.
fn handleBatchGet(srv: *Server, tenant_id: []const u8, col_name: []const u8, body: []const u8, as_of: ?i64, compact: bool) usize {
    const start_ns = compat.nanoTimestamp();
    srv.db.recordTenantOperation(tenant_id) catch return err(429, "tenant ops quota exceeded");
    const col = srv.db.collectionForTenant(tenant_id, col_name) catch return err(500, "open collection failed");
    var iter = KeyIter.init(body);
    return writeBatchDocsResponse(srv, tenant_id, col_name, col, &iter, as_of, "batch_get", start_ns, 0, compact);
}

/// POST /db/:col/batch_update — upsert multiple documents in one request.
/// Body: NDJSON — one {"key":"...","value":...} per line.
fn handleBatchUpdate(srv: *Server, tenant_id: []const u8, col_name: []const u8, body: []const u8, alloc: std.mem.Allocator) usize {
    _ = alloc;
    const start_ns = compat.nanoTimestamp();
    srv.db.recordTenantOperation(tenant_id) catch return err(429, "tenant ops quota exceeded");
    srv.db.ensureTenantStorageAvailable(tenant_id, body.len) catch return err(429, "tenant storage quota exceeded");
    const col = srv.db.collectionForTenant(tenant_id, col_name) catch return err(500, "open collection failed");

    var iter = BatchUpdateIter.init(body);
    var updated: u32 = 0;
    var inserted: u32 = 0;
    var errors: u32 = 0;
    var total_bytes: u64 = 0;

    while (iter.next()) |entry| {
        if (!entry.valid) {
            errors += 1;
            continue;
        }
        if (!documentFitsLeaf(entry.key, entry.value)) {
            errors += 1;
            continue;
        }

        const did_update = col.update(entry.key, entry.value) catch {
            errors += 1;
            continue;
        };
        if (did_update) {
            updated += 1;
        } else {
            _ = col.insert(entry.key, entry.value) catch {
                errors += 1;
                continue;
            };
            inserted += 1;
        }
        total_bytes += entry.line_len;
    }

    srv.recordQueryCost(tenant_id, "batch_update", updated + inserted + errors, total_bytes, start_ns);

    var fbs = compat.fixedBufferStream(getBodyBuf());
    compat.format(fbs.writer(), "{{\"updated\":{d},\"inserted\":{d},\"errors\":{d},\"collection\":\"{s}\",\"tenant\":\"{s}\"}}", .{ updated, inserted, errors, col_name, tenant_id }) catch {};
    return ok(getBodyBuf()[0..fbs.pos]);
}

/// POST /db/:col/batch_delete — delete multiple documents in one request.
/// Body can be a JSON string array, {"keys":[...]}, or one key per line.
fn handleBatchDelete(srv: *Server, tenant_id: []const u8, col_name: []const u8, body: []const u8) usize {
    const start_ns = compat.nanoTimestamp();
    srv.db.recordTenantOperation(tenant_id) catch return err(429, "tenant ops quota exceeded");
    const col = srv.db.collectionForTenant(tenant_id, col_name) catch return err(500, "open collection failed");

    var iter = KeyIter.init(body);
    var deleted: u32 = 0;
    var missing: u32 = 0;
    var errors: u32 = 0;
    while (iter.next()) |key| {
        if (key.len == 0) continue;
        const did_delete = col.delete(key) catch {
            errors += 1;
            continue;
        };
        if (did_delete) deleted += 1 else missing += 1;
    }

    srv.recordQueryCost(tenant_id, "batch_delete", deleted + missing + errors, 0, start_ns);

    var fbs = compat.fixedBufferStream(getBodyBuf());
    compat.format(fbs.writer(), "{{\"deleted\":{d},\"missing\":{d},\"errors\":{d},\"collection\":\"{s}\",\"tenant\":\"{s}\"}}", .{ deleted, missing, errors, col_name, tenant_id }) catch {};
    return ok(getBodyBuf()[0..fbs.pos]);
}

/// POST /db/:edge_col/join — read edge.value[field] as target keys and batch-get targets.
fn handleJoin(srv: *Server, tenant_id: []const u8, edge_col_name: []const u8, body: []const u8, compact: bool) usize {
    const req = JoinRequest.parse(body) orelse return err(400, "missing key, target_collection, or field");
    const start_ns = compat.nanoTimestamp();
    srv.db.recordTenantOperation(tenant_id) catch return err(429, "tenant ops quota exceeded");
    const edge_col = srv.db.collectionForTenant(tenant_id, edge_col_name) catch return err(500, "open collection failed");
    const target_col = srv.db.collectionForTenant(tenant_id, req.target_collection) catch return err(500, "open collection failed");

    const edge = edge_col.get(req.key) orelse return err(404, "not found");
    const raw_keys = jsonValue(edge.value, req.field) orelse return err(400, "missing join field");
    const keys = std.mem.trim(u8, raw_keys, " \t\r\n");
    if (keys.len == 0 or keys[0] != '[') return err(400, "join field must be an array");

    var iter = KeyIter.init(keys);
    return writeBatchDocsResponse(
        srv,
        tenant_id,
        req.target_collection,
        target_col,
        &iter,
        null,
        "join",
        start_ns,
        edge.key.len + edge.value.len,
        compact,
    );
}

fn writeBatchDocsResponse(
    srv: *Server,
    tenant_id: []const u8,
    col_name: []const u8,
    col: *collection.Collection,
    iter: *KeyIter,
    as_of: ?i64,
    op_name: []const u8,
    start_ns: i128,
    initial_bytes_read: usize,
    compact: bool,
) usize {
    const HEADER_RESERVE = 256;
    var resp = getRespBuf();
    var fbs = compat.fixedBufferStream(resp[HEADER_RESERVE..]);
    const w = fbs.writer();
    if (!compact) {
        compat.format(w, "{{\"tenant\":\"{s}\",\"collection\":\"{s}\",\"docs\":[", .{ tenant_id, col_name }) catch return err(500, "response too large");
    }

    var found: usize = 0;
    var missing: usize = 0;
    var bytes_read: usize = initial_bytes_read;
    var truncated = false;
    while (iter.next()) |key| {
        if (key.len == 0) continue;
        const d = if (as_of) |ts_ms|
            (col.getAsOfTimestamp(key, ts_ms) orelse {
                missing += 1;
                continue;
            })
        else
            (col.get(key) orelse {
                missing += 1;
                continue;
            });

        if (!compact) {
            const val = if (d.value.len > 0) d.value else "{}";
            const is_json = isJsonValue(val);
            const next_len = (if (found > 0) @as(usize, 1) else 0) + docJsonLen(d, val, is_json);
            if (fbs.pos + next_len + 80 >= resp[HEADER_RESERVE..].len) {
                truncated = true;
                break;
            }
            if (found > 0) w.writeByte(',') catch return err(500, "response too large");
            writeDocJson(w, d, val, is_json) catch return err(500, "response too large");
        }
        found += 1;
        bytes_read += d.key.len + d.value.len;
    }

    if (compact) {
        compat.format(w, "{{\"tenant\":\"{s}\",\"collection\":\"{s}\",\"count\":{d},\"missing\":{d},\"bytes_read\":{d},\"truncated\":{},\"mode\":\"count\"}}", .{ tenant_id, col_name, found, missing, bytes_read, truncated }) catch return err(500, "response too large");
    } else {
        compat.format(w, "],\"count\":{d},\"missing\":{d},\"truncated\":{}}}", .{ found, missing, truncated }) catch return err(500, "response too large");
    }
    const body_len = fbs.pos;

    srv.recordQueryCost(tenant_id, op_name, found + missing, bytes_read, start_ns);

    var hdr_fbs = compat.fixedBufferStream(resp[0..HEADER_RESERVE]);
    compat.format(hdr_fbs.writer(), "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {d}\r\nConnection: keep-alive\r\n\r\n", .{body_len}) catch {};
    const hdr_len = hdr_fbs.pos;
    if (hdr_len < HEADER_RESERVE) {
        std.mem.copyForwards(u8, resp[hdr_len .. hdr_len + body_len], resp[HEADER_RESERVE .. HEADER_RESERVE + body_len]);
    }
    return hdr_len + body_len;
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

    // Write JSON body directly into resp_buf at offset 256 (reserve space for headers)
    const HEADER_RESERVE = 256;
    var resp = getRespBuf();
    var fbs = compat.fixedBufferStream(resp[HEADER_RESERVE..]);
    const val = if (d.value.len > 0) d.value else "{}";
    const is_json = isJsonValue(val);
    const w = fbs.writer();
    writeDocJson(w, d, val, is_json) catch return err(500, "response too large");
    const body_len = fbs.pos;

    // Now write headers into the reserved space at the front
    var hdr_fbs = compat.fixedBufferStream(resp[0..HEADER_RESERVE]);
    compat.format(hdr_fbs.writer(), "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {d}\r\nConnection: keep-alive\r\n\r\n", .{body_len}) catch {};
    const hdr_len = hdr_fbs.pos;

    // Move body right after headers (memmove if needed)
    if (hdr_len < HEADER_RESERVE) {
        std.mem.copyForwards(u8, resp[hdr_len .. hdr_len + body_len], resp[HEADER_RESERVE .. HEADER_RESERVE + body_len]);
    }
    return hdr_len + body_len;
}

fn handleUpdate(srv: *Server, tenant_id: []const u8, col_name: []const u8, key: []const u8, body: []const u8, alloc: std.mem.Allocator) usize {
    const start_ns = compat.nanoTimestamp();
    _ = alloc;
    if (!documentFitsLeaf(key, body)) return err(413, "document too large");
    srv.db.recordTenantOperation(tenant_id) catch return err(429, "tenant ops quota exceeded");
    srv.db.ensureTenantStorageAvailable(tenant_id, body.len) catch return err(429, "tenant storage quota exceeded");
    const col = srv.db.collectionForTenant(tenant_id, col_name) catch return err(500, "open collection failed");
    const updated = col.update(key, body) catch return err(500, "update failed");
    if (updated) {
        srv.recordQueryCost(tenant_id, "update", 1, body.len, start_ns);
        return ok("{\"updated\":true,\"inserted\":false}");
    }

    const doc_id = col.insert(key, body) catch return err(500, "insert failed");
    srv.recordQueryCost(tenant_id, "upsert", 1, body.len, start_ns);

    var buf: [128]u8 = undefined;
    const response = std.fmt.bufPrint(&buf, "{{\"updated\":false,\"inserted\":true,\"doc_id\":{d}}}", .{doc_id}) catch return err(500, "format failed");
    return ok(response);
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

fn documentFitsLeaf(key: []const u8, value: []const u8) bool {
    return doc_mod.DocHeader.size + key.len + value.len <= page_mod.PAGE_USABLE;
}

fn handleScan(srv: *Server, tenant_id: []const u8, col_name: []const u8, query_str: []const u8, as_of: ?i64, alloc: std.mem.Allocator) usize {
    const start_ns = compat.nanoTimestamp();
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

    const emit_count = scanEmitCount(tenant_id, col_name, result.docs);

    var fbs = compat.fixedBufferStream(getBodyBuf());
    const w = fbs.writer();
    compat.format(w, "{{\"tenant\":\"{s}\",\"collection\":\"{s}\",\"count\":{d},\"docs\":[", .{ tenant_id, col_name, emit_count }) catch {};
    for (result.docs[0..emit_count], 0..) |d, i| {
        if (i > 0) w.writeByte(',') catch {};
        const val = if (d.value.len > 0) d.value else "{}";
        const is_json = isJsonValue(val);
        const start_pos = fbs.pos;
        writeDocJson(w, d, val, is_json) catch {
            fbs.pos = start_pos;
            break;
        };
    }
    w.writeAll("]}") catch {};
    return ok(getBodyBuf()[0..fbs.pos]);
}

fn scanEmitCount(tenant_id: []const u8, col_name: []const u8, docs: []const doc_mod.Doc) usize {
    const max_count_digits = decimalLen(docs.len);
    var used: usize = "{\"tenant\":\"".len + tenant_id.len +
        "\",\"collection\":\"".len + col_name.len +
        "\",\"count\":".len + max_count_digits +
        ",\"docs\":[".len + "]}".len;

    var emit_count: usize = 0;
    for (docs) |d| {
        const val = if (d.value.len > 0) d.value else "{}";
        const is_json = isJsonValue(val);
        const next_len = (if (emit_count > 0) @as(usize, 1) else 0) + docJsonLen(d, val, is_json);
        if (used + next_len > MAX_BODY) break;
        used += next_len;
        emit_count += 1;
    }
    return emit_count;
}

fn writeDocJson(w: anytype, d: doc_mod.Doc, val: []const u8, is_json: bool) !void {
    try w.writeAll("{\"doc_id\":");
    try w.print("{d}", .{d.header.doc_id});
    try w.writeAll(",\"key\":");
    try writeJsonString(w, d.key);
    try w.writeAll(",\"version\":");
    try w.print("{d}", .{d.header.version});
    try w.writeAll(",\"value\":");
    if (is_json) {
        try w.writeAll(val);
    } else {
        try writeJsonString(w, val);
    }
    try w.writeByte('}');
}

fn isJsonValue(value: []const u8) bool {
    if (value.len == 0) return false;
    return std.json.validate(std.heap.page_allocator, value) catch false;
}

fn docJsonLen(d: doc_mod.Doc, val: []const u8, is_json: bool) usize {
    return "{\"doc_id\":".len + decimalLen(d.header.doc_id) +
        ",\"key\":".len + jsonStringLen(d.key) +
        ",\"version\":".len + decimalLen(d.header.version) +
        ",\"value\":".len + if (is_json) val.len else jsonStringLen(val) +
        "}".len;
}

fn decimalLen(value: anytype) usize {
    var buf: [32]u8 = undefined;
    return (std.fmt.bufPrint(&buf, "{d}", .{value}) catch return 32).len;
}

fn jsonStringLen(value: []const u8) usize {
    var len: usize = 2;
    for (value) |ch| {
        len += switch (ch) {
            '"', '\\', '\n', '\r', '\t' => 2,
            0...8, 11...12, 14...0x1f => 6,
            else => 1,
        };
    }
    return len;
}

fn writeJsonString(w: anytype, value: []const u8) !void {
    const hex = "0123456789abcdef";
    try w.writeByte('"');
    for (value) |ch| {
        switch (ch) {
            '"' => try w.writeAll("\\\""),
            '\\' => try w.writeAll("\\\\"),
            '\n' => try w.writeAll("\\n"),
            '\r' => try w.writeAll("\\r"),
            '\t' => try w.writeAll("\\t"),
            0...8, 11...12, 14...0x1f => {
                try w.writeAll("\\u00");
                try w.writeByte(hex[ch >> 4]);
                try w.writeByte(hex[ch & 0x0f]);
            },
            else => try w.writeByte(ch),
        }
    }
    try w.writeByte('"');
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

    var fbs = compat.fixedBufferStream(getBodyBuf());
    const w = fbs.writer();
    // Write JSON header with escaped query string
    w.writeAll("{\"query\":\"") catch {};
    for (query) |ch| {
        if (ch == '"' or ch == '\\') {
            w.writeByte('\\') catch {};
        }
        w.writeByte(ch) catch {};
    }
    compat.format(w, "\",\"hits\":{d},\"candidates\":{d},\"total_docs\":{d},\"total_files\":{d},\"results\":[", .{ result.docs.len, result.candidate_paths.len, col.docCount(), result.total_files }) catch {};
    for (result.docs, 0..) |d, i| {
        if (fbs.pos + 256 >= MAX_BODY) break;
        if (i > 0) w.writeByte(',') catch {};
        // Output value as valid JSON — objects/arrays as-is, strings quoted
        const val = if (d.value.len > 0) d.value else "{}";
        const is_json = isJsonValue(val);
        if (is_json) {
            compat.format(w, "{{\"doc_id\":{d},\"key\":\"{s}\",\"value\":{s}}}", .{ d.header.doc_id, d.key, val }) catch {};
        } else {
            w.writeAll("{\"doc_id\":") catch {};
            compat.format(w, "{d},\"key\":\"{s}\",\"value\":\"", .{ d.header.doc_id, d.key }) catch {};
            for (val) |ch| {
                if (ch == '"' or ch == '\\') w.writeByte('\\') catch {};
                if (ch == '\n') {
                    w.writeAll("\\n") catch {};
                    continue;
                }
                w.writeByte(ch) catch {};
            }
            w.writeAll("\"}") catch {};
        }
    }
    w.writeAll("]}") catch {};
    return ok(getBodyBuf()[0..fbs.pos]);
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

    var fbs = compat.fixedBufferStream(getBodyBuf());
    const w = fbs.writer();

    // Write JSON: matching_files
    w.writeAll("{\"query\":\"") catch {};
    for (query) |ch| {
        if (ch == '"' or ch == '\\') {
            w.writeByte('\\') catch {};
        }
        w.writeByte(ch) catch {};
    }
    w.writeAll("\",\"matching_files\":[") catch {};
    for (result.matching_files, 0..) |d, i| {
        if (fbs.pos + 128 >= MAX_BODY) break;
        if (i > 0) w.writeByte(',') catch {};
        compat.format(w, "{{\"key\":\"{s}\",\"size\":{d}}}", .{ d.key, d.value.len }) catch {};
    }
    w.writeAll("],\"related_files\":[") catch {};
    for (result.related_files, 0..) |d, i| {
        if (fbs.pos + 128 >= MAX_BODY) break;
        if (i > 0) w.writeByte(',') catch {};
        compat.format(w, "{{\"key\":\"{s}\"}}", .{d.key}) catch {};
    }
    w.writeAll("],\"test_files\":[") catch {};
    for (result.test_files, 0..) |d, i| {
        if (fbs.pos + 128 >= MAX_BODY) break;
        if (i > 0) w.writeByte(',') catch {};
        compat.format(w, "{{\"key\":\"{s}\"}}", .{d.key}) catch {};
    }
    compat.format(w, "],\"recent_versions\":{d},\"total_files\":{d}}}", .{ result.recent_versions, result.total_files }) catch {};
    return ok(getBodyBuf()[0..fbs.pos]);
}

fn handleListCollections(srv: *Server, tenant_id: []const u8, alloc: std.mem.Allocator) usize {
    const start_ns = compat.nanoTimestamp();
    srv.db.recordTenantOperation(tenant_id) catch return err(429, "tenant ops quota exceeded");
    var cols = srv.db.listCollectionsForTenant(tenant_id, alloc) catch return err(500, "list collections failed");
    defer {
        for (cols.items) |name| alloc.free(name);
        cols.deinit(alloc);
    }
    var fbs = compat.fixedBufferStream(getBodyBuf());
    const w = fbs.writer();
    compat.format(w, "{{\"tenant\":\"{s}\",\"collections\":[", .{tenant_id}) catch {};
    for (cols.items, 0..) |name, i| {
        if (i > 0) w.writeByte(',') catch {};
        compat.format(w, "\"{s}\"", .{name}) catch {};
    }
    srv.recordQueryCost(tenant_id, "list_collections", cols.items.len, 0, start_ns);
    w.writeAll("]}") catch {};
    return ok(getBodyBuf()[0..fbs.pos]);
}

fn handleBillingLog(srv: *Server) usize {
    var fbs = compat.fixedBufferStream(getBodyBuf());
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
        compat.format(
            w,
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
    var fbs = compat.fixedBufferStream(getBodyBuf());
    compat.format(
        fbs.writer(),
        "{{\"state\":\"{s}\",\"queries_per_second\":{d},\"last_query_ms\":{d}}}",
        .{ resourceStateName(srv.activity.state()), srv.activity.queriesPerSecond(), srv.activity.lastQueryMs() },
    ) catch {};
    return ok(getBodyBuf()[0..fbs.pos]);
}

fn handleWebhookRegistration(srv: *Server, body: []const u8, auth_ctx: ?*const auth.AuthContext) usize {
    const tenant = requestBodyTenant(body, auth_ctx) orelse return err(400, "missing tenant");
    const webhook = jsonStr(body, "webhook_url") orelse return err(400, "missing webhook_url");
    const secret = jsonStr(body, "secret") orelse return err(400, "missing secret");
    const collection_name = jsonStr(body, "collection") orelse "";
    const id = srv.db.registerWebhook(tenant, collection_name, webhook, secret) catch return err(500, "register webhook failed");
    var fbs = compat.fixedBufferStream(getBodyBuf());
    compat.format(fbs.writer(), "{{\"subscription_id\":{d}}}", .{id}) catch {};
    return ok(getBodyBuf()[0..fbs.pos]);
}

fn handleCdcEvents(srv: *Server, tenant_filter: ?[]const u8, alloc: std.mem.Allocator) usize {
    const deliveries = srv.db.listWebhookDeliveries(alloc, tenant_filter) catch return err(500, "cdc read failed");
    defer alloc.free(deliveries);
    var fbs = compat.fixedBufferStream(getBodyBuf());
    const w = fbs.writer();
    w.writeAll("{\"events\":[") catch {};
    for (deliveries, 0..) |entry, i| {
        if (i > 0) w.writeByte(',') catch {};
        compat.format(
            w,
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
    var fbs = compat.fixedBufferStream(getBodyBuf());
    compat.format(fbs.writer(), "{{\"key\":\"{s}\",\"value\":{s}}}", .{
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
        var fbs = compat.fixedBufferStream(getBodyBuf());
        compat.format(fbs.writer(), "{{\"merged\":false,\"conflicts\":{d}}}", .{result.conflicts.len}) catch {};
        return respond(409, "Conflict", getBodyBuf()[0..fbs.pos]);
    }
    var fbs = compat.fixedBufferStream(getBodyBuf());
    compat.format(fbs.writer(), "{{\"merged\":true,\"applied\":{d}}}", .{result.applied}) catch {};
    return ok(getBodyBuf()[0..fbs.pos]);
}

fn handleBranchSearch(srv: *Server, tenant_id: []const u8, col_name: []const u8, branch_name: []const u8, query_text: []const u8, limit: u32, alloc: std.mem.Allocator) usize {
    srv.db.recordTenantOperation(tenant_id) catch return err(429, "tenant ops quota exceeded");
    const col = srv.db.collectionForTenant(tenant_id, col_name) catch return err(500, "open collection failed");
    const br = col.getBranch(branch_name) orelse return err(404, "branch not found");
    const result = col.searchOnBranch(br, query_text, limit, alloc) catch return err(500, "branch search failed");
    defer result.deinit();

    var fbs = compat.fixedBufferStream(getBodyBuf());
    const w = fbs.writer();
    compat.format(w, "{{\"branch\":\"{s}\",\"hits\":{d},\"results\":[", .{ branch_name, result.docs.len }) catch {};
    for (result.docs, 0..) |d, i| {
        if (i > 0) w.writeByte(',') catch {};
        compat.format(w, "{{\"doc_id\":{d},\"key\":\"{s}\",\"value\":{s}}}", .{ d.header.doc_id, d.key, if (d.value.len > 0) d.value else "{}" }) catch {};
    }
    w.writeAll("]}") catch {};
    return ok(getBodyBuf()[0..fbs.pos]);
}

fn handleListBranches(srv: *Server, tenant_id: []const u8, col_name: []const u8, alloc: std.mem.Allocator) usize {
    srv.db.recordTenantOperation(tenant_id) catch return err(429, "tenant ops quota exceeded");
    const col = srv.db.collectionForTenant(tenant_id, col_name) catch return err(500, "open collection failed");
    const names = col.listBranches(alloc) catch return err(500, "list branches failed");
    defer alloc.free(names);

    var fbs = compat.fixedBufferStream(getBodyBuf());
    const w = fbs.writer();
    w.writeAll("{\"branches\":[") catch {};
    for (names, 0..) |name, i| {
        if (i > 0) w.writeByte(',') catch {};
        compat.format(w, "\"{s}\"", .{name}) catch {};
    }
    w.writeAll("]}") catch {};
    return ok(getBodyBuf()[0..fbs.pos]);
}

fn requestTenant(raw: []const u8, query: []const u8, auth_ctx: ?*const auth.AuthContext) []const u8 {
    if (auth_ctx) |ctx| {
        if (ctx.perm != .admin) return boundTenant(ctx);
    }
    return header(raw, "X-Tenant-Id: ") orelse qparam(query, "tenant") orelse collection.DEFAULT_TENANT;
}

fn requestTenantFilter(query: []const u8, auth_ctx: ?*const auth.AuthContext) ?[]const u8 {
    if (auth_ctx) |ctx| {
        if (ctx.perm != .admin) return boundTenant(ctx);
    }
    return qparam(query, "tenant");
}

fn requestBodyTenant(body: []const u8, auth_ctx: ?*const auth.AuthContext) ?[]const u8 {
    if (auth_ctx) |ctx| {
        if (ctx.perm != .admin) return boundTenant(ctx);
    }
    return jsonStr(body, "tenant");
}

fn boundTenant(ctx: *const auth.AuthContext) []const u8 {
    const tenant_id = ctx.tenant();
    return if (tenant_id.len > 0) tenant_id else collection.DEFAULT_TENANT;
}

fn isBasicDbWriteMethod(method: []const u8) bool {
    return std.mem.eql(u8, method, "POST") or
        std.mem.eql(u8, method, "PUT") or
        std.mem.eql(u8, method, "DELETE");
}

fn canWriteDb(auth_ctx: ?*const auth.AuthContext) bool {
    const ctx = auth_ctx orelse return true;
    return ctx.perm != .read_only;
}

fn requestAsOf(raw: []const u8, query: []const u8) ?i64 {
    const value = header(raw, "X-As-Of: ") orelse qparam(query, "as_of") orelse return null;
    return parseAsOfTimestamp(value);
}

fn batchDocsCompact(query: []const u8) bool {
    if (qparam(query, "mode")) |mode|
        return std.mem.eql(u8, mode, "count") or std.mem.eql(u8, mode, "compact");
    if (qparam(query, "docs")) |docs|
        return std.mem.eql(u8, docs, "0") or std.mem.eql(u8, docs, "false");
    return false;
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
        403 => "Forbidden",
        429 => "Too Many Requests",
        413 => "Payload Too Large",
        404 => "Not Found",
        else => "Internal Server Error",
    };
    return respond(code, status, body);
}

fn respond(code: u16, status: []const u8, body: []const u8) usize {
    const resp = getRespBuf();
    var header_buf: [256]u8 = undefined;
    const header_bytes = std.fmt.bufPrint(
        &header_buf,
        "HTTP/1.1 {d} {s}\r\nContent-Type: application/json\r\nContent-Length: {d}\r\nConnection: keep-alive\r\n\r\n",
        .{ code, status, body.len },
    ) catch return 0;

    if (header_bytes.len + body.len > resp.len) {
        const fallback_body = "{\"error\":\"response too large\"}";
        const fallback_header = std.fmt.bufPrint(
            &header_buf,
            "HTTP/1.1 500 Internal Server Error\r\nContent-Type: application/json\r\nContent-Length: {d}\r\nConnection: close\r\n\r\n",
            .{fallback_body.len},
        ) catch return 0;
        @memcpy(resp[0..fallback_header.len], fallback_header);
        @memcpy(resp[fallback_header.len..][0..fallback_body.len], fallback_body);
        return fallback_header.len + fallback_body.len;
    }

    @memcpy(resp[0..header_bytes.len], header_bytes);
    @memcpy(resp[header_bytes.len..][0..body.len], body);
    return header_bytes.len + body.len;
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

fn grpcPayload(raw: []const u8, body: []const u8) ?[]const u8 {
    const requires_frame = headerContains(raw, "content-type", "application/grpc");
    if (body.len >= 5 and body[0] == 0) {
        const frame_len: usize = @intCast(std.mem.readInt(u32, body[1..][0..4], .big));
        if (frame_len == body.len - 5) return body[5..];
        if (requires_frame) return null;
    } else if (requires_frame) {
        return null;
    }
    return body;
}

const BinaryKeyValueRecord = struct {
    key: []const u8,
    value: []const u8,
};

fn nextBinaryKeyValue(body: []const u8, pos: *usize) ?BinaryKeyValueRecord {
    if (pos.* + 6 > body.len) return null;
    const key_len: usize = std.mem.readInt(u16, body[pos.*..][0..2], .little);
    const value_len: usize = std.mem.readInt(u32, body[pos.* + 2 ..][0..4], .little);
    pos.* += 6;
    const key_end = std.math.add(usize, pos.*, key_len) catch return null;
    const value_end = std.math.add(usize, key_end, value_len) catch return null;
    if (key_len == 0 or key_end > body.len or value_end > body.len) return null;
    const key = body[pos.*..key_end];
    const value = body[key_end..value_end];
    pos.* = value_end;
    return .{ .key = key, .value = value };
}

const BinaryKeyRecord = struct {
    key: []const u8,
};

fn nextBinaryKey(body: []const u8, pos: *usize) ?BinaryKeyRecord {
    if (pos.* + 2 > body.len) return null;
    const key_len: usize = std.mem.readInt(u16, body[pos.*..][0..2], .little);
    pos.* += 2;
    const key_end = std.math.add(usize, pos.*, key_len) catch return null;
    if (key_len == 0 or key_end > body.len) return null;
    const key = body[pos.*..key_end];
    pos.* = key_end;
    return .{ .key = key };
}

const KeyIter = struct {
    body: []const u8,
    pos: usize = 0,
    array_mode: bool = false,

    fn init(body: []const u8) KeyIter {
        const trimmed = std.mem.trim(u8, body, " \t\r\n");
        if (trimmed.len == 0) return .{ .body = trimmed };
        if (trimmed[0] == '{') {
            if (jsonValue(trimmed, "keys")) |keys| {
                if (keys.len > 0 and keys[0] == '[') {
                    return .{ .body = keys, .pos = 1, .array_mode = true };
                }
            }
        }
        if (trimmed[0] == '[') return .{ .body = trimmed, .pos = 1, .array_mode = true };
        return .{ .body = trimmed };
    }

    fn next(self: *KeyIter) ?[]const u8 {
        if (self.array_mode) return self.nextArray();
        return self.nextLine();
    }

    fn nextLine(self: *KeyIter) ?[]const u8 {
        while (self.pos < self.body.len) {
            const line_end = std.mem.indexOfScalarPos(u8, self.body, self.pos, '\n') orelse self.body.len;
            const raw_line = std.mem.trim(u8, self.body[self.pos..line_end], " \t\r");
            self.pos = if (line_end < self.body.len) line_end + 1 else line_end;
            if (raw_line.len == 0) continue;
            if (raw_line[0] == '{') return jsonStr(raw_line, "key") orelse continue;
            if (raw_line.len >= 2 and raw_line[0] == '"' and raw_line[raw_line.len - 1] == '"')
                return raw_line[1 .. raw_line.len - 1];
            return raw_line;
        }
        return null;
    }

    fn nextArray(self: *KeyIter) ?[]const u8 {
        while (self.pos < self.body.len) {
            while (self.pos < self.body.len and
                (self.body[self.pos] == ' ' or self.body[self.pos] == '\t' or
                    self.body[self.pos] == '\r' or self.body[self.pos] == '\n' or
                    self.body[self.pos] == ',')) : (self.pos += 1)
            {}
            if (self.pos >= self.body.len or self.body[self.pos] == ']') return null;

            if (self.body[self.pos] == '"') {
                const start = self.pos + 1;
                var i = start;
                while (i < self.body.len) : (i += 1) {
                    if (self.body[i] == '\\' and i + 1 < self.body.len) {
                        i += 1;
                        continue;
                    }
                    if (self.body[i] == '"') {
                        self.pos = i + 1;
                        return self.body[start..i];
                    }
                }
                self.pos = self.body.len;
                return null;
            }

            const start = self.pos;
            while (self.pos < self.body.len and self.body[self.pos] != ',' and self.body[self.pos] != ']') : (self.pos += 1) {}
            const raw_key = std.mem.trim(u8, self.body[start..self.pos], " \t\r\n");
            if (raw_key.len == 0) continue;
            return raw_key;
        }
        return null;
    }
};

const BulkLine = struct {
    key: []const u8,
    value: []const u8,
};

fn parseBulkLine(line: []const u8) ?BulkLine {
    if (parseBulkLineExact(line)) |parsed| return parsed;
    if (parseBulkLineFast(line)) |parsed| return parsed;
    const key = jsonStr(line, "key") orelse return null;
    const value = jsonValue(line, "value") orelse line;
    return .{ .key = key, .value = value };
}

fn parseStrictBulkLine(line: []const u8) ?BulkLine {
    if (parseBulkLineExact(line)) |parsed| return parsed;
    if (parseBulkLineFast(line)) |parsed| return parsed;
    const key = jsonStr(line, "key") orelse return null;
    const value = jsonValue(line, "value") orelse return null;
    return .{ .key = key, .value = value };
}

fn parseBulkLineExact(line: []const u8) ?BulkLine {
    const prefix = "{\"key\":\"";
    const middle = "\",\"value\":";
    if (line.len < prefix.len + middle.len + 1 or !std.mem.startsWith(u8, line, prefix)) return null;

    var key_end = prefix.len;
    while (key_end < line.len and line[key_end] != '"') : (key_end += 1) {
        if (line[key_end] == '\\') return null;
    }
    if (key_end >= line.len) return null;
    if (!std.mem.startsWith(u8, line[key_end..], middle)) return null;

    const value_start = key_end + middle.len;
    if (value_start >= line.len or line[line.len - 1] != '}') return null;
    return .{ .key = line[prefix.len..key_end], .value = line[value_start .. line.len - 1] };
}

fn parseBulkLineFast(line: []const u8) ?BulkLine {
    if (line.len < "{\"key\":\"\",\"value\":}".len or line[0] != '{') return null;
    var row_end = line.len;
    while (row_end > 0 and (line[row_end - 1] == ' ' or line[row_end - 1] == '\t' or line[row_end - 1] == '\r')) row_end -= 1;
    if (row_end == 0 or line[row_end - 1] != '}') return null;
    row_end -= 1;

    var i: usize = 1;
    skipJsonSpaces(line, &i, row_end);
    if (!std.mem.startsWith(u8, line[i..row_end], "\"key\"")) return null;
    i += "\"key\"".len;
    skipJsonSpaces(line, &i, row_end);
    if (i >= row_end or line[i] != ':') return null;
    i += 1;
    skipJsonSpaces(line, &i, row_end);
    const key = parseJsonStringToken(line, &i, row_end) orelse return null;
    skipJsonSpaces(line, &i, row_end);
    if (i >= row_end or line[i] != ',') return null;
    i += 1;
    skipJsonSpaces(line, &i, row_end);
    if (!std.mem.startsWith(u8, line[i..row_end], "\"value\"")) return null;
    i += "\"value\"".len;
    skipJsonSpaces(line, &i, row_end);
    if (i >= row_end or line[i] != ':') return null;
    i += 1;
    skipJsonSpaces(line, &i, row_end);
    if (i >= row_end) return null;

    var value_end = row_end;
    while (value_end > i and (line[value_end - 1] == ' ' or line[value_end - 1] == '\t')) value_end -= 1;
    if (value_end <= i) return null;
    return .{ .key = key, .value = line[i..value_end] };
}

fn skipJsonSpaces(data: []const u8, pos: *usize, limit: usize) void {
    while (pos.* < limit and (data[pos.*] == ' ' or data[pos.*] == '\t')) pos.* += 1;
}

fn parseJsonStringToken(data: []const u8, pos: *usize, limit: usize) ?[]const u8 {
    if (pos.* >= limit or data[pos.*] != '"') return null;
    pos.* += 1;
    const start = pos.*;
    while (pos.* < limit) : (pos.* += 1) {
        if (data[pos.*] == '\\' and pos.* + 1 < limit) {
            pos.* += 1;
            continue;
        }
        if (data[pos.*] == '"') {
            const end = pos.*;
            pos.* += 1;
            return data[start..end];
        }
    }
    return null;
}

const BatchUpdateLine = struct {
    key: []const u8 = "",
    value: []const u8 = "",
    line_len: usize = 0,
    valid: bool = false,
};

const BatchUpdateIter = struct {
    body: []const u8,
    pos: usize = 0,

    fn init(body: []const u8) BatchUpdateIter {
        return .{ .body = body };
    }

    fn next(self: *BatchUpdateIter) ?BatchUpdateLine {
        while (self.pos < self.body.len) {
            const line_end = std.mem.indexOfScalarPos(u8, self.body, self.pos, '\n') orelse self.body.len;
            const line = std.mem.trim(u8, self.body[self.pos..line_end], " \t\r");
            self.pos = if (line_end < self.body.len) line_end + 1 else line_end;
            if (line.len == 0) continue;

            const parsed = parseStrictBulkLine(line) orelse return .{ .line_len = line.len };
            return .{
                .key = parsed.key,
                .value = parsed.value,
                .line_len = line.len,
                .valid = true,
            };
        }
        return null;
    }
};

const JoinRequest = struct {
    key: []const u8,
    target_collection: []const u8,
    field: []const u8,

    fn parse(body: []const u8) ?JoinRequest {
        const key = jsonStr(body, "key") orelse return null;
        const target_collection = jsonStr(body, "target_collection") orelse return null;
        const field = jsonStr(body, "field") orelse return null;
        if (key.len == 0 or target_collection.len == 0 or field.len == 0) return null;
        return .{
            .key = key,
            .target_collection = target_collection,
            .field = field,
        };
    }
};

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
            if (json[i] == '\\' and i + 1 < json.len) {
                i += 1;
                continue;
            }
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
            if (json[i] == '\\' and in_str) {
                i += 1;
                continue;
            }
            if (json[i] == '"') {
                in_str = !in_str;
                continue;
            }
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
fn wsReadExact(conn: compat.net.Server.Connection, buf: []u8) !void {
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
                if (al != bl) {
                    match = false;
                    break;
                }
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
                if (al != bl) {
                    vmatch = false;
                    break;
                }
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
                if (al != bl) {
                    match = false;
                    break;
                }
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
fn handleWebSocket(srv: *Server, conn: compat.net.Server.Connection, initial: []const u8) !void {
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
    const resp_len = std.fmt.bufPrint(&resp_buf, "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: {s}\r\n\r\n", .{accept}) catch return error.FormatFailed;
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
        var mask: [4]u8 = .{ 0, 0, 0, 0 };
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
    const http_len = std.fmt.bufPrint(http_buf, "{s} HTTP/1.1\r\nContent-Length: {d}\r\n\r\n{s}", .{ req_line, body.len, body }) catch return "{\"error\":\"request too large\"}";

    const resp_len = dispatch(srv, http_len, std.heap.page_allocator);
    const resp = getRespBuf()[0..resp_len];

    // Strip HTTP headers from response, return just the body
    if (std.mem.indexOf(u8, resp, "\r\n\r\n")) |p| return resp[p + 4 ..];
    return resp;
}

fn wsWriteText(conn: compat.net.Server.Connection, payload: []const u8) !void {
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

fn wsWriteClose(conn: compat.net.Server.Connection, code: u16) void {
    var frame: [4]u8 = undefined;
    frame[0] = 0x88; // FIN + close
    frame[1] = 2; // payload = 2 bytes (status code)
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
    try std.testing.expectEqualStrings("header-tenant", requestTenant(raw, "tenant=query-tenant", null));
}

test "request tenant uses bound tenant for non-admin auth" {
    const raw = "GET /db/users?tenant=query-tenant HTTP/1.1\r\nX-Tenant-Id: header-tenant\r\n\r\n";
    const tenant = "bound-tenant";
    var ctx = auth.AuthContext{
        .perm = .read_write,
        .tenant_id = [_]u8{0} ** 64,
        .tenant_id_len = @intCast(tenant.len),
    };
    @memcpy(ctx.tenant_id[0..tenant.len], tenant);

    try std.testing.expectEqualStrings(tenant, requestTenant(raw, "tenant=query-tenant", &ctx));
    try std.testing.expectEqualStrings(tenant, requestTenantFilter("tenant=query-tenant", &ctx).?);
    try std.testing.expectEqualStrings(tenant, requestBodyTenant("{\"tenant\":\"body-tenant\"}", &ctx).?);
}

test "admin auth can select request tenant" {
    const raw = "GET /db/users?tenant=query-tenant HTTP/1.1\r\nX-Tenant-Id: header-tenant\r\n\r\n";
    var ctx = auth.AuthContext{
        .perm = .admin,
        .tenant_id = [_]u8{0} ** 64,
        .tenant_id_len = 0,
    };

    try std.testing.expectEqualStrings("header-tenant", requestTenant(raw, "tenant=query-tenant", &ctx));
}

test "respond writes bodies larger than format scratch buffer" {
    var bufs: ConnBufs = undefined;
    tl_bufs = &bufs;
    defer tl_bufs = null;

    var body: [9000]u8 = undefined;
    @memset(&body, 'a');

    const n = respond(200, "OK", body[0..]);
    try std.testing.expect(n > body.len);
    try std.testing.expect(std.mem.indexOf(u8, bufs.resp[0..n], "\r\n\r\n") != null);
    try std.testing.expect(std.mem.endsWith(u8, bufs.resp[0..n], body[0..]));
}

test "read-only auth cannot write basic db methods" {
    var ctx = auth.AuthContext{
        .perm = .read_only,
        .tenant_id = [_]u8{0} ** 64,
        .tenant_id_len = 0,
    };

    try std.testing.expect(canWriteDb(null));
    try std.testing.expect(!canWriteDb(&ctx));
    try std.testing.expect(isBasicDbWriteMethod("POST"));
    try std.testing.expect(isBasicDbWriteMethod("PUT"));
    try std.testing.expect(isBasicDbWriteMethod("DELETE"));
    try std.testing.expect(!isBasicDbWriteMethod("GET"));

    ctx.perm = .read_write;
    try std.testing.expect(canWriteDb(&ctx));
}

test "bulk line parser fast path and generic fallback" {
    const fast = parseBulkLine("{\"key\":\"u1\",\"value\":{\"name\":\"alice\",\"tags\":[1,2]}}").?;
    try std.testing.expectEqualStrings("u1", fast.key);
    try std.testing.expectEqualStrings("{\"name\":\"alice\",\"tags\":[1,2]}", fast.value);

    const fallback = parseBulkLine("{\"value\":{\"name\":\"bob\"},\"key\":\"u2\"}").?;
    try std.testing.expectEqualStrings("u2", fallback.key);
    try std.testing.expectEqualStrings("{\"name\":\"bob\"}", fallback.value);
}

test "bulk insert target recognizes binary bulk endpoint" {
    try std.testing.expect(isBulkInsertTarget("POST", "/db/users/bulk"));
    try std.testing.expect(isBulkInsertTarget("POST", "/db/users/bulk_binary"));
    try std.testing.expect(!isBulkInsertTarget("GET", "/db/users/bulk_binary"));
    try std.testing.expect(!isBulkInsertTarget("POST", "/db/users/batch_update"));
}

test "grpc bridge target parser and admission detection" {
    const target = parseGrpcTarget("/grpc/users/BulkInsert").?;
    try std.testing.expectEqualStrings("users", target.col_name);
    try std.testing.expectEqualStrings("BulkInsert", target.rpc);

    try std.testing.expect(isGrpcBulkMutation("POST", "/grpc/users/BulkInsert"));
    try std.testing.expect(isGrpcBulkMutation("POST", "/grpc/users/BulkUpdate"));
    try std.testing.expect(isGrpcBulkMutation("POST", "/grpc/users/BulkUpsert"));
    try std.testing.expect(isGrpcBulkMutation("POST", "/grpc/users/BulkDelete"));
    try std.testing.expect(!isGrpcBulkMutation("GET", "/grpc/users/BulkInsert"));
    try std.testing.expect(!isGrpcBulkMutation("POST", "/grpc/users/PointGet"));
    try std.testing.expect(!isGrpcBulkMutation("POST", "/grpc/users"));
}

test "grpc payload accepts raw binary or one uncompressed grpc frame" {
    const raw_plain = "POST /grpc/users/BulkInsert HTTP/1.1\r\nContent-Length: 3\r\n\r\nabc";
    try std.testing.expectEqualStrings("abc", grpcPayload(raw_plain, "abc").?);

    const raw_grpc = "POST /grpc/users/BulkInsert HTTP/1.1\r\nContent-Type: application/grpc\r\nContent-Length: 8\r\n\r\n";
    const framed = [_]u8{ 0, 0, 0, 0, 3, 'a', 'b', 'c' };
    try std.testing.expectEqualStrings("abc", grpcPayload(raw_grpc, framed[0..]).?);

    const bad_framed = [_]u8{ 0, 0, 0, 0, 4, 'a', 'b', 'c' };
    try std.testing.expect(grpcPayload(raw_grpc, bad_framed[0..]) == null);
    try std.testing.expect(grpcPayload(raw_grpc, "abc") == null);
}

test "binary bulk record parsers walk key value and key-only bodies" {
    const kv = [_]u8{
        2, 0, 7, 0, 0, 0, 'k', '1', '{', '"', 'n', '"', ':', '1', '}',
        2, 0, 7, 0, 0, 0, 'k', '2', '{', '"', 'n', '"', ':', '2', '}',
    };
    var kv_pos: usize = 0;
    const first = nextBinaryKeyValue(kv[0..], &kv_pos).?;
    try std.testing.expectEqualStrings("k1", first.key);
    try std.testing.expectEqualStrings("{\"n\":1}", first.value);
    const second = nextBinaryKeyValue(kv[0..], &kv_pos).?;
    try std.testing.expectEqualStrings("k2", second.key);
    try std.testing.expectEqualStrings("{\"n\":2}", second.value);
    try std.testing.expect(kv_pos == kv.len);
    try std.testing.expect(nextBinaryKeyValue(kv[0 .. kv.len - 1], &kv_pos) == null);

    const keys = [_]u8{ 2, 0, 'k', '1', 2, 0, 'k', '2' };
    var key_pos: usize = 0;
    try std.testing.expectEqualStrings("k1", nextBinaryKey(keys[0..], &key_pos).?.key);
    try std.testing.expectEqualStrings("k2", nextBinaryKey(keys[0..], &key_pos).?.key);
    try std.testing.expect(key_pos == keys.len);
}

test "bulk memory estimate includes body and bounded chunk workspace" {
    const small = estimateBulkMemoryBytes(1024);
    const large = estimateBulkMemoryBytes(MAX_BULK);
    try std.testing.expect(small > 1024);
    try std.testing.expect(large > MAX_BULK);
    try std.testing.expect(large < MAX_BULK + 40 * 1024 * 1024);
}

test "bulk memory admission enforces tenant and global limits" {
    const alloc = std.testing.allocator;
    var fake_db: Database = undefined;
    var srv = Server.init(alloc, &fake_db, 0);
    defer srv.deinit();

    const request_bytes = estimateBulkMemoryBytes(1024 * 1024);
    const global_limit = request_bytes * 2 + 1024;
    const tenant_limit = request_bytes + 512;

    var r1 = try srv.acquireBulkMemoryWithLimits("tenant-a", request_bytes, global_limit, tenant_limit);
    try std.testing.expectError(
        error.BulkMemoryLimitExceeded,
        srv.acquireBulkMemoryWithLimits("tenant-a", request_bytes, global_limit, tenant_limit),
    );

    var r2 = try srv.acquireBulkMemoryWithLimits("tenant-b", request_bytes, global_limit, tenant_limit);
    try std.testing.expectError(
        error.BulkMemoryLimitExceeded,
        srv.acquireBulkMemoryWithLimits("tenant-c", request_bytes, global_limit, tenant_limit),
    );

    r1.release();
    var r3 = try srv.acquireBulkMemoryWithLimits("tenant-c", request_bytes, global_limit, tenant_limit);
    r2.release();
    r3.release();

    try std.testing.expectEqual(@as(usize, 0), srv.bulk_inflight_bytes);
    try std.testing.expectEqual(@as(u32, 0), srv.bulk_inflight_requests);
    try std.testing.expectEqual(@as(u32, 0), srv.bulk_tenant_usage.count());
}

test "KeyIter reads JSON arrays and keyed newline bodies" {
    var array_iter = KeyIter.init("{\"keys\":[\"1\",\"2\",\"3\"]}");
    try std.testing.expectEqualStrings("1", array_iter.next().?);
    try std.testing.expectEqualStrings("2", array_iter.next().?);
    try std.testing.expectEqualStrings("3", array_iter.next().?);
    try std.testing.expect(array_iter.next() == null);

    var line_iter = KeyIter.init("{\"key\":\"a\"}\nplain\n\"quoted\"\n");
    try std.testing.expectEqualStrings("a", line_iter.next().?);
    try std.testing.expectEqualStrings("plain", line_iter.next().?);
    try std.testing.expectEqualStrings("quoted", line_iter.next().?);
    try std.testing.expect(line_iter.next() == null);
}

test "BatchUpdateIter reads NDJSON key value lines and flags invalid lines" {
    var iter = BatchUpdateIter.init(
        "{\"key\":\"a\",\"value\":{\"n\":1}}\n" ++
            "{\"key\":\"b\",\"value\":\"text\"}\n" ++
            "{\"key\":\"missing-value\"}\n",
    );

    const first = iter.next().?;
    try std.testing.expect(first.valid);
    try std.testing.expectEqualStrings("a", first.key);
    try std.testing.expectEqualStrings("{\"n\":1}", first.value);

    const second = iter.next().?;
    try std.testing.expect(second.valid);
    try std.testing.expectEqualStrings("b", second.key);
    try std.testing.expectEqualStrings("\"text\"", second.value);

    const third = iter.next().?;
    try std.testing.expect(!third.valid);
    try std.testing.expect(iter.next() == null);
}

test "JoinRequest parses required join fields" {
    const req = JoinRequest.parse("{\"key\":\"customer-1\",\"target_collection\":\"orders\",\"field\":\"order_ids\"}").?;
    try std.testing.expectEqualStrings("customer-1", req.key);
    try std.testing.expectEqualStrings("orders", req.target_collection);
    try std.testing.expectEqualStrings("order_ids", req.field);

    try std.testing.expect(JoinRequest.parse("{\"key\":\"customer-1\",\"field\":\"order_ids\"}") == null);
}

test "batchDocsCompact detects compact response query options" {
    try std.testing.expect(batchDocsCompact("mode=count"));
    try std.testing.expect(batchDocsCompact("mode=compact&tenant=t1"));
    try std.testing.expect(batchDocsCompact("tenant=t1&docs=0"));
    try std.testing.expect(!batchDocsCompact("mode=full"));
    try std.testing.expect(!batchDocsCompact(""));
}
