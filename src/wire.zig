/// TurboDB Binary Wire Protocol Server
///
/// Frame: [len:u32 BE][op:u8][payload...]
/// len includes the 5-byte header.
///
/// Ops: INSERT=0x01 GET=0x02 UPDATE=0x03 DELETE=0x04 SCAN=0x05 PING=0x06 BATCH=0x07
/// Status: OK=0x00 NOT_FOUND=0x01 ERROR=0x02
///
/// Batch v1 request payload: [count:u16 LE] repeated [op:u8][payload_len:u32 LE][payload].
/// Batch v1 response payload: [status:u8][count:u16 LE] repeated [op:u8][status:u8][payload_len:u32 LE][payload].
///
/// Wire v2 rides inside the same outer frame with op=0x20 so v1 clients can
/// keep using the same listener. V2 payload:
///   request:  [ver:u8=2][flags:u8][request_id:u64 LE][op:u8][reserved:u8][body_len:u32 LE][body]
///   response: [ver:u8=2][flags:u8][request_id:u64 LE][op:u8][status:u8][body_len:u32 LE][body]
const std = @import("std");
const compat = @import("compat");
const activity = @import("activity.zig");
const auth = @import("auth.zig");
const collection_mod = @import("collection.zig");
const Database = collection_mod.Database;
const Collection = collection_mod.Collection;

const OP_INSERT: u8 = 0x01;
const OP_GET: u8 = 0x02;
const OP_UPDATE: u8 = 0x03;
const OP_DELETE: u8 = 0x04;
const OP_SCAN: u8 = 0x05;
const OP_PING: u8 = 0x06;
const OP_BATCH: u8 = 0x07;
const OP_WIRE2: u8 = 0x20;

pub const WIRE2_VERSION: u8 = 2;
pub const WIRE2_OP_HELLO: u8 = 0x00;
pub const WIRE2_OP_PING: u8 = 0x01;
pub const WIRE2_OP_GET: u8 = 0x02;
pub const WIRE2_OP_PUT: u8 = 0x03;
pub const WIRE2_OP_INSERT: u8 = 0x04;
pub const WIRE2_OP_DELETE: u8 = 0x05;
pub const WIRE2_OP_BATCH_GET: u8 = 0x06;
pub const WIRE2_OP_BULK_INSERT: u8 = 0x07;
pub const WIRE2_OP_BULK_UPSERT: u8 = 0x08;
pub const WIRE2_OP_BULK_DELETE: u8 = 0x09;
pub const WIRE2_OP_AUTH: u8 = 0x0A;

const STATUS_OK: u8 = 0x00;
const STATUS_NOT_FOUND: u8 = 0x01;
const STATUS_ERROR: u8 = 0x02;
const STATUS_UNAUTHORIZED: u8 = 0x03;
const STATUS_FORBIDDEN: u8 = 0x04;
const STATUS_TOO_LARGE: u8 = 0x05;

const HDR: usize = 5;
const RD_BUF: usize = 65536;
const WR_BUF: usize = 4 * 1024 * 1024;
const MAX_FRAME: usize = 64 * 1024 * 1024 + HDR;
const BATCH_REQ_HDR: usize = 2;
const BATCH_ITEM_HDR: usize = 5;
const BATCH_RESP_HDR: usize = HDR + 1 + 2;
const BATCH_ITEM_RESP_HDR: usize = 1 + 1 + 4;
const WIRE2_HDR: usize = 16;
const WIRE2_FEATURE_AUTH: u32 = 1 << 0;
const WIRE2_FEATURE_PIPELINING: u32 = 1 << 1;
const WIRE2_FEATURE_BATCH_GET: u32 = 1 << 2;
const WIRE2_FEATURE_BULK_WRITE: u32 = 1 << 3;
const WIRE2_BULK_INSERT_CHUNK_ROWS: usize = 16384;
const WIRE2_BULK_INSERT_CHUNK_BYTES: usize = 16 * 1024 * 1024;
const CONN_THREAD_STACK_SIZE = 1024 * 1024;

pub const WireServer = struct {
    db: *Database,
    port: u16,
    running: std.atomic.Value(bool),
    activity: activity.ActivityTracker,
    op_mu: compat.RwLock,

    pub fn init(db: *Database, port: u16) WireServer {
        return .{
            .db = db,
            .port = port,
            .running = std.atomic.Value(bool).init(false),
            .activity = activity.ActivityTracker.init(),
            .op_mu = .{},
        };
    }

    pub fn run(self: *WireServer) !void {
        const addr = try compat.net.Address.parseIp("0.0.0.0", self.port);
        var listener = try addr.listen(.{ .reuse_address = true, .kernel_backlog = 1024 });
        defer listener.deinit();
        self.running.store(true, .release);
        std.log.info("TurboDB wire protocol on :{d}", .{self.port});
        while (self.running.load(.acquire)) {
            const conn = listener.accept() catch continue;
            const t = std.Thread.spawn(.{ .stack_size = CONN_THREAD_STACK_SIZE }, handleConn, .{ self, conn }) catch continue;
            t.detach();
        }
    }

    pub fn runUnix(self: *WireServer, path: []const u8) !void {
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

        // Construct sockaddr_un
        var addr: extern struct { family: u16, path: [104]u8 } = .{ .family = 1, .path = undefined };
        @memset(&addr.path, 0);
        if (path.len >= addr.path.len) return error.PathTooLong;
        @memcpy(addr.path[0..path.len], path);

        if (std.c.bind(fd, @ptrCast(&addr), @sizeOf(@TypeOf(addr))) != 0) return error.BindError;
        if (std.c.listen(fd, 1024) != 0) return error.ListenError;

        self.running.store(true, .release);
        std.log.info("TurboDB wire protocol on unix:{s}", .{path});

        while (self.running.load(.acquire)) {
            const client_fd = std.c.accept(fd, null, null);
            if (client_fd < 0) continue;
            const stream = compat.net.Stream{ .handle = client_fd };
            const conn = compat.net.Server.Connection{ .stream = stream, .address = compat.net.Address.initUnix(path) catch continue };
            const t = std.Thread.spawn(.{ .stack_size = CONN_THREAD_STACK_SIZE }, handleConn, .{ self, conn }) catch continue;
            t.detach();
        }
    }

    pub fn stop(self: *WireServer) void {
        self.running.store(false, .release);
    }
};

const Bufs = struct { rd: [RD_BUF]u8, wr: [WR_BUF]u8 };

const WireConnState = struct {
    authenticated: bool = false,
    auth_ctx: ?auth.AuthContext = null,
};

fn handleConn(srv: *WireServer, conn: compat.net.Server.Connection) void {
    defer conn.stream.close();
    const bufs = std.heap.page_allocator.create(Bufs) catch return;
    defer std.heap.page_allocator.destroy(bufs);
    var state = WireConnState{};

    // Pre-resolve the most common collection for the fast path
    var cached_col: ?*collection_mod.Collection = null;
    var cached_col_name: [128]u8 = undefined;
    var cached_col_len: usize = 0;

    var rp: usize = 0;

    while (true) {
        const n = conn.stream.read(bufs.rd[rp..]) catch return;
        if (n == 0) return;
        rp += n;

        var consumed: usize = 0;
        while (consumed + HDR <= rp) {
            const flen = rdU32BE(bufs.rd[consumed..]);
            if (flen < HDR or flen > MAX_FRAME) return;

            if (flen > RD_BUF) {
                if (consumed != 0) break;
                const big = std.heap.page_allocator.alloc(u8, flen) catch return;
                defer std.heap.page_allocator.free(big);

                @memcpy(big[0..rp], bufs.rd[0..rp]);
                var have = rp;
                while (have < flen) {
                    const got = conn.stream.read(big[have..flen]) catch return;
                    if (got == 0) return;
                    have += got;
                }

                const wn = processFrame(srv, &state, big[0..flen], &bufs.wr, &cached_col, &cached_col_name, &cached_col_len);
                conn.stream.writeAll(bufs.wr[0..wn]) catch return;
                rp = 0;
                consumed = 0;
                break;
            }

            if (consumed + flen > rp) break;
            const frame = bufs.rd[consumed .. consumed + flen];
            const wn = processFrame(srv, &state, frame, &bufs.wr, &cached_col, &cached_col_name, &cached_col_len);
            conn.stream.writeAll(bufs.wr[0..wn]) catch return;
            consumed += flen;
        }
        if (consumed > 0) {
            if (consumed < rp) std.mem.copyForwards(u8, bufs.rd[0..], bufs.rd[consumed..rp]);
            rp -= consumed;
        }
    }
}

fn processFrame(
    srv: *WireServer,
    state: *WireConnState,
    frame: []const u8,
    w: *[WR_BUF]u8,
    cached_col: *?*collection_mod.Collection,
    cached_name: *[128]u8,
    cached_len: *usize,
) usize {
    const op = frame[4];
    const payload = frame[HDR..];

    if (op == OP_WIRE2) {
        srv.activity.recordQuery();
        return dispatchWire2(srv, state, payload, w);
    }

    // ── FAST PATH: inline GET with zero-copy ──
    if (op == OP_GET) {
        srv.activity.recordQuery();
        srv.op_mu.lockShared();
        defer srv.op_mu.unlockShared();
        return fastGet(srv, payload, w, cached_col, cached_name, cached_len);
    }

    // Invalidate collection cache on writes.
    if (op == OP_INSERT or op == OP_UPDATE or op == OP_DELETE or op == OP_BATCH) {
        cached_col.* = null;
    }
    srv.activity.recordQuery();
    const lock_mutation = wireOpMutates(op);
    if (lock_mutation) {
        srv.op_mu.lock();
    } else if (wireOpReadsStorage(op)) {
        srv.op_mu.lockShared();
    }
    defer {
        if (lock_mutation) srv.op_mu.unlock() else if (wireOpReadsStorage(op)) srv.op_mu.unlockShared();
    }
    return dispatch(srv, op, payload, w);
}

fn wireOpMutates(op: u8) bool {
    return op == OP_INSERT or op == OP_UPDATE or op == OP_DELETE or op == OP_BATCH;
}

fn wireOpReadsStorage(op: u8) bool {
    return op == OP_SCAN;
}

/// Inlined GET: collection lookup is cached, response writes mmap'd bytes directly.
fn fastGet(
    srv: *WireServer,
    p: []const u8,
    w: *[WR_BUF]u8,
    cached_col: *?*collection_mod.Collection,
    cached_name: *[128]u8,
    cached_len: *usize,
) usize {
    const a = parseKey(p) orelse return errResp(w, OP_GET, STATUS_ERROR);

    // Fast collection lookup: skip mutex if same collection as last call
    const col = blk: {
        if (cached_col.*) |cc| {
            if (cached_len.* == a.col.len and std.mem.eql(u8, cached_name[0..cached_len.*], a.col)) {
                break :blk cc;
            }
        }
        const c = lookupCollection(srv, a.col) catch return errResp(w, OP_GET, STATUS_ERROR);
        @memcpy(cached_name[0..a.col.len], a.col);
        cached_len.* = a.col.len;
        cached_col.* = c;
        break :blk c;
    };

    const d = col.get(a.key) orelse return errResp(w, OP_GET, STATUS_NOT_FOUND);

    // Write response: [len:4][op:1][status:1][doc_id:8][ver:1][val_len:4][val:N]
    const rlen = HDR + 1 + 8 + 1 + 4 + d.value.len;
    if (rlen > WR_BUF) return errResp(w, OP_GET, STATUS_ERROR);
    wrU32BE(w, @intCast(rlen));
    w[4] = OP_GET;
    w[5] = STATUS_OK;
    wrU64LE(w[6..14], d.header.doc_id);
    w[14] = d.header.version;
    wrU32LE(w[15..19], @intCast(d.value.len));
    if (d.value.len > 0) @memcpy(w[19..][0..d.value.len], d.value);
    return rlen;
}

fn dispatch(srv: *WireServer, op: u8, p: []const u8, w: *[WR_BUF]u8) usize {
    return switch (op) {
        OP_INSERT => doInsert(srv, p, w),
        OP_GET => doGet(srv, p, w),
        OP_UPDATE => doUpdate(srv, p, w),
        OP_DELETE => doDelete(srv, p, w),
        OP_SCAN => doScan(srv, p, w),
        OP_PING => doPing(w),
        OP_BATCH => doBatch(srv, p, w),
        else => errResp(w, 0xFF, STATUS_ERROR),
    };
}

// ── INSERT ──────────────────────────────────────────────────────────────────

fn doInsert(srv: *WireServer, p: []const u8, w: *[WR_BUF]u8) usize {
    const r = execInsertPayload(srv, p, w[6..]);
    if (r.status != STATUS_OK) return errResp(w, OP_INSERT, r.status);
    wrU32BE(w, @intCast(HDR + 1 + r.payload_len));
    w[4] = OP_INSERT;
    w[5] = STATUS_OK;
    return HDR + 1 + r.payload_len;
}

fn execInsertPayload(srv: *WireServer, p: []const u8, out: []u8) ItemResult {
    const a = parseKV(p) orelse return .{ .status = STATUS_ERROR, .payload_len = 0 };
    if (out.len < 8) return .{ .status = STATUS_ERROR, .payload_len = 0 };
    const ref = resolveCollectionRef(a.col);
    srv.db.recordTenantOperation(ref.tenant_id) catch return .{ .status = STATUS_ERROR, .payload_len = 0 };
    srv.db.ensureTenantStorageAvailable(ref.tenant_id, a.val.len) catch return .{ .status = STATUS_ERROR, .payload_len = 0 };
    const col = srv.db.collectionForTenant(ref.tenant_id, ref.collection_name) catch return .{ .status = STATUS_ERROR, .payload_len = 0 };
    const doc_id = col.insert(a.key, a.val) catch return .{ .status = STATUS_ERROR, .payload_len = 0 };
    wrU64LE(out[0..8], doc_id);
    return .{ .status = STATUS_OK, .payload_len = 8 };
}

// ── GET ─────────────────────────────────────────────────────────────────────

fn doGet(srv: *WireServer, p: []const u8, w: *[WR_BUF]u8) usize {
    const r = execGetPayload(srv, p, w[6..]);
    if (r.status != STATUS_OK) return errResp(w, OP_GET, r.status);
    wrU32BE(w, @intCast(HDR + 1 + r.payload_len));
    w[4] = OP_GET;
    w[5] = STATUS_OK;
    return HDR + 1 + r.payload_len;
}

fn execGetPayload(srv: *WireServer, p: []const u8, out: []u8) ItemResult {
    const a = parseKey(p) orelse return .{ .status = STATUS_ERROR, .payload_len = 0 };
    const ref = resolveCollectionRef(a.col);
    srv.db.recordTenantOperation(ref.tenant_id) catch return .{ .status = STATUS_ERROR, .payload_len = 0 };
    const col = srv.db.collectionForTenant(ref.tenant_id, ref.collection_name) catch return .{ .status = STATUS_ERROR, .payload_len = 0 };
    const d = col.get(a.key) orelse return .{ .status = STATUS_NOT_FOUND, .payload_len = 0 };
    // [len:4][op:1][status:1][doc_id:8][ver:1][val_len:4][val:N]
    const payload_len = 8 + 1 + 4 + d.value.len;
    if (payload_len > out.len) return .{ .status = STATUS_ERROR, .payload_len = 0 };
    wrU64LE(out[0..8], d.header.doc_id);
    out[8] = d.header.version;
    wrU32LE(out[9..13], @intCast(d.value.len));
    if (d.value.len > 0) @memcpy(out[13..][0..d.value.len], d.value);
    return .{ .status = STATUS_OK, .payload_len = payload_len };
}

// ── UPDATE ──────────────────────────────────────────────────────────────────

fn doUpdate(srv: *WireServer, p: []const u8, w: *[WR_BUF]u8) usize {
    const r = execUpdatePayload(srv, p);
    if (r.status != STATUS_OK) return errResp(w, OP_UPDATE, r.status);
    wrU32BE(w, HDR + 1);
    w[4] = OP_UPDATE;
    w[5] = STATUS_OK;
    return HDR + 1;
}

fn execUpdatePayload(srv: *WireServer, p: []const u8) ItemResult {
    const a = parseKV(p) orelse return .{ .status = STATUS_ERROR, .payload_len = 0 };
    const ref = resolveCollectionRef(a.col);
    srv.db.recordTenantOperation(ref.tenant_id) catch return .{ .status = STATUS_ERROR, .payload_len = 0 };
    srv.db.ensureTenantStorageAvailable(ref.tenant_id, a.val.len) catch return .{ .status = STATUS_ERROR, .payload_len = 0 };
    const col = srv.db.collectionForTenant(ref.tenant_id, ref.collection_name) catch return .{ .status = STATUS_ERROR, .payload_len = 0 };
    const ok = col.update(a.key, a.val) catch return .{ .status = STATUS_ERROR, .payload_len = 0 };
    if (!ok) return .{ .status = STATUS_NOT_FOUND, .payload_len = 0 };
    return .{ .status = STATUS_OK, .payload_len = 0 };
}

// ── DELETE ───────────────────────────────────────────────────────────────────

fn doDelete(srv: *WireServer, p: []const u8, w: *[WR_BUF]u8) usize {
    const r = execDeletePayload(srv, p);
    if (r.status != STATUS_OK) return errResp(w, OP_DELETE, r.status);
    wrU32BE(w, HDR + 1);
    w[4] = OP_DELETE;
    w[5] = STATUS_OK;
    return HDR + 1;
}

fn execDeletePayload(srv: *WireServer, p: []const u8) ItemResult {
    const a = parseKey(p) orelse return .{ .status = STATUS_ERROR, .payload_len = 0 };
    const ref = resolveCollectionRef(a.col);
    srv.db.recordTenantOperation(ref.tenant_id) catch return .{ .status = STATUS_ERROR, .payload_len = 0 };
    const col = srv.db.collectionForTenant(ref.tenant_id, ref.collection_name) catch return .{ .status = STATUS_ERROR, .payload_len = 0 };
    const ok = col.delete(a.key) catch return .{ .status = STATUS_ERROR, .payload_len = 0 };
    if (!ok) return .{ .status = STATUS_NOT_FOUND, .payload_len = 0 };
    return .{ .status = STATUS_OK, .payload_len = 0 };
}

// ── BATCH ──────────────────────────────────────────────────────────────────

const ItemResult = struct { status: u8, payload_len: usize };
const BatchEnvelope = struct { count: u16 };

fn doBatch(srv: *WireServer, p: []const u8, w: *[WR_BUF]u8) usize {
    const env = validateBatchEnvelope(p) orelse return errResp(w, OP_BATCH, STATUS_ERROR);

    if (doInsertOnlyBatch(srv, p, env, w)) |written| return written;

    var req_pos: usize = BATCH_REQ_HDR;
    var resp_pos: usize = BATCH_RESP_HDR;
    w[4] = OP_BATCH;
    w[5] = STATUS_OK;
    wrU16LE(w[6..8], env.count);

    for (0..env.count) |idx| {
        const op = p[req_pos];
        const payload_len: usize = rdU32LE(p[req_pos + 1 ..][0..4]);
        const payload = p[req_pos + BATCH_ITEM_HDR ..][0..payload_len];

        const remaining_items = @as(usize, env.count) - idx - 1;
        const reserved = remaining_items * BATCH_ITEM_RESP_HDR;
        const item_payload_cap = WR_BUF - resp_pos - BATCH_ITEM_RESP_HDR - reserved;
        const item_payload_out = w[resp_pos + BATCH_ITEM_RESP_HDR ..][0..item_payload_cap];

        w[resp_pos] = op;
        w[resp_pos + 1] = STATUS_ERROR;
        wrU32LE(w[resp_pos + 2 ..][0..4], 0);

        const result = execBatchItem(srv, op, payload, item_payload_out);
        w[resp_pos + 1] = result.status;
        wrU32LE(w[resp_pos + 2 ..][0..4], @intCast(result.payload_len));
        resp_pos += BATCH_ITEM_RESP_HDR + result.payload_len;
        req_pos += BATCH_ITEM_HDR + payload_len;
    }

    wrU32BE(w, @intCast(resp_pos));
    return resp_pos;
}

fn doInsertOnlyBatch(srv: *WireServer, p: []const u8, env: BatchEnvelope, w: *[WR_BUF]u8) ?usize {
    if (env.count == 0) return null;

    var rows: std.ArrayList(Collection.BulkInsertRow) = .empty;
    defer rows.deinit(srv.db.alloc);
    rows.ensureTotalCapacity(srv.db.alloc, env.count) catch return null;

    var req_pos: usize = BATCH_REQ_HDR;
    var batch_col: []const u8 = "";
    var total_value_bytes: usize = 0;
    for (0..env.count) |idx| {
        const op = p[req_pos];
        if (op != OP_INSERT) return null;
        const payload_len: usize = rdU32LE(p[req_pos + 1 ..][0..4]);
        const payload = p[req_pos + BATCH_ITEM_HDR ..][0..payload_len];
        const kv = parseKV(payload) orelse return null;

        if (idx == 0) {
            batch_col = kv.col;
        } else if (!std.mem.eql(u8, batch_col, kv.col)) {
            return null;
        }

        if (kv.key.len == 0 or kv.key.len > 1024 or kv.val.len > 64 * 1024 * 1024) return null;
        total_value_bytes = std.math.add(usize, total_value_bytes, kv.val.len) catch return null;
        rows.appendAssumeCapacity(.{
            .key = kv.key,
            .value = kv.val,
            .line_len = kv.key.len + kv.val.len,
        });
        req_pos += BATCH_ITEM_HDR + payload_len;
    }

    const ref = resolveCollectionRef(batch_col);
    srv.db.recordTenantOperation(ref.tenant_id) catch return allBatchItemsError(w, env.count);
    srv.db.ensureTenantStorageAvailable(ref.tenant_id, total_value_bytes) catch return allBatchItemsError(w, env.count);
    const col = srv.db.collectionForTenant(ref.tenant_id, ref.collection_name) catch return allBatchItemsError(w, env.count);
    const result = col.insertBulk(rows.items) catch return allBatchItemsError(w, env.count);
    if (result.errors != 0 or result.inserted != env.count) return allBatchItemsError(w, env.count);

    w[4] = OP_BATCH;
    w[5] = STATUS_OK;
    wrU16LE(w[6..8], env.count);
    var resp_pos: usize = BATCH_RESP_HDR;
    for (0..env.count) |i| {
        w[resp_pos] = OP_INSERT;
        w[resp_pos + 1] = STATUS_OK;
        wrU32LE(w[resp_pos + 2 ..][0..4], 8);
        wrU64LE(w[resp_pos + BATCH_ITEM_RESP_HDR ..][0..8], result.first_doc_id + @as(u64, @intCast(i)));
        resp_pos += BATCH_ITEM_RESP_HDR + 8;
    }
    wrU32BE(w, @intCast(resp_pos));
    return resp_pos;
}

fn allBatchItemsError(w: *[WR_BUF]u8, count: u16) usize {
    w[4] = OP_BATCH;
    w[5] = STATUS_OK;
    wrU16LE(w[6..8], count);
    var resp_pos: usize = BATCH_RESP_HDR;
    for (0..count) |_| {
        w[resp_pos] = OP_INSERT;
        w[resp_pos + 1] = STATUS_ERROR;
        wrU32LE(w[resp_pos + 2 ..][0..4], 0);
        resp_pos += BATCH_ITEM_RESP_HDR;
    }
    wrU32BE(w, @intCast(resp_pos));
    return resp_pos;
}

fn execBatchItem(srv: *WireServer, op: u8, p: []const u8, out: []u8) ItemResult {
    return switch (op) {
        OP_INSERT => execInsertPayload(srv, p, out),
        OP_GET => execGetPayload(srv, p, out),
        OP_UPDATE => execUpdatePayload(srv, p),
        OP_DELETE => execDeletePayload(srv, p),
        else => .{ .status = STATUS_ERROR, .payload_len = 0 },
    };
}

fn validateBatchEnvelope(p: []const u8) ?BatchEnvelope {
    if (p.len < BATCH_REQ_HDR) return null;
    const count = rdU16LE(p[0..2]);
    const min_resp_len = BATCH_RESP_HDR + @as(usize, count) * BATCH_ITEM_RESP_HDR;
    if (min_resp_len > WR_BUF) return null;

    var pos: usize = BATCH_REQ_HDR;
    for (0..count) |_| {
        if (pos + BATCH_ITEM_HDR > p.len) return null;
        const op = p[pos];
        const payload_len: usize = rdU32LE(p[pos + 1 ..][0..4]);
        if (payload_len > p.len - pos - BATCH_ITEM_HDR) return null;
        const payload = p[pos + BATCH_ITEM_HDR ..][0..payload_len];
        if (!validBatchPayload(op, payload)) return null;
        pos += BATCH_ITEM_HDR + payload_len;
    }
    if (pos != p.len) return null;
    return .{ .count = count };
}

fn validBatchPayload(op: u8, p: []const u8) bool {
    return switch (op) {
        OP_INSERT, OP_UPDATE => parseKVExact(p),
        OP_GET, OP_DELETE => parseKeyExact(p),
        else => false,
    };
}

// ── SCAN ────────────────────────────────────────────────────────────────────

fn doScan(srv: *WireServer, p: []const u8, w: *[WR_BUF]u8) usize {
    if (p.len < 10) return errResp(w, OP_SCAN, STATUS_ERROR);
    const cl = rdU16LE(p[0..2]);
    if (2 + cl + 8 > p.len) return errResp(w, OP_SCAN, STATUS_ERROR);
    const col_name = p[2..][0..cl];
    const limit = rdU32LE(p[2 + cl ..]);
    const offset = rdU32LE(p[6 + cl ..]);
    const ref = resolveCollectionRef(col_name);
    srv.db.recordTenantOperation(ref.tenant_id) catch return errResp(w, OP_SCAN, STATUS_ERROR);
    const col = srv.db.collectionForTenant(ref.tenant_id, ref.collection_name) catch return errResp(w, OP_SCAN, STATUS_ERROR);
    const result = col.scan(limit, offset, std.heap.page_allocator) catch return errResp(w, OP_SCAN, STATUS_ERROR);
    defer result.deinit();

    var pos: usize = HDR + 1 + 4; // header + status + count
    w[4] = OP_SCAN;
    w[5] = STATUS_OK;
    wrU32LE(w[6..10], @intCast(result.docs.len));

    for (result.docs) |d| {
        const dsz = 8 + 1 + 2 + d.key.len + 4 + d.value.len;
        if (pos + dsz > WR_BUF) break;
        wrU64LE(w[pos..][0..8], d.header.doc_id);
        w[pos + 8] = d.header.version;
        wrU16LE(w[pos + 9 ..][0..2], @intCast(d.key.len));
        @memcpy(w[pos + 11 ..][0..d.key.len], d.key);
        wrU32LE(w[pos + 11 + d.key.len ..][0..4], @intCast(d.value.len));
        if (d.value.len > 0)
            @memcpy(w[pos + 15 + d.key.len ..][0..d.value.len], d.value);
        pos += dsz;
    }
    wrU32BE(w, @intCast(pos));
    return pos;
}

// ── PING ────────────────────────────────────────────────────────────────────

fn doPing(w: *[WR_BUF]u8) usize {
    wrU32BE(w, HDR + 1);
    w[4] = OP_PING;
    w[5] = STATUS_OK;
    return HDR + 1;
}

// ── Wire v2 ────────────────────────────────────────────────────────────────

const Wire2Request = struct {
    flags: u8,
    request_id: u64,
    op: u8,
    body: []const u8,
};

const Wire2Result = struct {
    status: u8,
    payload_len: usize = 0,
};

const Wire2CollectionBody = struct {
    col: []const u8,
    count: u32,
    pos: usize,
};

const Wire2Stats = struct {
    inserted: u32 = 0,
    updated: u32 = 0,
    deleted: u32 = 0,
    missing: u32 = 0,
    errors: u32 = 0,
    bytes: u64 = 0,
};

fn dispatchWire2(srv: *WireServer, state: *WireConnState, p: []const u8, w: *[WR_BUF]u8) usize {
    const req = parseWire2Request(p) orelse {
        const fallback = Wire2Request{ .flags = 0, .request_id = 0, .op = 0, .body = &.{} };
        return writeWire2Response(w, fallback, STATUS_ERROR, 0);
    };

    if (req.op == WIRE2_OP_HELLO) return doWire2Hello(req, w);
    if (req.op == WIRE2_OP_AUTH) return doWire2Auth(srv, state, req, w);

    if (srv.db.auth.isEnabled() and !state.authenticated) {
        return writeWire2Response(w, req, STATUS_UNAUTHORIZED, 0);
    }

    const out = wire2BodyOut(w);
    const lock_mutation = wire2OpMutates(req.op);
    if (lock_mutation) {
        srv.op_mu.lock();
    } else if (wire2OpReadsStorage(req.op)) {
        srv.op_mu.lockShared();
    }
    defer {
        if (lock_mutation) srv.op_mu.unlock() else if (wire2OpReadsStorage(req.op)) srv.op_mu.unlockShared();
    }
    const result = switch (req.op) {
        WIRE2_OP_PING => Wire2Result{ .status = STATUS_OK },
        WIRE2_OP_GET => doWire2Get(srv, state, req.body, out),
        WIRE2_OP_PUT => doWire2Put(srv, state, req.body, out),
        WIRE2_OP_INSERT => doWire2Insert(srv, state, req.body, out),
        WIRE2_OP_DELETE => doWire2Delete(srv, state, req.body),
        WIRE2_OP_BATCH_GET => doWire2BatchGet(srv, state, req.body, out),
        WIRE2_OP_BULK_INSERT => doWire2BulkInsert(srv, state, req.body, out),
        WIRE2_OP_BULK_UPSERT => doWire2BulkUpsert(srv, state, req.body, out),
        WIRE2_OP_BULK_DELETE => doWire2BulkDelete(srv, state, req.body, out),
        else => Wire2Result{ .status = STATUS_ERROR },
    };
    return writeWire2Response(w, req, result.status, result.payload_len);
}

fn wire2OpMutates(op: u8) bool {
    return switch (op) {
        WIRE2_OP_PUT,
        WIRE2_OP_INSERT,
        WIRE2_OP_DELETE,
        WIRE2_OP_BULK_INSERT,
        WIRE2_OP_BULK_UPSERT,
        WIRE2_OP_BULK_DELETE,
        => true,
        else => false,
    };
}

fn wire2OpReadsStorage(op: u8) bool {
    return op == WIRE2_OP_GET or op == WIRE2_OP_BATCH_GET;
}

fn parseWire2Request(p: []const u8) ?Wire2Request {
    if (p.len < WIRE2_HDR) return null;
    if (p[0] != WIRE2_VERSION) return null;
    const body_len: usize = rdU32LE(p[12..16]);
    if (WIRE2_HDR + body_len != p.len) return null;
    return .{
        .flags = p[1],
        .request_id = rdU64LE(p[2..10]),
        .op = p[10],
        .body = p[WIRE2_HDR..],
    };
}

fn wire2BodyOut(w: *[WR_BUF]u8) []u8 {
    return w[HDR + WIRE2_HDR ..];
}

fn writeWire2Response(w: *[WR_BUF]u8, req: Wire2Request, status: u8, payload_len: usize) usize {
    if (payload_len > WR_BUF - HDR - WIRE2_HDR) {
        return writeWire2Response(w, req, STATUS_TOO_LARGE, 0);
    }
    const total_len = HDR + WIRE2_HDR + payload_len;
    wrU32BE(w, @intCast(total_len));
    w[4] = OP_WIRE2;
    w[5] = WIRE2_VERSION;
    w[6] = req.flags;
    wrU64LE(w[7..15], req.request_id);
    w[15] = req.op;
    w[16] = status;
    wrU32LE(w[17..21], @intCast(payload_len));
    return total_len;
}

fn doWire2Hello(req: Wire2Request, w: *[WR_BUF]u8) usize {
    const out = wire2BodyOut(w);
    wrU32LE(out[0..4], @intCast(MAX_FRAME));
    wrU32LE(out[4..8], @intCast(WR_BUF - HDR - WIRE2_HDR));
    wrU32LE(out[8..12], WIRE2_FEATURE_AUTH | WIRE2_FEATURE_PIPELINING | WIRE2_FEATURE_BATCH_GET | WIRE2_FEATURE_BULK_WRITE);
    return writeWire2Response(w, req, STATUS_OK, 12);
}

fn doWire2Auth(srv: *WireServer, state: *WireConnState, req: Wire2Request, w: *[WR_BUF]u8) usize {
    if (req.body.len < 2) return writeWire2Response(w, req, STATUS_ERROR, 0);
    const key_len: usize = rdU16LE(req.body[0..2]);
    if (2 + key_len != req.body.len) return writeWire2Response(w, req, STATUS_ERROR, 0);
    const key = req.body[2..][0..key_len];

    const ctx = srv.db.auth.resolve(key) orelse return writeWire2Response(w, req, STATUS_UNAUTHORIZED, 0);
    state.authenticated = true;
    state.auth_ctx = ctx;

    const out = wire2BodyOut(w);
    const tenant = ctx.tenant();
    if (3 + tenant.len > out.len) return writeWire2Response(w, req, STATUS_TOO_LARGE, 0);
    out[0] = @intFromEnum(ctx.perm);
    wrU16LE(out[1..3], @intCast(tenant.len));
    if (tenant.len > 0) @memcpy(out[3..][0..tenant.len], tenant);
    return writeWire2Response(w, req, STATUS_OK, 3 + tenant.len);
}

fn doWire2Get(srv: *WireServer, state: *const WireConnState, body: []const u8, out: []u8) Wire2Result {
    if (!parseKeyExact(body)) return .{ .status = STATUS_ERROR };
    const a = parseKey(body).?;
    const col = wire2Collection(srv, state, a.col) catch return .{ .status = STATUS_ERROR };
    const d = col.get(a.key) orelse return .{ .status = STATUS_NOT_FOUND };
    const payload_len = 8 + 1 + 4 + d.value.len;
    if (payload_len > out.len) return .{ .status = STATUS_TOO_LARGE };
    wrU64LE(out[0..8], d.header.doc_id);
    out[8] = d.header.version;
    wrU32LE(out[9..13], @intCast(d.value.len));
    if (d.value.len > 0) @memcpy(out[13..][0..d.value.len], d.value);
    return .{ .status = STATUS_OK, .payload_len = payload_len };
}

fn doWire2Insert(srv: *WireServer, state: *const WireConnState, body: []const u8, out: []u8) Wire2Result {
    if (!wire2CanWrite(state)) return .{ .status = STATUS_FORBIDDEN };
    if (!parseKVExact(body)) return .{ .status = STATUS_ERROR };
    if (out.len < 8) return .{ .status = STATUS_TOO_LARGE };
    const a = parseKV(body).?;
    const ref = wire2ResolveCollectionRef(state, a.col);
    srv.db.recordTenantOperation(ref.tenant_id) catch return .{ .status = STATUS_ERROR };
    srv.db.ensureTenantStorageAvailable(ref.tenant_id, a.val.len) catch return .{ .status = STATUS_ERROR };
    const col = srv.db.collectionForTenant(ref.tenant_id, ref.collection_name) catch return .{ .status = STATUS_ERROR };
    const doc_id = col.insert(a.key, a.val) catch return .{ .status = STATUS_ERROR };
    wrU64LE(out[0..8], doc_id);
    return .{ .status = STATUS_OK, .payload_len = 8 };
}

fn doWire2Put(srv: *WireServer, state: *const WireConnState, body: []const u8, out: []u8) Wire2Result {
    if (!wire2CanWrite(state)) return .{ .status = STATUS_FORBIDDEN };
    if (!parseKVExact(body)) return .{ .status = STATUS_ERROR };
    const a = parseKV(body).?;
    const ref = wire2ResolveCollectionRef(state, a.col);
    srv.db.recordTenantOperation(ref.tenant_id) catch return .{ .status = STATUS_ERROR };
    srv.db.ensureTenantStorageAvailable(ref.tenant_id, a.val.len) catch return .{ .status = STATUS_ERROR };
    const col = srv.db.collectionForTenant(ref.tenant_id, ref.collection_name) catch return .{ .status = STATUS_ERROR };

    var stats = Wire2Stats{ .bytes = a.key.len + a.val.len };
    const updated = col.update(a.key, a.val) catch return .{ .status = STATUS_ERROR };
    if (updated) {
        stats.updated = 1;
    } else {
        _ = col.insert(a.key, a.val) catch return .{ .status = STATUS_ERROR };
        stats.inserted = 1;
    }
    return writeWire2Stats(out, stats);
}

fn doWire2Delete(srv: *WireServer, state: *const WireConnState, body: []const u8) Wire2Result {
    if (!wire2CanWrite(state)) return .{ .status = STATUS_FORBIDDEN };
    if (!parseKeyExact(body)) return .{ .status = STATUS_ERROR };
    const a = parseKey(body).?;
    const ref = wire2ResolveCollectionRef(state, a.col);
    srv.db.recordTenantOperation(ref.tenant_id) catch return .{ .status = STATUS_ERROR };
    const col = srv.db.collectionForTenant(ref.tenant_id, ref.collection_name) catch return .{ .status = STATUS_ERROR };
    const deleted = col.delete(a.key) catch return .{ .status = STATUS_ERROR };
    return .{ .status = if (deleted) STATUS_OK else STATUS_NOT_FOUND };
}

fn doWire2BatchGet(srv: *WireServer, state: *const WireConnState, body: []const u8, out: []u8) Wire2Result {
    const parsed = parseWire2CollectionBody(body) orelse return .{ .status = STATUS_ERROR };
    const col = wire2Collection(srv, state, parsed.col) catch return .{ .status = STATUS_ERROR };

    if (out.len < 4) return .{ .status = STATUS_TOO_LARGE };
    wrU32LE(out[0..4], parsed.count);
    var out_pos: usize = 4;
    var pos = parsed.pos;
    for (0..parsed.count) |_| {
        const key = nextWire2Key(body, &pos) orelse return .{ .status = STATUS_ERROR };
        const base_len: usize = 1 + 8 + 1 + 4;
        if (out_pos + base_len > out.len) return .{ .status = STATUS_TOO_LARGE };

        if (col.get(key)) |d| {
            const item_len = base_len + d.value.len;
            if (out_pos + item_len > out.len) return .{ .status = STATUS_TOO_LARGE };
            out[out_pos] = STATUS_OK;
            wrU64LE(out[out_pos + 1 ..][0..8], d.header.doc_id);
            out[out_pos + 9] = d.header.version;
            wrU32LE(out[out_pos + 10 ..][0..4], @intCast(d.value.len));
            if (d.value.len > 0) @memcpy(out[out_pos + base_len ..][0..d.value.len], d.value);
            out_pos += item_len;
        } else {
            out[out_pos] = STATUS_NOT_FOUND;
            wrU64LE(out[out_pos + 1 ..][0..8], 0);
            out[out_pos + 9] = 0;
            wrU32LE(out[out_pos + 10 ..][0..4], 0);
            out_pos += base_len;
        }
    }
    if (pos != body.len) return .{ .status = STATUS_ERROR };
    return .{ .status = STATUS_OK, .payload_len = out_pos };
}

fn doWire2BulkInsert(srv: *WireServer, state: *const WireConnState, body: []const u8, out: []u8) Wire2Result {
    if (!wire2CanWrite(state)) return .{ .status = STATUS_FORBIDDEN };
    const parsed = parseWire2CollectionBody(body) orelse return .{ .status = STATUS_ERROR };
    const ref = wire2ResolveCollectionRef(state, parsed.col);
    srv.db.recordTenantOperation(ref.tenant_id) catch return .{ .status = STATUS_ERROR };
    srv.db.ensureTenantStorageAvailable(ref.tenant_id, body.len) catch return .{ .status = STATUS_ERROR };
    const col = srv.db.collectionForTenant(ref.tenant_id, ref.collection_name) catch return .{ .status = STATUS_ERROR };

    var rows: std.ArrayList(Collection.BulkInsertRow) = .empty;
    defer rows.deinit(srv.db.alloc);
    rows.ensureTotalCapacity(srv.db.alloc, @min(WIRE2_BULK_INSERT_CHUNK_ROWS, @max(@as(usize, 1), body.len / 80))) catch
        return .{ .status = STATUS_TOO_LARGE };

    var stats = Wire2Stats{};
    var chunk_bytes: usize = 0;
    var pos = parsed.pos;
    var malformed = false;
    const Flush = struct {
        fn run(col_arg: *Collection, rows_arg: *std.ArrayList(Collection.BulkInsertRow), stats_arg: *Wire2Stats, chunk_bytes_arg: *usize) !void {
            if (rows_arg.items.len == 0) return;
            const bulk = try col_arg.insertBulk(rows_arg.items);
            stats_arg.inserted += bulk.inserted;
            stats_arg.errors += bulk.errors;
            stats_arg.bytes += bulk.bytes;
            rows_arg.clearRetainingCapacity();
            chunk_bytes_arg.* = 0;
        }
    };

    for (0..parsed.count) |_| {
        const rec = nextWire2KeyValue(body, &pos) orelse {
            stats.errors += 1;
            malformed = true;
            break;
        };
        const row_bytes = rec.key.len + rec.val.len + 128;
        if (rows.items.len > 0 and chunk_bytes + row_bytes > WIRE2_BULK_INSERT_CHUNK_BYTES) {
            Flush.run(col, &rows, &stats, &chunk_bytes) catch return .{ .status = STATUS_ERROR };
        }
        rows.append(srv.db.alloc, .{ .key = rec.key, .value = rec.val, .line_len = 6 + rec.key.len + rec.val.len }) catch
            return .{ .status = STATUS_TOO_LARGE };
        chunk_bytes += row_bytes;
        if (rows.items.len >= WIRE2_BULK_INSERT_CHUNK_ROWS) {
            Flush.run(col, &rows, &stats, &chunk_bytes) catch return .{ .status = STATUS_ERROR };
        }
    }
    if (!malformed and pos != body.len) stats.errors += 1;
    Flush.run(col, &rows, &stats, &chunk_bytes) catch return .{ .status = STATUS_ERROR };
    return writeWire2Stats(out, stats);
}

fn doWire2BulkUpsert(srv: *WireServer, state: *const WireConnState, body: []const u8, out: []u8) Wire2Result {
    if (!wire2CanWrite(state)) return .{ .status = STATUS_FORBIDDEN };
    const parsed = parseWire2CollectionBody(body) orelse return .{ .status = STATUS_ERROR };
    const ref = wire2ResolveCollectionRef(state, parsed.col);
    srv.db.recordTenantOperation(ref.tenant_id) catch return .{ .status = STATUS_ERROR };
    srv.db.ensureTenantStorageAvailable(ref.tenant_id, body.len) catch return .{ .status = STATUS_ERROR };
    const col = srv.db.collectionForTenant(ref.tenant_id, ref.collection_name) catch return .{ .status = STATUS_ERROR };

    var stats = Wire2Stats{};
    var pos = parsed.pos;
    var malformed = false;
    for (0..parsed.count) |_| {
        const rec = nextWire2KeyValue(body, &pos) orelse {
            stats.errors += 1;
            malformed = true;
            break;
        };
        const updated = col.update(rec.key, rec.val) catch {
            stats.errors += 1;
            continue;
        };
        if (updated) {
            stats.updated += 1;
        } else {
            _ = col.insert(rec.key, rec.val) catch {
                stats.errors += 1;
                continue;
            };
            stats.inserted += 1;
        }
        stats.bytes += rec.key.len + rec.val.len;
    }
    if (!malformed and pos != body.len) stats.errors += 1;
    return writeWire2Stats(out, stats);
}

fn doWire2BulkDelete(srv: *WireServer, state: *const WireConnState, body: []const u8, out: []u8) Wire2Result {
    if (!wire2CanWrite(state)) return .{ .status = STATUS_FORBIDDEN };
    const parsed = parseWire2CollectionBody(body) orelse return .{ .status = STATUS_ERROR };
    const ref = wire2ResolveCollectionRef(state, parsed.col);
    srv.db.recordTenantOperation(ref.tenant_id) catch return .{ .status = STATUS_ERROR };
    const col = srv.db.collectionForTenant(ref.tenant_id, ref.collection_name) catch return .{ .status = STATUS_ERROR };

    var stats = Wire2Stats{};
    var pos = parsed.pos;
    var malformed = false;
    for (0..parsed.count) |_| {
        const key = nextWire2Key(body, &pos) orelse {
            stats.errors += 1;
            malformed = true;
            break;
        };
        const deleted = col.delete(key) catch {
            stats.errors += 1;
            continue;
        };
        if (deleted) stats.deleted += 1 else stats.missing += 1;
        stats.bytes += key.len;
    }
    if (!malformed and pos != body.len) stats.errors += 1;
    return writeWire2Stats(out, stats);
}

fn writeWire2Stats(out: []u8, stats: Wire2Stats) Wire2Result {
    const payload_len: usize = 28;
    if (out.len < payload_len) return .{ .status = STATUS_TOO_LARGE };
    wrU32LE(out[0..4], stats.inserted);
    wrU32LE(out[4..8], stats.updated);
    wrU32LE(out[8..12], stats.deleted);
    wrU32LE(out[12..16], stats.missing);
    wrU32LE(out[16..20], stats.errors);
    wrU64LE(out[20..28], stats.bytes);
    return .{ .status = STATUS_OK, .payload_len = payload_len };
}

fn parseWire2CollectionBody(body: []const u8) ?Wire2CollectionBody {
    if (body.len < 6) return null;
    const col_len: usize = rdU16LE(body[0..2]);
    if (col_len == 0 or 2 + col_len + 4 > body.len) return null;
    const count = rdU32LE(body[2 + col_len ..][0..4]);
    return .{
        .col = body[2..][0..col_len],
        .count = count,
        .pos = 2 + col_len + 4,
    };
}

fn nextWire2Key(body: []const u8, pos: *usize) ?[]const u8 {
    if (pos.* + 2 > body.len) return null;
    const key_len: usize = rdU16LE(body[pos.*..][0..2]);
    pos.* += 2;
    if (key_len == 0 or pos.* + key_len > body.len) return null;
    const key = body[pos.*..][0..key_len];
    pos.* += key_len;
    return key;
}

fn nextWire2KeyValue(body: []const u8, pos: *usize) ?struct { key: []const u8, val: []const u8 } {
    if (pos.* + 6 > body.len) return null;
    const key_len: usize = rdU16LE(body[pos.*..][0..2]);
    const val_len: usize = rdU32LE(body[pos.* + 2 ..][0..4]);
    pos.* += 6;
    if (key_len == 0 or pos.* + key_len > body.len) return null;
    const key = body[pos.*..][0..key_len];
    pos.* += key_len;
    if (pos.* + val_len > body.len) return null;
    const val = body[pos.*..][0..val_len];
    pos.* += val_len;
    return .{ .key = key, .val = val };
}

fn wire2Collection(srv: *WireServer, state: *const WireConnState, full_name: []const u8) !*Collection {
    const ref = wire2ResolveCollectionRef(state, full_name);
    try srv.db.recordTenantOperation(ref.tenant_id);
    return srv.db.collectionForTenant(ref.tenant_id, ref.collection_name);
}

fn wire2ResolveCollectionRef(state: *const WireConnState, full_name: []const u8) collection_mod.TenantCollectionRef {
    if (state.auth_ctx) |ctx| {
        if (ctx.perm != .admin) {
            const tenant_id = ctx.tenant();
            return .{
                .tenant_id = if (tenant_id.len > 0) tenant_id else collection_mod.DEFAULT_TENANT,
                .collection_name = full_name,
            };
        }
    }
    return resolveCollectionRef(full_name);
}

fn wire2CanWrite(state: *const WireConnState) bool {
    const ctx = state.auth_ctx orelse return true;
    return ctx.perm != .read_only;
}

// ── Helpers ─────────────────────────────────────────────────────────────────

fn errResp(w: *[WR_BUF]u8, op: u8, status: u8) usize {
    wrU32BE(w, HDR + 1);
    w[4] = op;
    w[5] = status;
    return HDR + 1;
}

const KV = struct { col: []const u8, key: []const u8, val: []const u8 };
const Key = struct { col: []const u8, key: []const u8 };

fn parseKV(p: []const u8) ?KV {
    if (p.len < 8) return null;
    const cl: usize = rdU16LE(p[0..2]);
    if (2 + cl + 2 > p.len) return null;
    const ko = 2 + cl;
    const kl: usize = rdU16LE(p[ko..][0..2]);
    const vo = ko + 2 + kl;
    if (vo + 4 > p.len) return null;
    const vl: usize = rdU32LE(p[vo..]);
    if (vo + 4 + vl > p.len) return null;
    return KV{ .col = p[2..][0..cl], .key = p[ko + 2 ..][0..kl], .val = p[vo + 4 ..][0..vl] };
}

fn parseKVExact(p: []const u8) bool {
    if (p.len < 8) return false;
    const cl: usize = rdU16LE(p[0..2]);
    if (2 + cl + 2 > p.len) return false;
    const ko = 2 + cl;
    const kl: usize = rdU16LE(p[ko..][0..2]);
    const vo = ko + 2 + kl;
    if (vo + 4 > p.len) return false;
    const vl: usize = rdU32LE(p[vo..][0..4]);
    return vo + 4 + vl == p.len;
}

fn parseKey(p: []const u8) ?Key {
    if (p.len < 4) return null;
    const cl: usize = rdU16LE(p[0..2]);
    if (2 + cl + 2 > p.len) return null;
    const ko = 2 + cl;
    const kl: usize = rdU16LE(p[ko..][0..2]);
    if (ko + 2 + kl > p.len) return null;
    return Key{ .col = p[2..][0..cl], .key = p[ko + 2 ..][0..kl] };
}

fn parseKeyExact(p: []const u8) bool {
    if (p.len < 4) return false;
    const cl: usize = rdU16LE(p[0..2]);
    if (2 + cl + 2 > p.len) return false;
    const ko = 2 + cl;
    const kl: usize = rdU16LE(p[ko..][0..2]);
    return ko + 2 + kl == p.len;
}

// ── Binary encoding ─────────────────────────────────────────────────────────

fn rdU32BE(b: []const u8) u32 {
    return (@as(u32, b[0]) << 24) | (@as(u32, b[1]) << 16) | (@as(u32, b[2]) << 8) | b[3];
}
fn rdU16LE(b: []const u8) u16 {
    return @as(u16, b[0]) | (@as(u16, b[1]) << 8);
}
fn rdU32LE(b: []const u8) u32 {
    return @as(u32, b[0]) | (@as(u32, b[1]) << 8) | (@as(u32, b[2]) << 16) | (@as(u32, b[3]) << 24);
}
fn rdU64LE(b: []const u8) u64 {
    return @as(u64, b[0]) |
        (@as(u64, b[1]) << 8) |
        (@as(u64, b[2]) << 16) |
        (@as(u64, b[3]) << 24) |
        (@as(u64, b[4]) << 32) |
        (@as(u64, b[5]) << 40) |
        (@as(u64, b[6]) << 48) |
        (@as(u64, b[7]) << 56);
}
fn wrU32BE(w: *[WR_BUF]u8, v: u32) void {
    w[0] = @intCast(v >> 24);
    w[1] = @intCast((v >> 16) & 0xFF);
    w[2] = @intCast((v >> 8) & 0xFF);
    w[3] = @intCast(v & 0xFF);
}
fn wrU64LE(w: []u8, v: u64) void {
    @memcpy(w[0..8], &std.mem.toBytes(v));
}
fn wrU32LE(w: []u8, v: u32) void {
    @memcpy(w[0..4], &std.mem.toBytes(v));
}
fn wrU16LE(w: []u8, v: u16) void {
    @memcpy(w[0..2], &std.mem.toBytes(v));
}

fn lookupCollection(srv: *WireServer, full_name: []const u8) !*collection_mod.Collection {
    const ref = resolveCollectionRef(full_name);
    try srv.db.recordTenantOperation(ref.tenant_id);
    return srv.db.collectionForTenant(ref.tenant_id, ref.collection_name);
}

fn resolveCollectionRef(full_name: []const u8) collection_mod.TenantCollectionRef {
    return collection_mod.splitTenantCollectionKey(full_name) orelse .{
        .tenant_id = collection_mod.DEFAULT_TENANT,
        .collection_name = full_name,
    };
}

test "wire batch envelope accepts supported exact items" {
    const get_payload = [_]u8{ 1, 0, 'c', 1, 0, 'k' };
    const insert_payload = [_]u8{ 1, 0, 'c', 1, 0, 'k', 1, 0, 0, 0, 'v' };

    var buf: [128]u8 = undefined;
    wrU16LE(buf[0..2], 2);
    var pos: usize = BATCH_REQ_HDR;

    buf[pos] = OP_GET;
    wrU32LE(buf[pos + 1 ..][0..4], get_payload.len);
    @memcpy(buf[pos + BATCH_ITEM_HDR ..][0..get_payload.len], &get_payload);
    pos += BATCH_ITEM_HDR + get_payload.len;

    buf[pos] = OP_INSERT;
    wrU32LE(buf[pos + 1 ..][0..4], insert_payload.len);
    @memcpy(buf[pos + BATCH_ITEM_HDR ..][0..insert_payload.len], &insert_payload);
    pos += BATCH_ITEM_HDR + insert_payload.len;

    const env = validateBatchEnvelope(buf[0..pos]) orelse return error.TestExpectedBatchEnvelope;
    try std.testing.expectEqual(@as(u16, 2), env.count);
}

test "wire batch envelope rejects unsupported and trailing payloads" {
    var unsupported: [BATCH_REQ_HDR + BATCH_ITEM_HDR]u8 = undefined;
    wrU16LE(unsupported[0..2], 1);
    unsupported[BATCH_REQ_HDR] = OP_SCAN;
    wrU32LE(unsupported[BATCH_REQ_HDR + 1 ..][0..4], 0);
    try std.testing.expect(validateBatchEnvelope(&unsupported) == null);

    const trailing = [_]u8{ 0, 0, 0 };
    try std.testing.expect(validateBatchEnvelope(&trailing) == null);

    const get_payload_with_trailing = [_]u8{ 1, 0, 'c', 1, 0, 'k', 0 };
    var bad_item: [BATCH_REQ_HDR + BATCH_ITEM_HDR + get_payload_with_trailing.len]u8 = undefined;
    wrU16LE(bad_item[0..2], 1);
    bad_item[BATCH_REQ_HDR] = OP_GET;
    wrU32LE(bad_item[BATCH_REQ_HDR + 1 ..][0..4], get_payload_with_trailing.len);
    @memcpy(bad_item[BATCH_REQ_HDR + BATCH_ITEM_HDR ..], &get_payload_with_trailing);
    try std.testing.expect(validateBatchEnvelope(&bad_item) == null);
}

test "wire2 envelope parses request id and rejects mismatched body length" {
    var payload: [WIRE2_HDR + 3]u8 = undefined;
    payload[0] = WIRE2_VERSION;
    payload[1] = 0;
    wrU64LE(payload[2..10], 0xAABBCCDD);
    payload[10] = WIRE2_OP_PING;
    payload[11] = 0;
    wrU32LE(payload[12..16], 3);
    @memcpy(payload[16..19], "abc");

    const req = parseWire2Request(&payload).?;
    try std.testing.expectEqual(@as(u64, 0xAABBCCDD), req.request_id);
    try std.testing.expectEqual(WIRE2_OP_PING, req.op);
    try std.testing.expectEqualStrings("abc", req.body);

    wrU32LE(payload[12..16], 2);
    try std.testing.expect(parseWire2Request(&payload) == null);
}

test "wire2 auth gates writes for read-only keys" {
    const alloc = std.testing.allocator;
    const tmp_dir = "/tmp/turbodb_wire2_auth_test";
    compat.cwd().deleteTree(tmp_dir) catch {};
    defer compat.cwd().deleteTree(tmp_dir) catch {};

    const db = try Database.open(alloc, tmp_dir);
    defer db.close();
    _ = db.auth.addKeyForTenant("reader-key", "reader", "tenant-a", .read_only);

    var srv = WireServer.init(db, 27017);
    var state = WireConnState{};
    const resp = try std.heap.page_allocator.create([WR_BUF]u8);
    defer std.heap.page_allocator.destroy(resp);

    var body: [128]u8 = undefined;
    var body_len: usize = 0;
    wrU16LE(body[0..2], 5);
    @memcpy(body[2..7], "users");
    wrU16LE(body[7..9], 2);
    @memcpy(body[9..11], "u1");
    body_len = 11;

    var req_buf: [256]u8 = undefined;
    const get_req = makeWire2TestRequest(&req_buf, 1, WIRE2_OP_GET, body[0..body_len]);
    _ = dispatchWire2(&srv, &state, get_req, resp);
    try std.testing.expectEqual(STATUS_UNAUTHORIZED, resp[16]);

    wrU16LE(body[0..2], 10);
    @memcpy(body[2..12], "reader-key");
    const auth_req = makeWire2TestRequest(&req_buf, 2, WIRE2_OP_AUTH, body[0..12]);
    _ = dispatchWire2(&srv, &state, auth_req, resp);
    try std.testing.expectEqual(STATUS_OK, resp[16]);
    try std.testing.expect(state.authenticated);

    body_len = encodeWire2TestKeyValue(&body, "users", "u1", "{\"x\":1}");
    const put_req = makeWire2TestRequest(&req_buf, 3, WIRE2_OP_PUT, body[0..body_len]);
    _ = dispatchWire2(&srv, &state, put_req, resp);
    try std.testing.expectEqual(STATUS_FORBIDDEN, resp[16]);
}

test "wire2 bulk insert and batch get use binary bodies" {
    const alloc = std.testing.allocator;
    const tmp_dir = "/tmp/turbodb_wire2_bulk_test";
    compat.cwd().deleteTree(tmp_dir) catch {};
    defer compat.cwd().deleteTree(tmp_dir) catch {};

    const db = try Database.open(alloc, tmp_dir);
    defer db.close();

    var srv = WireServer.init(db, 27017);
    var state = WireConnState{};
    const resp = try std.heap.page_allocator.create([WR_BUF]u8);
    defer std.heap.page_allocator.destroy(resp);

    var body: [512]u8 = undefined;
    var pos: usize = 0;
    pos += encodeWire2TestCollectionBody(body[pos..], "events", 2);
    pos += encodeWire2TestKeyValueRecord(body[pos..], "a", "{\"n\":1}");
    pos += encodeWire2TestKeyValueRecord(body[pos..], "b", "{\"n\":2}");

    var req_buf: [768]u8 = undefined;
    const bulk_req = makeWire2TestRequest(&req_buf, 10, WIRE2_OP_BULK_INSERT, body[0..pos]);
    _ = dispatchWire2(&srv, &state, bulk_req, resp);
    try std.testing.expectEqual(STATUS_OK, resp[16]);
    const bulk_body = wire2TestResponseBody(resp);
    try std.testing.expectEqual(@as(u32, 2), rdU32LE(bulk_body[0..4])); // inserted
    try std.testing.expectEqual(@as(u32, 0), rdU32LE(bulk_body[16..20])); // errors

    pos = 0;
    pos += encodeWire2TestCollectionBody(body[pos..], "events", 3);
    pos += encodeWire2TestKeyRecord(body[pos..], "a");
    pos += encodeWire2TestKeyRecord(body[pos..], "b");
    pos += encodeWire2TestKeyRecord(body[pos..], "missing");
    const get_req = makeWire2TestRequest(&req_buf, 11, WIRE2_OP_BATCH_GET, body[0..pos]);
    _ = dispatchWire2(&srv, &state, get_req, resp);
    try std.testing.expectEqual(STATUS_OK, resp[16]);

    const get_body = wire2TestResponseBody(resp);
    try std.testing.expectEqual(@as(u32, 3), rdU32LE(get_body[0..4]));
    var rp: usize = 4;
    try std.testing.expectEqual(STATUS_OK, get_body[rp]);
    const first_len = rdU32LE(get_body[rp + 10 ..][0..4]);
    rp += 14 + first_len;
    try std.testing.expectEqual(STATUS_OK, get_body[rp]);
    const second_len = rdU32LE(get_body[rp + 10 ..][0..4]);
    rp += 14 + second_len;
    try std.testing.expectEqual(STATUS_NOT_FOUND, get_body[rp]);
}

fn makeWire2TestRequest(buf: []u8, request_id: u64, op: u8, body: []const u8) []const u8 {
    buf[0] = WIRE2_VERSION;
    buf[1] = 0;
    wrU64LE(buf[2..10], request_id);
    buf[10] = op;
    buf[11] = 0;
    wrU32LE(buf[12..16], @intCast(body.len));
    @memcpy(buf[16..][0..body.len], body);
    return buf[0 .. WIRE2_HDR + body.len];
}

fn wire2TestResponseBody(resp: *[WR_BUF]u8) []const u8 {
    const body_len: usize = rdU32LE(resp[17..21]);
    return resp[21 .. 21 + body_len];
}

fn encodeWire2TestCollectionBody(out: []u8, col: []const u8, count: u32) usize {
    wrU16LE(out[0..2], @intCast(col.len));
    @memcpy(out[2..][0..col.len], col);
    wrU32LE(out[2 + col.len ..][0..4], count);
    return 2 + col.len + 4;
}

fn encodeWire2TestKeyRecord(out: []u8, key: []const u8) usize {
    wrU16LE(out[0..2], @intCast(key.len));
    @memcpy(out[2..][0..key.len], key);
    return 2 + key.len;
}

fn encodeWire2TestKeyValueRecord(out: []u8, key: []const u8, value: []const u8) usize {
    wrU16LE(out[0..2], @intCast(key.len));
    wrU32LE(out[2..6], @intCast(value.len));
    @memcpy(out[6..][0..key.len], key);
    @memcpy(out[6 + key.len ..][0..value.len], value);
    return 6 + key.len + value.len;
}

fn encodeWire2TestKeyValue(out: []u8, col: []const u8, key: []const u8, value: []const u8) usize {
    wrU16LE(out[0..2], @intCast(col.len));
    @memcpy(out[2..][0..col.len], col);
    const key_off = 2 + col.len;
    wrU16LE(out[key_off..][0..2], @intCast(key.len));
    @memcpy(out[key_off + 2 ..][0..key.len], key);
    const val_off = key_off + 2 + key.len;
    wrU32LE(out[val_off..][0..4], @intCast(value.len));
    @memcpy(out[val_off + 4 ..][0..value.len], value);
    return val_off + 4 + value.len;
}
