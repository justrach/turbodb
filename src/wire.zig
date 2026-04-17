/// TurboDB Binary Wire Protocol Server
///
/// Frame: [len:u32 BE][op:u8][payload...]
/// len includes the 5-byte header.
///
/// Ops: INSERT=0x01 GET=0x02 UPDATE=0x03 DELETE=0x04 SCAN=0x05 PING=0x06
/// Status: OK=0x00 NOT_FOUND=0x01 ERROR=0x02
const std = @import("std");
const activity = @import("activity.zig");
const collection_mod = @import("collection.zig");
const Database = collection_mod.Database;
const Collection = collection_mod.Collection;

const OP_INSERT: u8 = 0x01;
const OP_GET: u8 = 0x02;
const OP_UPDATE: u8 = 0x03;
const OP_DELETE: u8 = 0x04;
const OP_SCAN: u8 = 0x05;
const OP_PING: u8 = 0x06;

const STATUS_OK: u8 = 0x00;
const STATUS_NOT_FOUND: u8 = 0x01;
const STATUS_ERROR: u8 = 0x02;

const HDR: usize = 5;
const MAX_FRAME: usize = 1048576;
const RD_BUF: usize = 65536;
const WR_BUF: usize = 131072;

pub const WireServer = struct {
    db: *Database,
    port: u16,
    running: std.atomic.Value(bool),
    activity: activity.ActivityTracker,

    pub fn init(db: *Database, port: u16) WireServer {
        return .{ .db = db, .port = port, .running = std.atomic.Value(bool).init(false), .activity = activity.ActivityTracker.init() };
    }

    pub fn run(self: *WireServer) !void {
        const addr = try std.net.Address.parseIp("0.0.0.0", self.port);
        var listener = try addr.listen(.{ .reuse_address = true, .kernel_backlog = 1024 });
        defer listener.deinit();
        self.running.store(true, .release);
        std.log.info("TurboDB wire protocol on :{d}", .{self.port});
        while (self.running.load(.acquire)) {
            const conn = listener.accept() catch continue;
            const t = std.Thread.spawn(.{}, handleConn, .{ self, conn }) catch continue;
            t.detach();
        }
    }

    pub fn runUnix(self: *WireServer, path: []const u8) !void {
        // Remove any existing socket file
        // Remove any existing socket file
        var path_buf: [4096]u8 = undefined;
        if (path.len < path_buf.len) {
            @memcpy(path_buf[0..path.len], path);
            path_buf[path.len] = 0;
            _ = std.c.unlink(@ptrCast(&path_buf));
        }
        const fd = try std.posix.socket(std.posix.AF.UNIX, std.posix.SOCK.STREAM, 0);
        defer std.posix.close(fd);

        // Construct sockaddr_un
        var addr: std.posix.sockaddr.un = .{ .family = std.posix.AF.UNIX, .path = undefined };
        @memset(&addr.path, 0);
        if (path.len >= addr.path.len) return error.PathTooLong;
        @memcpy(addr.path[0..path.len], path);

        try std.posix.bind(fd, @ptrCast(&addr), @sizeOf(std.posix.sockaddr.un));
        try std.posix.listen(fd, 1024);

        self.running.store(true, .release);
        std.log.info("TurboDB wire protocol on unix:{s}", .{path});

        while (self.running.load(.acquire)) {
            var client_addr: std.posix.sockaddr.un = undefined;
            var addr_len: std.posix.socklen_t = @sizeOf(std.posix.sockaddr.un);
            const client_fd = std.posix.accept(fd, @ptrCast(&client_addr), &addr_len, 0) catch continue;
            const stream = std.net.Stream{ .handle = client_fd };
            const conn = std.net.Server.Connection{ .stream = stream, .address = std.net.Address.initUnix(path) catch continue };
            const t = std.Thread.spawn(.{}, handleConn, .{ self, conn }) catch continue;
            t.detach();
        }
    }

    pub fn stop(self: *WireServer) void {
        self.running.store(false, .release);
    }
};

const Bufs = struct { rd: [RD_BUF]u8, wr: [WR_BUF]u8 };

fn handleConn(srv: *WireServer, conn: std.net.Server.Connection) void {
    defer conn.stream.close();
    const bufs = std.heap.page_allocator.create(Bufs) catch return;
    defer std.heap.page_allocator.destroy(bufs);

    // Pre-resolve the most common collection for the fast path
    var cached_col: ?*collection_mod.Collection = null;
    var cached_col_name: [128]u8 = undefined;
    var cached_col_len: usize = 0;

    var rp: usize = 0;

    while (true) {
        if (rp >= RD_BUF) rp = 0;
        const n = conn.stream.read(bufs.rd[rp..]) catch return;
        if (n == 0) return;
        rp += n;

        var consumed: usize = 0;
        while (consumed + HDR <= rp) {
            const flen = rdU32BE(bufs.rd[consumed..]);
            if (flen < HDR or flen > MAX_FRAME) return;
            if (consumed + flen > rp) break;
            const op = bufs.rd[consumed + 4];
            const payload = bufs.rd[consumed + HDR .. consumed + flen];

            // ── FAST PATH: inline GET with zero-copy ──
            if (op == OP_GET) {
                srv.activity.recordQuery();
                const wn = fastGet(srv, payload, &bufs.wr, &cached_col, &cached_col_name, &cached_col_len);
                conn.stream.writeAll(bufs.wr[0..wn]) catch return;
            } else {
                // Invalidate collection cache on writes
                if (op == OP_INSERT or op == OP_UPDATE or op == OP_DELETE) {
                    cached_col = null;
                }
                srv.activity.recordQuery();
                const wn = dispatch(srv, op, payload, &bufs.wr);
                conn.stream.writeAll(bufs.wr[0..wn]) catch return;
            }
            consumed += flen;
        }
        if (consumed > 0) {
            if (consumed < rp) std.mem.copyForwards(u8, bufs.rd[0..], bufs.rd[consumed..rp]);
            rp -= consumed;
        }
    }
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
        else => errResp(w, 0xFF, STATUS_ERROR),
    };
}

// ── INSERT ──────────────────────────────────────────────────────────────────

fn doInsert(srv: *WireServer, p: []const u8, w: *[WR_BUF]u8) usize {
    const a = parseKV(p) orelse return errResp(w, OP_INSERT, STATUS_ERROR);
    const ref = resolveCollectionRef(a.col);
    srv.db.recordTenantOperation(ref.tenant_id) catch return errResp(w, OP_INSERT, STATUS_ERROR);
    srv.db.ensureTenantStorageAvailable(ref.tenant_id, a.val.len) catch return errResp(w, OP_INSERT, STATUS_ERROR);
    const col = srv.db.collectionForTenant(ref.tenant_id, ref.collection_name) catch return errResp(w, OP_INSERT, STATUS_ERROR);
    const doc_id = col.insert(a.key, a.val) catch return errResp(w, OP_INSERT, STATUS_ERROR);
    // [len:4][op:1][status:1][doc_id:8] = 14
    wrU32BE(w, 14);
    w[4] = OP_INSERT;
    w[5] = STATUS_OK;
    wrU64LE(w[6..14], doc_id);
    return 14;
}

// ── GET ─────────────────────────────────────────────────────────────────────

fn doGet(srv: *WireServer, p: []const u8, w: *[WR_BUF]u8) usize {
    const a = parseKey(p) orelse return errResp(w, OP_GET, STATUS_ERROR);
    const ref = resolveCollectionRef(a.col);
    srv.db.recordTenantOperation(ref.tenant_id) catch return errResp(w, OP_GET, STATUS_ERROR);
    const col = srv.db.collectionForTenant(ref.tenant_id, ref.collection_name) catch return errResp(w, OP_GET, STATUS_ERROR);
    const d = col.get(a.key) orelse return errResp(w, OP_GET, STATUS_NOT_FOUND);
    // [len:4][op:1][status:1][doc_id:8][ver:1][val_len:4][val:N]
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

// ── UPDATE ──────────────────────────────────────────────────────────────────

fn doUpdate(srv: *WireServer, p: []const u8, w: *[WR_BUF]u8) usize {
    const a = parseKV(p) orelse return errResp(w, OP_UPDATE, STATUS_ERROR);
    const ref = resolveCollectionRef(a.col);
    srv.db.recordTenantOperation(ref.tenant_id) catch return errResp(w, OP_UPDATE, STATUS_ERROR);
    srv.db.ensureTenantStorageAvailable(ref.tenant_id, a.val.len) catch return errResp(w, OP_UPDATE, STATUS_ERROR);
    const col = srv.db.collectionForTenant(ref.tenant_id, ref.collection_name) catch return errResp(w, OP_UPDATE, STATUS_ERROR);
    const ok = col.update(a.key, a.val) catch return errResp(w, OP_UPDATE, STATUS_ERROR);
    if (!ok) return errResp(w, OP_UPDATE, STATUS_NOT_FOUND);
    wrU32BE(w, HDR + 1);
    w[4] = OP_UPDATE;
    w[5] = STATUS_OK;
    return HDR + 1;
}

// ── DELETE ───────────────────────────────────────────────────────────────────

fn doDelete(srv: *WireServer, p: []const u8, w: *[WR_BUF]u8) usize {
    const a = parseKey(p) orelse return errResp(w, OP_DELETE, STATUS_ERROR);
    const ref = resolveCollectionRef(a.col);
    srv.db.recordTenantOperation(ref.tenant_id) catch return errResp(w, OP_DELETE, STATUS_ERROR);
    const col = srv.db.collectionForTenant(ref.tenant_id, ref.collection_name) catch return errResp(w, OP_DELETE, STATUS_ERROR);
    const ok = col.delete(a.key) catch return errResp(w, OP_DELETE, STATUS_ERROR);
    if (!ok) return errResp(w, OP_DELETE, STATUS_NOT_FOUND);
    wrU32BE(w, HDR + 1);
    w[4] = OP_DELETE;
    w[5] = STATUS_OK;
    return HDR + 1;
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

fn parseKey(p: []const u8) ?Key {
    if (p.len < 4) return null;
    const cl: usize = rdU16LE(p[0..2]);
    if (2 + cl + 2 > p.len) return null;
    const ko = 2 + cl;
    const kl: usize = rdU16LE(p[ko..][0..2]);
    if (ko + 2 + kl > p.len) return null;
    return Key{ .col = p[2..][0..cl], .key = p[ko + 2 ..][0..kl] };
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
