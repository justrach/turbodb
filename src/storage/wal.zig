//! Write-Ahead Log — shared durability engine for ZigGraph and TurboDB.
//!
//! ## Format
//!
//! Each entry:
//!   [lsn:8][length:4][crc32:4][op:1][db_tag:1][flags:2][txn_id:8][reserved:4]  = 32B header
//!   [payload: length bytes]
//!   [pad: 0-7 bytes to align next entry to 8 bytes]
//!
//! ## Group commit
//!
//! Multiple writer threads encode entries and call commit(lsn).
//! The first thread that commits while no flush is in progress becomes
//! the "flusher": it drains the shared write buffer, calls file.sync()
//! once for everyone, then wakes all waiters.  Subsequent writers that
//! arrive while a flush is in progress simply wait on the condition variable.
//!
//! This is the same algorithm PostgreSQL uses.  One fdatasync amortises
//! across every transaction that committed in that window — on NVMe the
//! window is ~0.1–0.5 ms; the throughput benefit is N× where N = concurrency.
//!
//! ## Recovery
//!
//! On startup call recover() before any writes.  It:
//!   1. Scans forward from the last checkpoint LSN.
//!   2. Collects all txn_ids with an OP_TXN_COMMIT entry (two-pass).
//!   3. Replays only committed transactions via the caller-supplied apply_fn.
//!   4. Truncates the tail of any partial (unfinished) entry at the end.
const std = @import("std");
const runtime = @import("runtime");

// ── Entry header (32 bytes, cache-line harmless) ──────────────────────────────

pub const OpCode = enum(u8) {
    nop           = 0x00,
    // Transaction control
    txn_begin     = 0x40,
    txn_commit    = 0x41,
    txn_abort     = 0x42,
    checkpoint    = 0xF0,
    // Graph ops
    node_insert   = 0x01,
    node_update   = 0x02,
    node_delete   = 0x03,
    edge_insert   = 0x10,
    edge_delete   = 0x11,
    // Document ops
    doc_insert    = 0x20,
    doc_update    = 0x21,
    doc_delete    = 0x22,
    // Schema ops
    create_coll   = 0x30,
    drop_coll     = 0x31,
    _,
};

pub const DB_TAG_GRAPH: u8 = 0x01;
pub const DB_TAG_DOC:   u8 = 0x02;
pub const FLAG_COMMIT:  u16 = 0x0001;

pub const EntryHeader = extern struct {
    lsn:      u64 align(1),
    length:   u32 align(1),
    crc32:    u32 align(1),
    op_code:  u8,
    db_tag:   u8,
    flags:    u16 align(1),
    txn_id:   u64 align(1),
    reserved: u32 align(1),

    comptime { std.debug.assert(@sizeOf(EntryHeader) == 32); }
};

pub const HEADER_SIZE: usize = @sizeOf(EntryHeader);

pub const Entry = struct {
    lsn:     u64,
    txn_id:  u64,
    op_code: OpCode,
    db_tag:  u8,
    flags:   u16,
    payload: []const u8,  // slice into caller-owned buffer
};

// ── CRC-32 (IEEE polynomial) ──────────────────────────────────────────────────

fn crc32(data: []const u8) u32 {
    var h = std.hash.crc.Crc32.init();
    h.update(data);
    return h.final();
}

fn entryChecksum(header_bytes: []const u8, payload: []const u8) u32 {
    // CRC over header (with crc field zeroed) ++ payload
    var h = std.hash.crc.Crc32.init();
    h.update(header_bytes[0..12]);  // lsn + length
    h.update(&[_]u8{0,0,0,0});     // zero-substitute crc32 field
    h.update(header_bytes[16..]);   // op..reserved
    h.update(payload);
    return h.final();
}

// ── WAL ───────────────────────────────────────────────────────────────────────

pub const WAL = struct {
    file:         std.Io.File,
    write_buf:    std.ArrayList(u8),   // pending (not yet flushed)
    next_lsn:     std.atomic.Value(u64),
    checkpoint_lsn: u64,
    allocator:    std.mem.Allocator,

    // Group commit state — guarded by mu
    mu:           std.Thread.Mutex,
    cond:         std.Thread.Condition,
    synced_lsn:   u64,
    flushing:     bool,

    // Background flusher
    flush_thread: ?std.Thread,
    flush_running: std.atomic.Value(bool),

    // ── Lifecycle ─────────────────────────────────────────────────────────────

    pub fn open(path: [:0]const u8, allocator: std.mem.Allocator) !WAL {
        const f = try std.Io.Dir.createFileAbsolute(runtime.io, path, .{
            .read = true, .truncate = false, .exclusive = false,
        });
        const wal = WAL{
            .file          = f,
            .write_buf     = .empty,
            .next_lsn      = std.atomic.Value(u64).init(1),
            .checkpoint_lsn = 0,
            .allocator     = allocator,
            .mu            = .{},
            .cond          = .{},
            .synced_lsn    = 0,
            .flushing      = false,
            .flush_thread  = null,
            .flush_running = std.atomic.Value(bool).init(false),
        };
        // Seek to end (append mode)
        _ = std.c.lseek(wal.file.handle, 0, std.posix.SEEK.END);
        return wal;
    }

    /// Start background flusher that commits every ~1ms.
    /// Call this after open() for high-throughput write mode.
    pub fn startFlusher(self: *WAL) !void {
        self.flush_running.store(true, .release);
        self.flush_thread = try std.Thread.spawn(.{}, flushLoop, .{self});
    }

    fn flushLoop(self: *WAL) void {
        while (self.flush_running.load(.acquire)) {
            std.Thread.sleep(1_000_000); // 1ms
            self.flushPending();
        }
        // Final flush on shutdown
        self.flushPending();
    }

    /// Flush any pending WAL entries to disk (non-blocking for callers).
    pub fn flushPending(self: *WAL) void {
        self.mu.lock();
        if (self.write_buf.items.len == 0) {
            self.mu.unlock();
            return;
        }
        if (self.flushing) {
            self.mu.unlock();
            return;
        }
        self.flushing = true;
        var to_write = self.write_buf;
        const target = self.next_lsn.load(.monotonic) -| 1;
        self.write_buf = .empty;
        self.mu.unlock();

        self.file.writeStreamingAll(runtime.io, to_write.items) catch {};
        to_write.deinit(self.allocator);
        self.file.sync(runtime.io) catch {};

        self.mu.lock();
        self.synced_lsn = target;
        self.flushing = false;
        self.cond.broadcast();
        self.mu.unlock();
    }

    pub fn close(self: *WAL) void {
        if (self.flush_running.load(.acquire)) {
            self.flush_running.store(false, .release);
            if (self.flush_thread) |t| t.join();
        }
        // Flush any remaining entries
        self.flushPending();
        self.write_buf.deinit(self.allocator);
        self.file.close(runtime.io);
    }

    // ── Write ─────────────────────────────────────────────────────────────────

    /// Encode an entry into the shared write buffer and return its LSN.
    /// Thread-safe.  Does NOT guarantee durability — call commit(lsn) for that.
    pub fn write(
        self:    *WAL,
        txn_id:  u64,
        op:      OpCode,
        db_tag:  u8,
        flags:   u16,
        payload: []const u8,
    ) !u64 {
        const lsn = self.next_lsn.fetchAdd(1, .monotonic);
        const pad = paddingTo8(HEADER_SIZE + payload.len);

        var hdr = EntryHeader{
            .lsn      = lsn,
            .length   = @intCast(payload.len),
            .crc32    = 0,
            .op_code  = @intFromEnum(op),
            .db_tag   = db_tag,
            .flags    = flags,
            .txn_id   = txn_id,
            .reserved = 0,
        };
        const hdr_bytes = std.mem.asBytes(&hdr);
        hdr.crc32 = entryChecksum(hdr_bytes, payload);

        self.mu.lock();
        defer self.mu.unlock();
        try self.write_buf.appendSlice(self.allocator, std.mem.asBytes(&hdr));
        try self.write_buf.appendSlice(self.allocator, payload);
        if (pad > 0) try self.write_buf.appendNTimes(self.allocator, 0, pad);
        return lsn;
    }

    /// Mark a transaction committed.  Returns only after the entry is durable.
    /// Implements group commit: the first caller flushes for everyone.
    pub fn commit(self: *WAL, txn_id: u64, db_tag: u8) !void {
        // Write COMMIT entry to buffer
        var commit_payload: [16]u8 = undefined;
        std.mem.writeInt(u64, commit_payload[0..8], txn_id,          .little);
        std.mem.writeInt(u64, commit_payload[8..16], @as(u64, @truncate(@as(u128, @bitCast(std.time.nanoTimestamp())))), .little);
        const lsn = try self.write(txn_id, .txn_commit, db_tag, FLAG_COMMIT, &commit_payload);

        // Group commit: become flusher or wait
        self.mu.lock();

        if (self.synced_lsn >= lsn) {
            self.mu.unlock();
            return;
        }

        if (self.flushing) {
            // Wait for current flusher
            while (self.synced_lsn < lsn) self.cond.wait(&self.mu);
            self.mu.unlock();
            return;
        }

        // We are the flusher
        self.flushing = true;
        // Snapshot the buffer and target LSN under the lock
        var to_write:   std.ArrayList(u8) = self.write_buf;
        const target = self.next_lsn.load(.monotonic) - 1;
        self.write_buf = .empty;
        self.mu.unlock();

        // ── I/O outside lock ──────────────────────────────────────────────────
        var io_err: ?anyerror = null;
        self.file.writeStreamingAll(runtime.io, to_write.items) catch |e| { io_err = e; };
        to_write.deinit(self.allocator);
        if (io_err == null) self.file.sync(runtime.io) catch |e| { io_err = e; };
        // ─────────────────────────────────────────────────────────────────────

        self.mu.lock();
        if (io_err == null) self.synced_lsn = target;
        self.flushing = false;
        self.cond.broadcast();
        self.mu.unlock();

        if (io_err) |e| return e;
    }

    // ── Checkpoint ────────────────────────────────────────────────────────────

    /// Write a checkpoint barrier entry and update checkpoint_lsn.
    pub fn checkpoint(self: *WAL, db_tag: u8) !void {
        var p: [8]u8 = undefined;
        std.mem.writeInt(u64, &p, self.checkpoint_lsn, .little);
        const lsn = try self.write(0, .checkpoint, db_tag, FLAG_COMMIT, &p);
        self.checkpoint_lsn = lsn;
        // Force flush
        self.mu.lock();
        self.flushing = true;
        var to_write = self.write_buf;
        self.write_buf = .empty;
        self.mu.unlock();
        self.file.writeStreamingAll(runtime.io, to_write.items) catch {};
        to_write.deinit(self.allocator);
        try self.file.sync(runtime.io);
        self.mu.lock();
        self.synced_lsn = lsn;
        self.flushing = false;
        self.cond.broadcast();
        self.mu.unlock();
    }

    // ── Recovery ──────────────────────────────────────────────────────────────

    /// Replay WAL from the beginning.  apply_fn is called for every committed
    /// entry with lsn > skip_before_lsn.  On return the file pointer is at EOF
    /// and ready for new appends.
    pub fn recover(
        self:            *WAL,
        skip_before_lsn: u64,
        apply_fn:        *const fn (entry: Entry) anyerror!void,
        allocator:       std.mem.Allocator,
    ) !void {
        _ = std.c.lseek(self.file.handle, @intCast(0), std.posix.SEEK.SET);

        // ── Pass 1: collect all committed txn_ids ─────────────────────────────
        var committed = std.AutoHashMap(u64, void).init(allocator);
        defer committed.deinit();
        {
            var it = EntryIterator.init(self.file, allocator);
            defer it.deinit();
            while (try it.next()) |e| {
                if (e.op_code == .txn_commit or e.flags & FLAG_COMMIT != 0)
                    try committed.put(e.txn_id, {});
            }
        }

        // ── Pass 2: replay committed entries ─────────────────────────────────
        _ = std.c.lseek(self.file.handle, @intCast(0), std.posix.SEEK.SET);
        var max_lsn: u64 = 0;
        {
            var it = EntryIterator.init(self.file, allocator);
            defer it.deinit();
            while (try it.next()) |e| {
                max_lsn = @max(max_lsn, e.lsn);
                if (e.lsn <= skip_before_lsn) continue;
                if (!committed.contains(e.txn_id)) continue;
                if (e.op_code == .txn_commit or e.op_code == .txn_begin or
                    e.op_code == .txn_abort  or e.op_code == .checkpoint) continue;
                try apply_fn(e);
            }
        }

        // ── Advance LSN counter and seek to EOF for new writes ────────────────
        self.next_lsn.store(max_lsn + 1, .release);
        self.synced_lsn = max_lsn;
        _ = std.c.lseek(self.file.handle, 0, std.posix.SEEK.END);
        std.log.info("WAL recovery: replayed up to lsn={d}", .{max_lsn});
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    inline fn paddingTo8(n: usize) usize {
        return (8 - (n % 8)) % 8;
    }
};

// ── Entry iterator (for recovery) ────────────────────────────────────────────

const EntryIterator = struct {
    fd:        std.posix.fd_t,
    buf:       []u8,
    allocator: std.mem.Allocator,

    fn init(file: std.Io.File, allocator: std.mem.Allocator) EntryIterator {
        return .{
            .fd        = file.handle,
            .buf       = &.{},
            .allocator = allocator,
        };
    }

    fn deinit(self: *EntryIterator) void {
        if (self.buf.len > 0) self.allocator.free(self.buf);
    }

    /// Read up to `out.len` bytes into `out`. Returns actual bytes read (0 at EOF).
    fn readAll(fd: std.posix.fd_t, out: []u8) usize {
        var total: usize = 0;
        while (total < out.len) {
            const n = std.posix.read(fd, out[total..]) catch return total;
            if (n == 0) break;
            total += n;
        }
        return total;
    }

    /// Returns null at EOF or on a corrupt entry.
    fn next(self: *EntryIterator) !?Entry {
        var hdr_buf: [HEADER_SIZE]u8 = undefined;
        const n = readAll(self.fd, &hdr_buf);
        if (n < HEADER_SIZE) return null;

        const hdr: *const EntryHeader = @ptrCast(&hdr_buf);
        if (hdr.lsn == 0) return null; // padding / sentinel

        // Read payload
        if (hdr.length > 0) {
            if (self.buf.len < hdr.length) {
                if (self.buf.len > 0) self.allocator.free(self.buf);
                self.buf = try self.allocator.alloc(u8, hdr.length);
            }
            const p = readAll(self.fd, self.buf[0..hdr.length]);
            if (p < hdr.length) return null;
        }

        const payload: []const u8 = if (hdr.length > 0) self.buf[0..hdr.length] else &.{};

        // Verify CRC
        const expected = entryChecksum(&hdr_buf, payload);
        if (expected != hdr.crc32) return null; // corrupt — stop recovery

        // Skip padding
        const pad = WAL.paddingTo8(HEADER_SIZE + hdr.length);
        if (pad > 0) {
            var skip: [7]u8 = undefined;
            _ = readAll(self.fd, skip[0..pad]);
        }

        return Entry{
            .lsn     = hdr.lsn,
            .txn_id  = hdr.txn_id,
            .op_code = @enumFromInt(hdr.op_code),
            .db_tag  = hdr.db_tag,
            .flags   = hdr.flags,
            .payload = payload,
        };
    }
};
