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
const compat = @import("compat");

// ── Entry header (32 bytes, cache-line harmless) ──────────────────────────────

pub const OpCode = enum(u8) {
    nop = 0x00,
    // Transaction control
    txn_begin = 0x40,
    txn_commit = 0x41,
    txn_abort = 0x42,
    checkpoint = 0xF0,
    // Graph ops
    node_insert = 0x01,
    node_update = 0x02,
    node_delete = 0x03,
    edge_insert = 0x10,
    edge_delete = 0x11,
    // Document ops
    doc_insert = 0x20,
    doc_update = 0x21,
    doc_delete = 0x22,
    // Schema ops
    create_coll = 0x30,
    drop_coll = 0x31,
    _,
};

pub const DB_TAG_GRAPH: u8 = 0x01;
pub const DB_TAG_DOC: u8 = 0x02;
pub const FLAG_COMMIT: u16 = 0x0001;

pub const EntryHeader = extern struct {
    lsn: u64 align(1),
    length: u32 align(1),
    crc32: u32 align(1),
    op_code: u8,
    db_tag: u8,
    flags: u16 align(1),
    txn_id: u64 align(1),
    reserved: u32 align(1),

    comptime {
        std.debug.assert(@sizeOf(EntryHeader) == 32);
    }
};

pub const HEADER_SIZE: usize = @sizeOf(EntryHeader);
pub const MAX_ENTRY_PAYLOAD: usize = 64 * 1024 * 1024;
const MAX_ENTRY_PAYLOAD_U32: u32 = @intCast(MAX_ENTRY_PAYLOAD);

pub const Entry = struct {
    lsn: u64,
    txn_id: u64,
    op_code: OpCode,
    db_tag: u8,
    flags: u16,
    payload: []const u8, // slice into caller-owned buffer
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
    h.update(header_bytes[0..12]); // lsn + length
    h.update(&[_]u8{ 0, 0, 0, 0 }); // zero-substitute crc32 field
    h.update(header_bytes[16..]); // op..reserved
    h.update(payload);
    return h.final();
}

// ── WAL ───────────────────────────────────────────────────────────────────────

pub const WAL = struct {
    file: compat.File,
    uring: ?compat.LinuxUring,
    write_buf: std.ArrayList(u8), // pending (not yet flushed)
    next_lsn: std.atomic.Value(u64),
    checkpoint_lsn: u64,
    append_offset: u64,
    uring_flushes: usize,
    sync_flushes: usize,
    uring_min_write_and_sync: usize,
    allocator: std.mem.Allocator,

    // Group commit state — guarded by mu
    mu: compat.Mutex,
    cond: compat.Condition,
    synced_lsn: u64,
    flushing: bool,
    last_flush_error: ?anyerror,

    // Background flusher
    flush_thread: ?std.Thread,
    flush_running: std.atomic.Value(bool),

    /// Backpressure: block writers if pending buffer exceeds this.
    const MAX_WRITE_BUF: usize = 8 * 1024 * 1024; // 8 MiB
    const DEFAULT_URING_MIN_WRITE_AND_SYNC: usize = 32 * 1024;

    // ── Lifecycle ─────────────────────────────────────────────────────────────

    pub fn open(path: [:0]const u8, allocator: std.mem.Allocator) !WAL {
        const f = blk: {
            const fd = std.c.open(path, .{ .ACCMODE = .RDWR, .CREAT = true }, @as(std.c.mode_t, 0o644));
            if (fd < 0) return error.CreateFileError;
            break :blk compat.File{ .handle = fd };
        };
        const end = std.c.lseek(f.handle, 0, std.c.SEEK.END);
        const wal = WAL{
            .file = f,
            .uring = compat.initLinuxUring(8),
            .write_buf = .empty,
            .next_lsn = std.atomic.Value(u64).init(1),
            .checkpoint_lsn = 0,
            .append_offset = if (end >= 0) @intCast(end) else 0,
            .uring_flushes = 0,
            .sync_flushes = 0,
            .uring_min_write_and_sync = compat.envUsize("TURBODB_IO_URING_MIN_BYTES", DEFAULT_URING_MIN_WRITE_AND_SYNC),
            .allocator = allocator,
            .mu = .{},
            .cond = .{},
            .synced_lsn = 0,
            .flushing = false,
            .last_flush_error = null,
            .flush_thread = null,
            .flush_running = std.atomic.Value(bool).init(false),
        };
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
            compat.sleep(1_000_000); // 1ms
            self.flushPending();
        }
        // Final flush on shutdown
        self.flushPending();
    }

    /// Flush any pending WAL entries to disk (non-blocking for callers).
    pub fn flushPending(self: *WAL) void {
        self.mu.lock();
        if (self.last_flush_error != null) {
            self.mu.unlock();
            return;
        }
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

        const io_err = self.writeAndSync(to_write.items);
        to_write.deinit(self.allocator);

        self.mu.lock();
        if (io_err) |e| {
            self.last_flush_error = e;
        } else {
            self.synced_lsn = target;
            self.last_flush_error = null;
        }
        self.flushing = false;
        self.cond.broadcast();
        self.mu.unlock();
    }

    pub fn lastFlushError(self: *WAL) ?anyerror {
        self.mu.lock();
        defer self.mu.unlock();
        return self.last_flush_error;
    }

    pub fn usingLinuxUring(self: *const WAL) bool {
        return self.uring != null;
    }

    pub fn usedLinuxUring(self: *const WAL) bool {
        return self.uring_flushes > 0;
    }

    pub fn close(self: *WAL) void {
        if (self.flush_running.load(.acquire)) {
            self.flush_running.store(false, .release);
            if (self.flush_thread) |t| t.join();
        }
        // Flush any remaining entries
        self.flushPending();
        self.write_buf.deinit(self.allocator);
        if (self.uring) |*ring| {
            compat.deinitLinuxUring(ring);
            self.uring = null;
        }
        self.file.close();
    }

    // ── Write ─────────────────────────────────────────────────────────────────

    /// Encode an entry into the shared write buffer and return its LSN.
    /// Thread-safe.  Does NOT guarantee durability — call commit(txn_id, db_tag) for that.
    /// Blocks if write buffer exceeds MAX_WRITE_BUF to apply backpressure.
    pub fn write(
        self: *WAL,
        txn_id: u64,
        op: OpCode,
        db_tag: u8,
        flags: u16,
        payload: []const u8,
    ) !u64 {
        if (payload.len > MAX_ENTRY_PAYLOAD) return error.WALPayloadTooLarge;

        const lsn = self.next_lsn.fetchAdd(1, .monotonic);
        const pad = paddingTo8(HEADER_SIZE + payload.len);

        var hdr = EntryHeader{
            .lsn = lsn,
            .length = @intCast(payload.len),
            .crc32 = 0,
            .op_code = @intFromEnum(op),
            .db_tag = db_tag,
            .flags = flags,
            .txn_id = txn_id,
            .reserved = 0,
        };
        const hdr_bytes = std.mem.asBytes(&hdr);
        hdr.crc32 = entryChecksum(hdr_bytes, payload);

        self.mu.lock();
        if (self.last_flush_error) |e| {
            self.mu.unlock();
            return e;
        }
        // Backpressure: wait until flusher drains the buffer below threshold.
        // Use condition variable instead of yield-loop so the flusher can wake us.
        while (self.write_buf.items.len >= MAX_WRITE_BUF) {
            if (self.last_flush_error) |e| {
                self.mu.unlock();
                return e;
            }
            // Release lock and wait for flusher to signal.
            self.cond.wait(&self.mu);
        }
        defer self.mu.unlock();
        try self.write_buf.appendSlice(self.allocator, std.mem.asBytes(&hdr));
        try self.write_buf.appendSlice(self.allocator, payload);
        if (pad > 0) try self.write_buf.appendNTimes(self.allocator, 0, pad);
        return lsn;
    }

    /// Append a single-entry committed operation without forcing durability.
    /// The background flusher or a later commit/checkpoint is responsible for fsync.
    /// Use this only when the txn_id is not shared with a multi-entry transaction.
    pub fn writeCommitted(
        self: *WAL,
        txn_id: u64,
        op: OpCode,
        db_tag: u8,
        payload: []const u8,
    ) !u64 {
        return self.write(txn_id, op, db_tag, FLAG_COMMIT, payload);
    }

    /// Mark a transaction committed.  Returns only after the entry is durable.
    /// Implements group commit: the first caller flushes for everyone.
    pub fn commit(self: *WAL, txn_id: u64, db_tag: u8) !void {
        // Write COMMIT entry to buffer
        var commit_payload: [16]u8 = undefined;
        std.mem.writeInt(u64, commit_payload[0..8], txn_id, .little);
        std.mem.writeInt(u64, commit_payload[8..16], @as(u64, @truncate(@as(u128, @bitCast(compat.nanoTimestamp())))), .little);
        const lsn = try self.write(txn_id, .txn_commit, db_tag, FLAG_COMMIT, &commit_payload);

        // Group commit: become flusher or wait
        self.mu.lock();

        while (self.synced_lsn < lsn) {
            if (self.last_flush_error) |e| {
                self.mu.unlock();
                return e;
            }

            if (self.flushing) {
                // Wait for the current flusher, then either observe its LSN
                // advance or become the next flusher for entries it missed.
                self.cond.wait(&self.mu);
                continue;
            }

            if (self.write_buf.items.len == 0) {
                self.mu.unlock();
                return error.WALNotDurable;
            }

            // We are the flusher
            self.flushing = true;
            // Snapshot the buffer and target LSN under the lock
            var to_write: std.ArrayList(u8) = self.write_buf;
            const target = self.next_lsn.load(.monotonic) - 1;
            self.write_buf = .empty;
            self.mu.unlock();

            // ── I/O outside lock ──────────────────────────────────────────────
            const io_err = self.writeAndSync(to_write.items);
            to_write.deinit(self.allocator);
            // ─────────────────────────────────────────────────────────────────

            self.mu.lock();
            if (io_err) |e| {
                self.last_flush_error = e;
            } else {
                self.synced_lsn = target;
                self.last_flush_error = null;
            }
            self.flushing = false;
            self.cond.broadcast();

            if (io_err) |e| {
                self.mu.unlock();
                return e;
            }
        }
        self.mu.unlock();
    }

    // ── Checkpoint ────────────────────────────────────────────────────────────

    /// Write a checkpoint barrier entry, flush, then truncate the WAL file.
    /// After checkpoint all data is durable in page files, so the WAL can be
    /// safely cleared to reclaim disk space.
    pub fn checkpoint(self: *WAL, db_tag: u8) !void {
        var p: [8]u8 = undefined;
        std.mem.writeInt(u64, &p, self.checkpoint_lsn, .little);
        const lsn = try self.write(0, .checkpoint, db_tag, FLAG_COMMIT, &p);
        self.checkpoint_lsn = lsn;
        // Force flush
        self.mu.lock();
        if (self.last_flush_error) |e| {
            self.mu.unlock();
            return e;
        }
        self.flushing = true;
        var to_write = self.write_buf;
        self.write_buf = .empty;
        self.mu.unlock();
        const io_err = self.writeAndSync(to_write.items);
        to_write.deinit(self.allocator);
        if (io_err) |e| {
            self.mu.lock();
            self.last_flush_error = e;
            self.flushing = false;
            self.cond.broadcast();
            self.mu.unlock();
            return e;
        }

        // Truncate WAL — all data is checkpointed to page files.
        self.file.seekTo(0) catch {};
        self.file.setEndPos(0) catch {};
        self.append_offset = 0;

        self.mu.lock();
        self.synced_lsn = lsn;
        self.last_flush_error = null;
        self.flushing = false;
        self.cond.broadcast();
        self.mu.unlock();
    }

    // ── Recovery ──────────────────────────────────────────────────────────────

    /// Replay WAL from the beginning.  apply_fn is called for every committed
    /// entry with lsn > skip_before_lsn.  On return the file pointer is at EOF
    /// and ready for new appends.
    pub fn recover(
        self: *WAL,
        skip_before_lsn: u64,
        apply_fn: *const fn (entry: Entry) anyerror!void,
        allocator: std.mem.Allocator,
    ) !void {
        try self.file.seekTo(0);

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
        try self.file.seekTo(0);
        var max_lsn: u64 = 0;
        var valid_end: u64 = 0;
        {
            var it = EntryIterator.init(self.file, allocator);
            defer it.deinit();
            while (try it.next()) |e| {
                max_lsn = @max(max_lsn, e.lsn);
                if (e.lsn <= skip_before_lsn) continue;
                if (!committed.contains(e.txn_id)) continue;
                if (e.op_code == .txn_commit or e.op_code == .txn_begin or
                    e.op_code == .txn_abort or e.op_code == .checkpoint) continue;
                try apply_fn(e);
            }
            valid_end = it.valid_end;
        }

        const stat = try self.file.stat();
        if (valid_end < stat.size) try self.file.setEndPos(valid_end);

        // ── Advance LSN counter and seek to EOF for new writes ────────────────
        self.next_lsn.store(max_lsn + 1, .release);
        self.synced_lsn = max_lsn;
        self.append_offset = valid_end;
        try self.file.seekTo(valid_end);
        std.log.info("WAL recovery: replayed up to lsn={d}", .{max_lsn});
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    inline fn paddingTo8(n: usize) usize {
        return (8 - (n % 8)) % 8;
    }

    fn writeAndSync(self: *WAL, bytes: []const u8) ?anyerror {
        if (bytes.len >= self.uring_min_write_and_sync) {
            if (self.uring) |*ring| {
                compat.uringWriteAndSyncAt(ring, self.file, bytes, self.append_offset) catch {
                    compat.deinitLinuxUring(ring);
                    self.uring = null;
                };
                if (self.uring != null) {
                    self.append_offset += bytes.len;
                    self.uring_flushes += 1;
                    return null;
                }
            }
        }

        self.file.pwriteAll(bytes, self.append_offset) catch |e| return e;
        self.file.syncData() catch |e| return e;
        self.append_offset += bytes.len;
        self.sync_flushes += 1;
        return null;
    }
};

// ── Entry iterator (for recovery) ────────────────────────────────────────────

const EntryIterator = struct {
    reader: compat.GenericBufferedReader(4096, compat.File.Reader),
    buf: []u8,
    allocator: std.mem.Allocator,
    pos: u64,
    valid_end: u64,

    fn init(file: compat.File, allocator: std.mem.Allocator) EntryIterator {
        return .{
            .reader = compat.bufferedReader(file.reader()),
            .buf = &.{},
            .allocator = allocator,
            .pos = 0,
            .valid_end = 0,
        };
    }

    fn deinit(self: *EntryIterator) void {
        if (self.buf.len > 0) self.allocator.free(self.buf);
    }

    /// Returns null at EOF or on a corrupt entry.
    fn next(self: *EntryIterator) !?Entry {
        var hdr_buf: [HEADER_SIZE]u8 = undefined;
        const entry_start = self.pos;
        const n = self.reader.reader().readAll(&hdr_buf) catch return null;
        self.pos += n;
        if (n == 0) return null;
        if (n < HEADER_SIZE) return null;

        const hdr: *const EntryHeader = @ptrCast(&hdr_buf);
        if (hdr.lsn == 0) return null; // padding / sentinel
        if (hdr.length > MAX_ENTRY_PAYLOAD_U32) return null; // absurd/corrupt length

        // Read payload
        const payload_len: usize = @intCast(hdr.length);
        if (payload_len > 0) {
            if (self.buf.len < payload_len) {
                if (self.buf.len > 0) self.allocator.free(self.buf);
                self.buf = try self.allocator.alloc(u8, payload_len);
            }
            const p = self.reader.reader().readAll(self.buf[0..payload_len]) catch return null;
            self.pos += p;
            if (p < payload_len) return null;
        }

        const payload: []const u8 = if (payload_len > 0) self.buf[0..payload_len] else &.{};

        // Verify CRC
        const expected = entryChecksum(&hdr_buf, payload);
        if (expected != hdr.crc32) return null; // corrupt — stop recovery

        // Skip padding
        const pad = WAL.paddingTo8(HEADER_SIZE + payload_len);
        if (pad > 0) {
            var skip: [7]u8 = undefined;
            const skipped = self.reader.reader().readAll(skip[0..pad]) catch return null;
            self.pos += skipped;
            if (skipped < pad) return null;
        }
        self.valid_end = entry_start + HEADER_SIZE + payload_len + pad;

        return Entry{
            .lsn = hdr.lsn,
            .txn_id = hdr.txn_id,
            .op_code = @enumFromInt(hdr.op_code),
            .db_tag = hdr.db_tag,
            .flags = hdr.flags,
            .payload = payload,
        };
    }
};

// ── Tests ───────────────────────────────────────────────────────────────────

fn testingWalPath(allocator: std.mem.Allocator, tmp: *const std.testing.TmpDir, name: []const u8) ![:0]u8 {
    return std.fmt.allocPrintSentinel(allocator, ".zig-cache/tmp/{s}/{s}", .{ tmp.sub_path, name }, 0);
}

const ReplayProbe = struct {
    var count: usize = 0;
    var last_lsn: u64 = 0;
    var last_txn_id: u64 = 0;
    var last_op: OpCode = .nop;
    var last_flags: u16 = 0;
    var payload_buf: [128]u8 = undefined;
    var payload_len: usize = 0;

    fn reset() void {
        count = 0;
        last_lsn = 0;
        last_txn_id = 0;
        last_op = .nop;
        last_flags = 0;
        payload_len = 0;
    }

    fn apply(entry: Entry) !void {
        if (entry.payload.len > payload_buf.len) return error.PayloadTooLarge;
        count += 1;
        last_lsn = entry.lsn;
        last_txn_id = entry.txn_id;
        last_op = entry.op_code;
        last_flags = entry.flags;
        payload_len = entry.payload.len;
        @memcpy(payload_buf[0..payload_len], entry.payload);
    }

    fn payload() []const u8 {
        return payload_buf[0..payload_len];
    }
};

test "WAL recovery replays committed entries only" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try testingWalPath(allocator, &tmp, "committed-only.wal");
    defer allocator.free(path);

    {
        var wal = try WAL.open(path, allocator);
        _ = try wal.write(10, .doc_insert, DB_TAG_DOC, 0, "committed");
        try wal.commit(10, DB_TAG_DOC);
        _ = try wal.write(11, .doc_insert, DB_TAG_DOC, 0, "uncommitted");
        wal.close();
    }

    {
        var wal = try WAL.open(path, allocator);
        defer wal.close();

        ReplayProbe.reset();
        try wal.recover(0, ReplayProbe.apply, allocator);

        try std.testing.expectEqual(@as(usize, 1), ReplayProbe.count);
        try std.testing.expectEqual(OpCode.doc_insert, ReplayProbe.last_op);
        try std.testing.expectEqual(@as(u64, 10), ReplayProbe.last_txn_id);
        try std.testing.expectEqualStrings("committed", ReplayProbe.payload());
    }
}

test "writeCommitted buffers committed entry without forcing fsync" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try testingWalPath(allocator, &tmp, "write-committed.wal");
    defer allocator.free(path);

    {
        var wal = try WAL.open(path, allocator);
        const lsn = try wal.writeCommitted(40, .doc_insert, DB_TAG_DOC, "inline");

        try std.testing.expectEqual(@as(u64, 1), lsn);
        try std.testing.expectEqual(@as(u64, 0), wal.synced_lsn);
        try std.testing.expect(wal.write_buf.items.len > 0);

        wal.flushPending();
        try std.testing.expectEqual(lsn, wal.synced_lsn);
        wal.close();
    }

    {
        var wal = try WAL.open(path, allocator);
        defer wal.close();

        ReplayProbe.reset();
        try wal.recover(0, ReplayProbe.apply, allocator);

        try std.testing.expectEqual(@as(usize, 1), ReplayProbe.count);
        try std.testing.expectEqual(OpCode.doc_insert, ReplayProbe.last_op);
        try std.testing.expectEqual(@as(u64, 40), ReplayProbe.last_txn_id);
        try std.testing.expect((ReplayProbe.last_flags & FLAG_COMMIT) != 0);
        try std.testing.expectEqualStrings("inline", ReplayProbe.payload());
    }
}

test "commit still forces durable transaction commit record" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try testingWalPath(allocator, &tmp, "commit-durable.wal");
    defer allocator.free(path);

    var wal = try WAL.open(path, allocator);
    _ = try wal.write(50, .doc_insert, DB_TAG_DOC, 0, "durable");
    try wal.commit(50, DB_TAG_DOC);

    try std.testing.expectEqual(@as(u64, 2), wal.synced_lsn);
    try std.testing.expectEqual(@as(usize, 0), wal.write_buf.items.len);

    wal.close();
}

test "WAL recovery truncates torn tail after committed entries" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try testingWalPath(allocator, &tmp, "torn-tail.wal");
    defer allocator.free(path);

    {
        var wal = try WAL.open(path, allocator);
        _ = try wal.write(20, .doc_insert, DB_TAG_DOC, 0, "kept");
        try wal.commit(20, DB_TAG_DOC);
        wal.close();
    }

    var before_size: u64 = 0;
    {
        var file = try compat.openFileAbsolute(path, .{});
        defer file.close();
        _ = std.c.lseek(file.handle, 0, std.c.SEEK.END);
        try file.writeAll("torn-tail");
        try file.sync();
        before_size = (try file.stat()).size;
    }

    {
        var wal = try WAL.open(path, allocator);
        defer wal.close();

        ReplayProbe.reset();
        try wal.recover(0, ReplayProbe.apply, allocator);

        const after_size = (try wal.file.stat()).size;
        try std.testing.expect(after_size < before_size);
        try std.testing.expectEqual(@as(usize, 1), ReplayProbe.count);
        try std.testing.expectEqualStrings("kept", ReplayProbe.payload());
        try std.testing.expectEqual(@as(u64, 3), wal.next_lsn.load(.acquire));
        try std.testing.expectEqual(@as(u64, 2), wal.synced_lsn);
    }
}

test "WAL recovery treats oversized payload length as corrupt tail" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try testingWalPath(allocator, &tmp, "oversized-tail.wal");
    defer allocator.free(path);

    {
        var file = try compat.createFileAbsolute(path, .{});
        defer file.close();

        var hdr = EntryHeader{
            .lsn = 1,
            .length = MAX_ENTRY_PAYLOAD_U32 + 1,
            .crc32 = 0,
            .op_code = @intFromEnum(OpCode.doc_insert),
            .db_tag = DB_TAG_DOC,
            .flags = FLAG_COMMIT,
            .txn_id = 60,
            .reserved = 0,
        };
        hdr.crc32 = entryChecksum(std.mem.asBytes(&hdr), &.{});
        try file.writeAll(std.mem.asBytes(&hdr));
        try file.sync();
    }

    {
        var wal = try WAL.open(path, allocator);
        defer wal.close();

        ReplayProbe.reset();
        try wal.recover(0, ReplayProbe.apply, allocator);

        const after_size = (try wal.file.stat()).size;
        try std.testing.expectEqual(@as(usize, 0), ReplayProbe.count);
        try std.testing.expectEqual(@as(u64, 0), after_size);
        try std.testing.expectEqual(@as(u64, 1), wal.next_lsn.load(.acquire));
        try std.testing.expectEqual(@as(u64, 0), wal.synced_lsn);
    }
}

test "flushPending records write failure without advancing synced lsn" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try testingWalPath(allocator, &tmp, "flush-error.wal");
    defer allocator.free(path);

    var wal = try WAL.open(path, allocator);
    const lsn = try wal.write(30, .doc_insert, DB_TAG_DOC, 0, "not-durable");

    wal.file.close();
    wal.file.handle = -1;
    wal.flushPending();

    try std.testing.expect(wal.synced_lsn < lsn);
    try std.testing.expect(wal.lastFlushError() != null);
    try std.testing.expect(wal.lastFlushError().? == error.WriteError);

    wal.write_buf.deinit(allocator);
}
