const std = @import("std");
const builtin = @import("builtin");
const compat = @import("compat");
const linux = std.os.linux;
const Allocator = std.mem.Allocator;

/// Operation types for WAL entries.
pub const OpType = enum(u8) {
    put = 1,
    delete = 2,
    checkpoint = 3,
};

/// Header prepended to every WAL entry within a segment buffer.
/// Layout: [op:1][key_hash:8][len:4][data:len]
const ENTRY_HEADER_SIZE = 13;

/// Per-core WAL segment. Each segment owns a file and a page-aligned write
/// buffer. Appends reserve space with `pos`, then publish completed entries
/// into `published_pos` in reservation order so flush never reads a partial
/// entry.
pub const WALSegment = struct {
    segment_id: u32,
    fd: compat.File,
    uring: ?compat.LinuxUring,
    buf: []align(4096) u8,
    pos: std.atomic.Value(u32),
    published_pos: std.atomic.Value(u32),
    flushed_pos: u32,

    pub const BUF_SIZE: u32 = 65536;

    pub fn init(alloc: Allocator, data_dir: []const u8, id: u32) !WALSegment {
        // Build path like "data_dir/wal_seg_0003"
        var path_buf: [512]u8 = undefined;
        const path = std.fmt.bufPrint(&path_buf, "{s}/wal_seg_{d:0>4}", .{ data_dir, id }) catch return error.PathTooLong;

        const fd = try compat.cwd().createFile(path, .{ .truncate = true });

        const buf = try alloc.alignedAlloc(u8, .fromByteUnits(4096), BUF_SIZE);
        @memset(buf, 0);

        return WALSegment{
            .segment_id = id,
            .fd = fd,
            .uring = compat.initLinuxUring(8),
            .buf = buf,
            .pos = std.atomic.Value(u32).init(0),
            .published_pos = std.atomic.Value(u32).init(0),
            .flushed_pos = 0,
        };
    }

    pub fn deinit(self: *WALSegment, alloc: Allocator) void {
        if (self.uring) |*ring| {
            compat.deinitLinuxUring(ring);
            self.uring = null;
        }
        self.fd.close();
        alloc.free(self.buf);
    }

    /// Append an entry to the segment buffer. Returns the offset where data
    /// was written, or `error.SegmentFull` if the buffer cannot hold the entry.
    pub fn append(self: *WALSegment, op: OpType, key_hash: u64, data: []const u8) !u32 {
        if (data.len > @as(usize, BUF_SIZE - ENTRY_HEADER_SIZE)) return error.SegmentFull;

        const data_len: u32 = @intCast(data.len);
        const total: u32 = ENTRY_HEADER_SIZE + data_len;
        const offset = try self.reserve(total);

        // Write header: [op:1][key_hash:8][len:4]
        self.buf[offset] = @intFromEnum(op);
        @memcpy(self.buf[offset + 1 .. offset + 9], std.mem.asBytes(&key_hash));
        @memcpy(self.buf[offset + 9 .. offset + 13], std.mem.asBytes(&data_len));

        // Write payload.
        @memcpy(self.buf[offset + ENTRY_HEADER_SIZE .. offset + total], data);

        self.publish(offset, offset + total);
        return offset;
    }

    fn reserve(self: *WALSegment, total: u32) !u32 {
        while (true) {
            const current = self.pos.load(.monotonic);
            if (current > BUF_SIZE or total > BUF_SIZE - current) return error.SegmentFull;

            if (self.pos.cmpxchgWeak(current, current + total, .monotonic, .monotonic) == null) {
                return current;
            }

            std.atomic.spinLoopHint();
        }
    }

    fn tryPublish(self: *WALSegment, offset: u32, end: u32) bool {
        return self.published_pos.cmpxchgStrong(offset, end, .release, .acquire) == null;
    }

    fn publish(self: *WALSegment, offset: u32, end: u32) void {
        while (!self.tryPublish(offset, end)) {
            std.atomic.spinLoopHint();
        }
    }

    /// Flush dirty bytes to the underlying file. Called by the group-commit
    /// flusher — NOT per-transaction.
    pub fn flush(self: *WALSegment) !void {
        const current = self.published_pos.load(.acquire);
        if (current <= self.flushed_pos) return;

        const dirty = self.buf[self.flushed_pos..current];
        if (self.uring) |*ring| {
            compat.uringWriteAllAt(ring, self.fd, dirty, self.flushed_pos) catch {
                compat.deinitLinuxUring(ring);
                self.uring = null;
            };
            if (self.uring == null) try self.fd.pwriteAll(dirty, self.flushed_pos);
        } else {
            try self.fd.pwriteAll(dirty, self.flushed_pos);
        }

        self.flushed_pos = current;
    }

    /// Fsync the segment file to durable storage.
    pub fn sync(self: *WALSegment) !void {
        if (self.uring) |*ring| {
            compat.uringFsync(ring, self.fd) catch {
                compat.deinitLinuxUring(ring);
                self.uring = null;
            };
            if (self.uring == null) try self.fd.syncData();
        } else {
            try self.fd.syncData();
        }
    }

    /// Read back an entry at the given buffer offset.  Returns the fields and
    /// a slice into the buffer for the payload.
    pub fn readEntry(self: *const WALSegment, offset: u32) !struct {
        op: OpType,
        key_hash: u64,
        data: []const u8,
        next_offset: u32,
    } {
        const end = self.published_pos.load(.acquire);
        if (offset + ENTRY_HEADER_SIZE > end) return error.InvalidOffset;

        const op: OpType = @enumFromInt(self.buf[offset]);
        const key_hash = std.mem.bytesToValue(u64, self.buf[offset + 1 .. offset + 9]);
        const data_len = std.mem.bytesToValue(u32, self.buf[offset + 9 .. offset + 13]);

        const data_end = offset + ENTRY_HEADER_SIZE + data_len;
        if (data_end > end) return error.InvalidOffset;

        return .{
            .op = op,
            .key_hash = key_hash,
            .data = self.buf[offset + ENTRY_HEADER_SIZE .. data_end],
            .next_offset = data_end,
        };
    }
};

/// Parallel WAL with per-core segments and epoch-based group commit.
///
/// Design (Silo-style):
///   - One WAL segment per core/thread → no cross-thread contention on append.
///   - Lock-free append within each segment via atomic position counter.
///   - Background flusher calls `groupCommit()` every ~1 ms, advancing the
///     global epoch and fsyncing all segments in one batch.
///   - No per-transaction fsync — throughput scales linearly with cores.
pub const ParallelWAL = struct {
    segments: []*WALSegment,
    n_segments: u32,
    current_epoch: std.atomic.Value(u64),
    allocator: Allocator,
    flusher_thread: ?std.Thread,
    running: std.atomic.Value(u8),
    uring: ?compat.LinuxUring,
    dirty_ends: []u32,
    dirty_lens: []usize,

    const MAX_URING_SEGMENTS: u32 = 256;
    const URING_QUEUE_ENTRIES: u16 = 512;
    const SYNC_USER_DATA_BASE: u64 = MAX_URING_SEGMENTS + 1;

    pub fn init(alloc: Allocator, data_dir: []const u8, n_segments: u32) !*ParallelWAL {
        const segs = try alloc.alloc(*WALSegment, n_segments);
        const dirty_ends = try alloc.alloc(u32, n_segments);
        errdefer alloc.free(dirty_ends);
        const dirty_lens = try alloc.alloc(usize, n_segments);
        errdefer alloc.free(dirty_lens);
        @memset(dirty_ends, 0);
        @memset(dirty_lens, 0);
        var inited: u32 = 0;
        errdefer {
            var j: u32 = 0;
            while (j < inited) : (j += 1) {
                segs[j].deinit(alloc);
                alloc.destroy(segs[j]);
            }
            alloc.free(segs);
        }

        var i: u32 = 0;
        while (i < n_segments) : (i += 1) {
            const seg = try alloc.create(WALSegment);
            seg.* = try WALSegment.init(alloc, data_dir, i);
            segs[i] = seg;
            inited = i + 1;
        }

        const self = try alloc.create(ParallelWAL);
        self.* = ParallelWAL{
            .segments = segs,
            .n_segments = n_segments,
            .current_epoch = std.atomic.Value(u64).init(0),
            .allocator = alloc,
            .flusher_thread = null,
            .running = std.atomic.Value(u8).init(0),
            .uring = compat.initLinuxUring(URING_QUEUE_ENTRIES),
            .dirty_ends = dirty_ends,
            .dirty_lens = dirty_lens,
        };
        return self;
    }

    pub fn deinit(self: *ParallelWAL) void {
        self.stopFlusher();
        if (self.uring) |*ring| {
            compat.deinitLinuxUring(ring);
            self.uring = null;
        }
        var i: u32 = 0;
        while (i < self.n_segments) : (i += 1) {
            self.segments[i].deinit(self.allocator);
            self.allocator.destroy(self.segments[i]);
        }
        self.allocator.free(self.dirty_ends);
        self.allocator.free(self.dirty_lens);
        self.allocator.free(self.segments);
        self.allocator.destroy(self);
    }

    /// Select a segment for the calling thread — thread ID modulo n_segments.
    pub fn getSegment(self: *ParallelWAL) *WALSegment {
        const tid = std.Thread.getCurrentId();
        const idx = @as(u32, @intCast(@as(u64, @intCast(tid)) % @as(u64, self.n_segments)));
        return self.segments[idx];
    }

    /// Append an entry to the thread-local segment (lock-free).
    pub fn write(self: *ParallelWAL, op: OpType, key_hash: u64, data: []const u8) !u32 {
        const seg = self.getSegment();
        return seg.append(op, key_hash, data);
    }

    pub fn usingLinuxUring(self: *const ParallelWAL) bool {
        return self.uring != null;
    }

    /// Group commit: flush + fsync every segment and advance the epoch.
    /// Called by the background flusher or manually in tests.
    pub fn groupCommit(self: *ParallelWAL) !void {
        if (self.groupCommitWithUring()) {
            _ = self.current_epoch.fetchAdd(1, .release);
            return;
        }

        // Phase 1: flush dirty buffers to OS page cache.
        var i: u32 = 0;
        while (i < self.n_segments) : (i += 1) {
            try self.segments[i].flush();
        }

        // Phase 2: fsync all segments for durability.
        i = 0;
        while (i < self.n_segments) : (i += 1) {
            try self.segments[i].sync();
        }

        // Advance epoch — readers use this to know data is durable.
        _ = self.current_epoch.fetchAdd(1, .release);
    }

    fn groupCommitWithUring(self: *ParallelWAL) bool {
        if (comptime builtin.os.tag != .linux) return false;

        if (self.n_segments == 0 or self.n_segments > MAX_URING_SEGMENTS) return false;
        const ring = if (self.uring) |*r| r else return false;

        @memset(self.dirty_lens, 0);
        var dirty_segments: u32 = 0;
        var i: u32 = 0;
        while (i < self.n_segments) : (i += 1) {
            const seg = self.segments[i];
            const current = seg.published_pos.load(.acquire);
            self.dirty_ends[i] = current;
            if (current <= seg.flushed_pos) continue;

            const dirty = seg.buf[seg.flushed_pos..current];
            self.dirty_lens[i] = dirty.len;
            const write_sqe = ring.write(writeUserData(i), seg.fd.handle, dirty, seg.flushed_pos) catch return self.disableUring();
            write_sqe.flags |= linux.IOSQE_IO_LINK;
            _ = ring.fsync(syncUserData(i), seg.fd.handle, linux.IORING_FSYNC_DATASYNC) catch return self.disableUring();
            dirty_segments += 1;
        }

        if (dirty_segments == 0) return true;
        _ = ring.submit_and_wait(dirty_segments * 2) catch return self.disableUring();
        if (!self.collectLinkedUringCompletions(dirty_segments * 2)) return self.disableUring();

        i = 0;
        while (i < self.n_segments) : (i += 1) {
            if (self.dirty_lens[i] == 0) continue;
            self.segments[i].flushed_pos = self.dirty_ends[i];
        }
        return true;
    }

    fn collectLinkedUringCompletions(self: *ParallelWAL, expected: u32) bool {
        const ring = if (self.uring) |*r| r else return false;
        var completed: u32 = 0;
        while (completed < expected) : (completed += 1) {
            const cqe = ring.copy_cqe() catch return false;
            if (isSyncUserData(cqe.user_data)) {
                _ = self.cqeSyncSegmentIndex(cqe.user_data) orelse return false;
                if (cqe.res < 0) return false;
            } else {
                const idx = self.cqeWriteSegmentIndex(cqe.user_data) orelse return false;
                if (cqe.res < 0) return false;
                const n: usize = @intCast(cqe.res);
                if (n != self.dirty_lens[idx]) return false;
            }
        }
        return true;
    }

    fn writeUserData(segment_index: u32) u64 {
        return @as(u64, segment_index) + 1;
    }

    fn syncUserData(segment_index: u32) u64 {
        return SYNC_USER_DATA_BASE + @as(u64, segment_index);
    }

    fn isSyncUserData(user_data: u64) bool {
        return user_data >= SYNC_USER_DATA_BASE;
    }

    fn cqeWriteSegmentIndex(self: *const ParallelWAL, user_data: u64) ?usize {
        if (user_data == 0 or isSyncUserData(user_data)) return null;
        const idx: usize = @intCast(user_data - 1);
        if (idx >= self.n_segments) return null;
        return idx;
    }

    fn cqeSyncSegmentIndex(self: *const ParallelWAL, user_data: u64) ?usize {
        if (!isSyncUserData(user_data)) return null;
        const idx: usize = @intCast(user_data - SYNC_USER_DATA_BASE);
        if (idx >= self.n_segments) return null;
        return idx;
    }

    fn disableUring(self: *ParallelWAL) bool {
        if (self.uring) |*ring| {
            compat.deinitLinuxUring(ring);
            self.uring = null;
        }
        return false;
    }

    /// Start the background flusher thread (~1 ms group commit interval).
    pub fn startFlusher(self: *ParallelWAL) !void {
        if (self.running.load(.acquire) == 1) return;
        self.running.store(1, .release);
        self.flusher_thread = try std.Thread.spawn(.{}, flusherLoop, .{self});
    }

    /// Stop the background flusher and join the thread.
    pub fn stopFlusher(self: *ParallelWAL) void {
        if (self.running.load(.acquire) == 0) return;
        self.running.store(0, .release);
        if (self.flusher_thread) |t| {
            t.join();
            self.flusher_thread = null;
        }
    }

    fn flusherLoop(self: *ParallelWAL) void {
        while (self.running.load(.acquire) == 1) {
            self.groupCommit() catch {};
            compat.sleep(1 * std.time.ns_per_ms);
        }
        // Final flush on shutdown.
        self.groupCommit() catch {};
    }
};

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

fn testingTmpPath(alloc: Allocator, tmp: *const std.testing.TmpDir) ![]u8 {
    return std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}", .{tmp.sub_path});
}

test "WALSegment single-threaded append and read" {
    const alloc = std.testing.allocator;

    // Use a temp directory.
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const tmp_path = try testingTmpPath(alloc, &tmp);
    defer alloc.free(tmp_path);

    var seg = try WALSegment.init(alloc, tmp_path, 0);
    defer seg.deinit(alloc);

    // Write two entries.
    const off1 = try seg.append(.put, 0xDEAD, "hello");
    const off2 = try seg.append(.delete, 0xBEEF, "world");

    // Read them back.
    const e1 = try seg.readEntry(off1);
    try std.testing.expectEqual(OpType.put, e1.op);
    try std.testing.expectEqual(@as(u64, 0xDEAD), e1.key_hash);
    try std.testing.expectEqualStrings("hello", e1.data);

    const e2 = try seg.readEntry(off2);
    try std.testing.expectEqual(OpType.delete, e2.op);
    try std.testing.expectEqual(@as(u64, 0xBEEF), e2.key_hash);
    try std.testing.expectEqualStrings("world", e2.data);
}

test "WALSegment publish requires contiguous offsets" {
    const alloc = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const tmp_path = try testingTmpPath(alloc, &tmp);
    defer alloc.free(tmp_path);

    var seg = try WALSegment.init(alloc, tmp_path, 0);
    defer seg.deinit(alloc);

    const first_total: u32 = ENTRY_HEADER_SIZE + 5;
    const second_total: u32 = ENTRY_HEADER_SIZE + 6;

    try std.testing.expect(!seg.tryPublish(first_total, first_total + second_total));
    try std.testing.expectEqual(@as(u32, 0), seg.published_pos.load(.acquire));

    try std.testing.expect(seg.tryPublish(0, first_total));
    try std.testing.expectEqual(first_total, seg.published_pos.load(.acquire));

    try std.testing.expect(seg.tryPublish(first_total, first_total + second_total));
    try std.testing.expectEqual(first_total + second_total, seg.published_pos.load(.acquire));
}

test "WALSegment flush ignores reserved unpublished bytes" {
    const alloc = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const tmp_path = try testingTmpPath(alloc, &tmp);
    defer alloc.free(tmp_path);

    var seg = try WALSegment.init(alloc, tmp_path, 0);
    defer seg.deinit(alloc);

    const payload = "complete";
    const data_len: u32 = payload.len;
    const total: u32 = ENTRY_HEADER_SIZE + data_len;
    const key_hash: u64 = 0xABCDEF;

    seg.pos.store(total, .release);
    seg.buf[0] = @intFromEnum(OpType.put);
    @memcpy(seg.buf[1..9], std.mem.asBytes(&key_hash));
    @memcpy(seg.buf[9..13], std.mem.asBytes(&data_len));
    @memcpy(seg.buf[ENTRY_HEADER_SIZE..@as(usize, total)], payload);

    try std.testing.expectError(error.InvalidOffset, seg.readEntry(0));

    try seg.flush();
    try std.testing.expectEqual(@as(u32, 0), seg.flushed_pos);
    var stat = try seg.fd.stat();
    try std.testing.expectEqual(@as(u64, 0), stat.size);

    seg.published_pos.store(total, .release);

    const entry = try seg.readEntry(0);
    try std.testing.expectEqual(OpType.put, entry.op);
    try std.testing.expectEqual(key_hash, entry.key_hash);
    try std.testing.expectEqualStrings(payload, entry.data);

    try seg.flush();
    try std.testing.expectEqual(total, seg.flushed_pos);
    stat = try seg.fd.stat();
    try std.testing.expectEqual(@as(u64, total), stat.size);

    var persisted: [ENTRY_HEADER_SIZE + payload.len]u8 = undefined;
    var path_buf: [512]u8 = undefined;
    const wal_path = try std.fmt.bufPrint(&path_buf, "{s}/wal_seg_{d:0>4}", .{ tmp_path, 0 });
    var read_file = try compat.cwd().openFile(wal_path, .{});
    defer read_file.close();

    const read = try read_file.readAll(persisted[0..]);
    try std.testing.expectEqual(@as(usize, total), read);
    try std.testing.expectEqualStrings(payload, persisted[ENTRY_HEADER_SIZE..]);
}

test "ParallelWAL multi-threaded writes" {
    const alloc = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const tmp_path = try testingTmpPath(alloc, &tmp);
    defer alloc.free(tmp_path);

    const n_segments: u32 = 4;
    var wal = try ParallelWAL.init(alloc, tmp_path, n_segments);
    defer wal.deinit();

    const N_THREADS = 8;
    const WRITES_PER_THREAD = 100;

    const Worker = struct {
        fn run(w: *ParallelWAL) void {
            var i: usize = 0;
            while (i < WRITES_PER_THREAD) : (i += 1) {
                _ = w.write(.put, @as(u64, i), "data") catch {};
            }
        }
    };

    var threads: [N_THREADS]std.Thread = undefined;
    var spawned: usize = 0;

    var t: usize = 0;
    while (t < N_THREADS) : (t += 1) {
        threads[t] = try std.Thread.spawn(.{}, Worker.run, .{wal});
        spawned += 1;
    }

    t = 0;
    while (t < spawned) : (t += 1) {
        threads[t].join();
    }

    // Verify: total bytes written across all segments should account for
    // N_THREADS * WRITES_PER_THREAD entries.
    var total_bytes: u64 = 0;
    var total_published_bytes: u64 = 0;
    var i: u32 = 0;
    while (i < n_segments) : (i += 1) {
        total_bytes += wal.segments[i].pos.load(.acquire);
        total_published_bytes += wal.segments[i].published_pos.load(.acquire);
    }

    const expected_bytes: u64 = N_THREADS * WRITES_PER_THREAD * (ENTRY_HEADER_SIZE + 4); // "data" = 4 bytes
    try std.testing.expectEqual(expected_bytes, total_bytes);
    try std.testing.expectEqual(expected_bytes, total_published_bytes);
}

test "ParallelWAL group commit advances epoch" {
    const alloc = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const tmp_path = try testingTmpPath(alloc, &tmp);
    defer alloc.free(tmp_path);

    var wal = try ParallelWAL.init(alloc, tmp_path, 2);
    defer wal.deinit();

    try std.testing.expectEqual(@as(u64, 0), wal.current_epoch.load(.acquire));

    _ = try wal.write(.put, 42, "test-value");
    try wal.groupCommit();

    try std.testing.expectEqual(@as(u64, 1), wal.current_epoch.load(.acquire));

    try wal.groupCommit();
    try std.testing.expectEqual(@as(u64, 2), wal.current_epoch.load(.acquire));
}

test "ParallelWAL background flusher runs" {
    const alloc = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const tmp_path = try testingTmpPath(alloc, &tmp);
    defer alloc.free(tmp_path);

    var wal = try ParallelWAL.init(alloc, tmp_path, 2);
    defer wal.deinit();

    try wal.startFlusher();

    // Write some data and let the flusher pick it up.
    _ = try wal.write(.put, 99, "flusher-test");

    // Sleep a bit to let the flusher run at least once (~1 ms interval).
    compat.sleep(10 * std.time.ns_per_ms);

    const epoch = wal.current_epoch.load(.acquire);
    try std.testing.expect(epoch >= 1);

    wal.stopFlusher();
}
