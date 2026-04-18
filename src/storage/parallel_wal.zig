const std = @import("std");
const compat = @import("compat");
const runtime = @import("runtime");
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
/// buffer. Appends are lock-free: writers atomically reserve space via
/// `fetchAdd` on `pos`.
pub const WALSegment = struct {
    segment_id: u32,
    fd: std.fs.File,
    buf: []align(4096) u8,
    pos: std.atomic.Value(u32),
    flushed_pos: u32,

    pub const BUF_SIZE: u32 = 65536;

    pub fn init(alloc: Allocator, data_dir: []const u8, id: u32) !WALSegment {
        // Build path like "data_dir/wal_seg_0003"
        var path_buf: [512]u8 = undefined;
        const path = std.fmt.bufPrint(&path_buf, "{s}/wal_seg_{d:0>4}", .{ data_dir, id }) catch return error.PathTooLong;

        const fd = try compat.fs.cwdCreateFile(path, .{ .truncate = true });

        const buf = try alloc.alignedAlloc(u8, .fromByteUnits(4096), BUF_SIZE);
        @memset(buf, 0);

        return WALSegment{
            .segment_id = id,
            .fd = fd,
            .buf = buf,
            .pos = std.atomic.Value(u32).init(0),
            .flushed_pos = 0,
        };
    }

    pub fn deinit(self: *WALSegment, alloc: Allocator) void {
        self.fd.close();
        alloc.free(self.buf);
    }

    /// Append an entry to the segment buffer. Returns the offset where data
    /// was written, or `error.SegmentFull` if the buffer cannot hold the entry.
    pub fn append(self: *WALSegment, op: OpType, key_hash: u64, data: []const u8) !u32 {
        const total: u32 = ENTRY_HEADER_SIZE + @as(u32, @intCast(data.len));

        // Reserve space atomically — lock-free.
        const offset = self.pos.fetchAdd(total, .monotonic);
        if (offset + total > BUF_SIZE) {
            // Roll back so other threads see accurate remaining space.
            _ = self.pos.fetchSub(total, .monotonic);
            return error.SegmentFull;
        }

        // Write header: [op:1][key_hash:8][len:4]
        self.buf[offset] = @intFromEnum(op);
        @memcpy(self.buf[offset + 1 .. offset + 9], std.mem.asBytes(&key_hash));
        const data_len: u32 = @intCast(data.len);
        @memcpy(self.buf[offset + 9 .. offset + 13], std.mem.asBytes(&data_len));

        // Write payload.
        @memcpy(self.buf[offset + ENTRY_HEADER_SIZE .. offset + total], data);

        return offset;
    }

    /// Flush dirty bytes to the underlying file. Called by the group-commit
    /// flusher — NOT per-transaction.
    pub fn flush(self: *WALSegment) !void {
        const current = self.pos.load(.acquire);
        if (current <= self.flushed_pos) return;

        const dirty = self.buf[self.flushed_pos..current];
        const written = try self.fd.write(dirty);
        if (written != dirty.len) return error.ShortWrite;

        self.flushed_pos = current;
    }

    /// Fsync the segment file to durable storage.
    pub fn sync(self: *WALSegment) !void {
        try self.fd.sync();
    }

    /// Read back an entry at the given buffer offset.  Returns the fields and
    /// a slice into the buffer for the payload.
    pub fn readEntry(self: *const WALSegment, offset: u32) !struct {
        op: OpType,
        key_hash: u64,
        data: []const u8,
        next_offset: u32,
    } {
        const end = self.pos.load(.acquire);
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
    wake_signal: std.atomic.Value(u32),

    pub fn init(alloc: Allocator, data_dir: []const u8, n_segments: u32) !*ParallelWAL {
        const segs = try alloc.alloc(*WALSegment, n_segments);
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
            .wake_signal = std.atomic.Value(u32).init(0),
        };
        return self;
    }

    pub fn deinit(self: *ParallelWAL) void {
        self.stopFlusher();
        var i: u32 = 0;
        while (i < self.n_segments) : (i += 1) {
            self.segments[i].deinit(self.allocator);
            self.allocator.destroy(self.segments[i]);
        }
        self.allocator.free(self.segments);
        self.allocator.destroy(self);
    }

    /// Select a segment for the calling thread — thread ID modulo n_segments.
    pub fn getSegment(self: *ParallelWAL) *WALSegment {
        const tid = std.Thread.getCurrentId();
        const idx = @as(u32, @intCast(@as(u64, @bitCast(tid)) % @as(u64, self.n_segments)));
        return self.segments[idx];
    }

    /// Append an entry to the thread-local segment (lock-free).
    pub fn write(self: *ParallelWAL, op: OpType, key_hash: u64, data: []const u8) !u32 {
        const seg = self.getSegment();
        const off = try seg.append(op, key_hash, data);
        // Wake the flusher. Signal value only needs to change; actual count is ignored.
        _ = self.wake_signal.fetchAdd(1, .release);
        runtime.io.futexWake(u32, &self.wake_signal.raw, 1);
        return off;
    }

    /// Group commit: flush + fsync every segment and advance the epoch.
    /// Called by the background flusher or manually in tests.
    pub fn groupCommit(self: *ParallelWAL) !void {
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
        // Wake the flusher so it observes running=0 without waiting out its timeout.
        _ = self.wake_signal.fetchAdd(1, .release);
        runtime.io.futexWake(u32, &self.wake_signal.raw, 1);
        if (self.flusher_thread) |t| {
            t.join();
            self.flusher_thread = null;
        }
    }

    /// Returns true if any segment has unflushed bytes.
    fn anyDirty(self: *ParallelWAL) bool {
        var i: u32 = 0;
        while (i < self.n_segments) : (i += 1) {
            const seg = self.segments[i];
            if (seg.pos.load(.acquire) > seg.flushed_pos) return true;
        }
        return false;
    }

    fn flusherLoop(self: *ParallelWAL) void {
        while (self.running.load(.acquire) == 1) {
            if (self.anyDirty()) {
                self.groupCommit() catch {};
                continue;
            }
            // Idle. Sleep on futex; writes or stopFlusher will wake us.
            const cur = self.wake_signal.load(.acquire);
            if (self.running.load(.acquire) == 0) break;
            runtime.io.futexWaitTimeout(
                u32,
                &self.wake_signal.raw,
                cur,
                .{ .duration = .{ .raw = std.Io.Duration.fromNanoseconds(10 * std.time.ns_per_ms), .clock = .awake } },
            ) catch {};
        }
        // Final flush on shutdown.
        self.groupCommit() catch {};
    }
};

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

test "WALSegment single-threaded append and read" {
    const alloc = std.testing.allocator;

    // Use a temp directory.
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const tmp_path = try tmp.dir.realpathAlloc(alloc, ".");
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

test "ParallelWAL multi-threaded writes" {
    const alloc = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const tmp_path = try tmp.dir.realpathAlloc(alloc, ".");
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
    var i: u32 = 0;
    while (i < n_segments) : (i += 1) {
        total_bytes += wal.segments[i].pos.load(.acquire);
    }

    const expected_bytes: u64 = N_THREADS * WRITES_PER_THREAD * (ENTRY_HEADER_SIZE + 4); // "data" = 4 bytes
    try std.testing.expectEqual(expected_bytes, total_bytes);
}

test "ParallelWAL group commit advances epoch" {
    const alloc = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const tmp_path = try tmp.dir.realpathAlloc(alloc, ".");
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
    const tmp_path = try tmp.dir.realpathAlloc(alloc, ".");
    defer alloc.free(tmp_path);

    var wal = try ParallelWAL.init(alloc, tmp_path, 2);
    defer wal.deinit();

    try wal.startFlusher();

    // Write some data and let the flusher pick it up.
    _ = try wal.write(.put, 99, "flusher-test");

    // Sleep a bit to let the flusher run at least once (~1 ms interval).
    compat.threadSleep(10 * std.time.ns_per_ms);

    const epoch = wal.current_epoch.load(.acquire);
    try std.testing.expect(epoch >= 1);

    wal.stopFlusher();
}
