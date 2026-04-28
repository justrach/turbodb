//! Epoch-based reclamation — safe memory reclaim for MVCC version chains.
//!
//! How it works:
//!   1. A global monotonic timestamp advances on every writer commit.
//!   2. Each reader "pins" the current epoch when it starts a transaction.
//!   3. The GC coordinator finds the minimum pinned epoch across all readers.
//!   4. Any object (document version, edge, etc.) whose commit_ts is strictly
//!      less than the minimum pinned epoch is invisible to ALL current readers.
//!      It is therefore safe to free.
//!
//! This gives readers true snapshot isolation at zero cost: no locks, no
//! reference counts, no deferred deallocation lists per-thread.
//!
//! Capacity: MAX_READERS concurrent reader threads.  Each occupies one 64-byte
//! slot to avoid false sharing.
const std = @import("std");
const compat = @import("compat");

pub const MAX_READERS: usize = 1024;
pub const EPOCH_INACTIVE: u64 = std.math.maxInt(u64);

/// One slot per reader thread.  Padded to 64 bytes to avoid false sharing.
const Slot = struct {
    epoch: std.atomic.Value(u64) = std.atomic.Value(u64).init(EPOCH_INACTIVE),
    _pad: [56]u8 = [_]u8{0} ** 56,
    comptime {
        std.debug.assert(@sizeOf(@This()) == 64);
    }
};

pub const EpochManager = struct {
    pub const EpochPoint = struct {
        epoch: u64,
        ts_ms: i64,
    };

    /// Global clock — advanced by writers on commit.
    global_ts: std.atomic.Value(u64),
    /// Per-reader pinned epoch.
    slots: []Slot,
    allocator: std.mem.Allocator,
    timeline: std.ArrayList(EpochPoint),
    timeline_mu: compat.Mutex,

    pub fn init(allocator: std.mem.Allocator) !EpochManager {
        const slots = try allocator.alloc(Slot, MAX_READERS);
        for (slots) |*s| s.epoch.store(EPOCH_INACTIVE, .release);
        return .{
            .global_ts = std.atomic.Value(u64).init(1),
            .slots = slots,
            .allocator = allocator,
            .timeline = .empty,
            .timeline_mu = .{},
        };
    }

    pub fn deinit(self: *EpochManager) void {
        self.timeline.deinit(self.allocator);
        self.allocator.free(self.slots);
    }

    // ── Writer side ───────────────────────────────────────────────────────────

    /// Called by a writer at commit.  Returns the new timestamp.
    pub fn advance(self: *EpochManager) u64 {
        return self.advanceAt(compat.milliTimestamp());
    }

    pub fn advanceAt(self: *EpochManager, ts_ms: i64) u64 {
        const epoch = self.global_ts.fetchAdd(1, .acq_rel) + 1;
        self.timeline_mu.lock();
        defer self.timeline_mu.unlock();
        self.timeline.append(self.allocator, .{ .epoch = epoch, .ts_ms = ts_ms }) catch {};
        return epoch;
    }

    /// Reserve a contiguous epoch range for a single batched commit.
    /// Returns the first epoch in the range. Timestamp lookups map to the
    /// batch's final epoch so the whole batch becomes visible together.
    pub fn advanceMany(self: *EpochManager, count: usize) u64 {
        return self.advanceManyAt(count, compat.milliTimestamp());
    }

    pub fn advanceManyAt(self: *EpochManager, count: usize, ts_ms: i64) u64 {
        if (count == 0) return self.now();
        const count_u64: u64 = @intCast(count);
        const first_epoch = self.global_ts.fetchAdd(count_u64, .acq_rel) + 1;
        const last_epoch = first_epoch + count_u64 - 1;
        self.timeline_mu.lock();
        defer self.timeline_mu.unlock();
        self.timeline.append(self.allocator, .{ .epoch = last_epoch, .ts_ms = ts_ms }) catch {};
        return first_epoch;
    }

    /// Current global timestamp (snapshot for reads).
    pub fn now(self: *const EpochManager) u64 {
        return self.global_ts.load(.acquire);
    }

    pub fn epochForTimestamp(self: *const EpochManager, ts_ms: i64) ?u64 {
        const mutable: *EpochManager = @constCast(self);
        mutable.timeline_mu.lock();
        defer mutable.timeline_mu.unlock();

        var best: ?u64 = null;
        for (mutable.timeline.items) |entry| {
            if (entry.ts_ms <= ts_ms) {
                best = entry.epoch;
            } else {
                break;
            }
        }
        return best;
    }

    // ── Reader side ───────────────────────────────────────────────────────────

    /// Pin the current epoch.  Returns (reader_slot, snapshot_ts).
    /// The caller must call unpin(slot) when done.
    pub fn pin(self: *EpochManager) struct { slot: usize, ts: u64 } {
        const ts = self.global_ts.load(.acquire);
        // Find a free slot
        for (self.slots, 0..) |*s, i| {
            const cur = s.epoch.load(.acquire);
            if (cur == EPOCH_INACTIVE) {
                if (s.epoch.cmpxchgStrong(cur, ts, .acq_rel, .monotonic) == null)
                    return .{ .slot = i, .ts = ts };
            }
        }
        // Fallback: return slot 0 (reader count exceeded MAX_READERS — very rare)
        return .{ .slot = 0, .ts = ts };
    }

    pub fn unpin(self: *EpochManager, slot: usize) void {
        self.slots[slot].epoch.store(EPOCH_INACTIVE, .release);
    }

    // ── GC coordinator ────────────────────────────────────────────────────────

    /// Returns the oldest epoch any current reader is pinned in.
    /// Objects with commit_ts strictly less than this are safe to free.
    pub fn safeTs(self: *const EpochManager) u64 {
        var min: u64 = EPOCH_INACTIVE;
        for (self.slots) |*s| {
            const e = s.epoch.load(.acquire);
            if (e < min) min = e;
        }
        return if (min == EPOCH_INACTIVE) self.global_ts.load(.acquire) else min;
    }
};

test "epoch manager maps timestamps to epochs" {
    const alloc = std.testing.allocator;
    var mgr = try EpochManager.init(alloc);
    defer mgr.deinit();

    try std.testing.expectEqual(@as(?u64, null), mgr.epochForTimestamp(999));
    try std.testing.expectEqual(@as(u64, 2), mgr.advanceAt(1_000));
    try std.testing.expectEqual(@as(u64, 3), mgr.advanceAt(2_000));
    try std.testing.expectEqual(@as(?u64, @as(u64, 2)), mgr.epochForTimestamp(1_500));
    try std.testing.expectEqual(@as(?u64, @as(u64, 3)), mgr.epochForTimestamp(2_000));
}

test "epoch manager reserves batched epoch ranges" {
    const alloc = std.testing.allocator;
    var mgr = try EpochManager.init(alloc);
    defer mgr.deinit();

    try std.testing.expectEqual(@as(u64, 2), mgr.advanceManyAt(4, 1_000));
    try std.testing.expectEqual(@as(u64, 5), mgr.now());
    try std.testing.expectEqual(@as(?u64, @as(u64, 5)), mgr.epochForTimestamp(1_000));
    try std.testing.expectEqual(@as(u64, 6), mgr.advanceAt(2_000));
    try std.testing.expectEqual(@as(?u64, @as(u64, 6)), mgr.epochForTimestamp(2_000));
}
