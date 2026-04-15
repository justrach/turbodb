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

pub const MAX_READERS:    usize = 1024;
pub const EPOCH_INACTIVE: u64   = std.math.maxInt(u64);

/// One slot per reader thread.  Padded to 64 bytes to avoid false sharing.
const Slot = struct {
    epoch: std.atomic.Value(u64) = std.atomic.Value(u64).init(EPOCH_INACTIVE),
    _pad:  [56]u8 = [_]u8{0} ** 56,
    comptime { std.debug.assert(@sizeOf(@This()) == 64); }
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
    timeline_mu: std.Thread.Mutex,

    pub fn init(allocator: std.mem.Allocator) !EpochManager {
        const slots = try allocator.alloc(Slot, MAX_READERS);
        for (slots) |*s| s.epoch.store(EPOCH_INACTIVE, .release);
        return .{
            .global_ts = std.atomic.Value(u64).init(1),
            .slots     = slots,
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
        return self.advanceAt(std.time.milliTimestamp());
    }

    pub fn advanceAt(self: *EpochManager, ts_ms: i64) u64 {
        const epoch = self.global_ts.fetchAdd(1, .acq_rel) + 1;
        self.timeline_mu.lock();
        defer self.timeline_mu.unlock();
        self.timeline.append(self.allocator, .{ .epoch = epoch, .ts_ms = ts_ms }) catch {};
        // Prune old timeline entries to prevent unbounded memory growth.
        // Keep at most 100K entries (~1.6 MiB); drop the oldest half when full.
        const TIMELINE_CAP = 100_000;
        if (self.timeline.items.len > TIMELINE_CAP) {
            const drop = self.timeline.items.len / 2;
            std.mem.copyForwards(EpochPoint, self.timeline.items[0..self.timeline.items.len - drop], self.timeline.items[drop..]);
            self.timeline.items.len -= drop;
        }
        return epoch;
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
    /// Returns error.ReadersExhausted when all slots are occupied.
    pub fn pin(self: *EpochManager) error{ReadersExhausted}!struct { slot: usize, ts: u64 } {
        const ts = self.global_ts.load(.acquire);
        // Find a free slot
        for (self.slots, 0..) |*s, i| {
            const cur = s.epoch.load(.acquire);
            if (cur == EPOCH_INACTIVE) {
                if (s.epoch.cmpxchgStrong(cur, ts, .acq_rel, .monotonic) == null)
                    return .{ .slot = i, .ts = ts };
            }
        }
        // All slots occupied — cannot safely pin
        return error.ReadersExhausted;
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
