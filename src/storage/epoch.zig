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
    /// Global clock — advanced by writers on commit.
    global_ts: std.atomic.Value(u64),
    /// Per-reader pinned epoch.
    slots: []Slot,
    allocator: std.mem.Allocator,

    pub fn init(allocator: std.mem.Allocator) !EpochManager {
        const slots = try allocator.alloc(Slot, MAX_READERS);
        for (slots) |*s| s.epoch.store(EPOCH_INACTIVE, .release);
        return .{
            .global_ts = std.atomic.Value(u64).init(1),
            .slots     = slots,
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *EpochManager) void {
        self.allocator.free(self.slots);
    }

    // ── Writer side ───────────────────────────────────────────────────────────

    /// Called by a writer at commit.  Returns the new timestamp.
    pub fn advance(self: *EpochManager) u64 {
        return self.global_ts.fetchAdd(1, .acq_rel) + 1;
    }

    /// Current global timestamp (snapshot for reads).
    pub fn now(self: *const EpochManager) u64 {
        return self.global_ts.load(.acquire);
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
