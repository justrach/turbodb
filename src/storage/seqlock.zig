//! Seqlock — optimistic, wait-free reader concurrency primitive.
//!
//! A seqlock is ideal for small, frequently-read, infrequently-written data
//! (e.g. graph node/edge records) where reader blocking would hurt throughput.
//!
//! Protocol:
//!   Writer: increment seq (now ODD), write data, increment seq (now EVEN).
//!   Reader: read seq (must be EVEN), copy data, read seq again — if unchanged,
//!           the copy is consistent; otherwise retry.
//!
//! The struct is padded to one cache line (64 bytes) so the sequence counter
//! lives on its own line, separate from the data being protected.  This prevents
//! the false-sharing that would otherwise serialise readers through the writer.
//!
//! Important: the *data* protected by the seqlock must also be on separate cache
//! lines for maximum throughput (use extern structs with comptime-checked size).
const std = @import("std");

pub const Seqlock = struct {
    seq:  std.atomic.Value(u64) = std.atomic.Value(u64).init(0),
    _pad: [56]u8 = [_]u8{0} ** 56, // pad to 64 bytes

    comptime { std.debug.assert(@sizeOf(@This()) == 64); }

    // ── Reader side (wait-free) ───────────────────────────────────────────────

    /// Begin a read.  Returns the current sequence number.
    /// Spins until no write is in progress (seq is even).
    pub inline fn readBegin(self: *const Seqlock) u64 {
        var spins: u32 = 0;
        while (true) {
            const s = self.seq.load(.acquire);
            if (s & 1 == 0) return s;
            spins += 1;
            if (spins > 16) {
                std.Thread.yield() catch {};
                spins = 0;
            } else {
                std.atomic.spinLoopHint();
            }
        }
    }

    /// End a read.  Returns true if the read was clean (no concurrent write).
    /// If false, the caller must retry.
    pub inline fn readValid(self: *const Seqlock, begin_seq: u64) bool {
        return self.seq.load(.acquire) == begin_seq;
    }

    // ── Writer side (exclusive, no nesting) ──────────────────────────────────

    /// Acquire the write lock (spins until no other writer is active).
    /// After this call seq is ODD — readers will spin.
    pub fn writeLock(self: *Seqlock) void {
        var spins: u32 = 0;
        while (true) {
            const s = self.seq.load(.acquire);
            if (s & 1 == 0) {
                // Try CAS even → odd
                if (self.seq.cmpxchgStrong(s, s + 1, .acq_rel, .monotonic) == null)
                    return;
            }
            spins += 1;
            if (spins > 16) {
                std.Thread.yield() catch {};
                spins = 0;
            } else {
                std.atomic.spinLoopHint();
            }
        }
    }

    /// Release the write lock.  seq becomes EVEN again.
    pub fn writeUnlock(self: *Seqlock) void {
        _ = self.seq.fetchAdd(1, .release); // odd + 1 = even
    }
};
