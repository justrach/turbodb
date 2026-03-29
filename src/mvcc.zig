const std = @import("std");
const Allocator = std.mem.Allocator;
const testing = std.testing;

/// Location of a version in the page store.
pub const VersionPtr = struct {
    page_no: u32,
    page_off: u16,
    epoch: u64,
    /// Previous version in the chain (0 means no previous).
    prev_page_no: u32,
    prev_page_off: u16,
};

const NO_PREV_PAGE_NO = std.math.maxInt(u32);
const NO_PREV_PAGE_OFF = std.math.maxInt(u16);

/// A read transaction that pins the epoch at which it started.
pub const ReadTxn = struct {
    epoch: u64,
    chain: *VersionChain,

    /// Return the version of `doc_id` visible at this transaction's epoch.
    /// Walks the chain backwards until it finds a version with epoch <= pinned epoch.
    pub fn get(self: ReadTxn, doc_id: u64) ?VersionPtr {
        const ptr = self.chain.latest.get(doc_id) orelse return null;
        return self.walkChain(ptr);
    }

    fn walkChain(self: ReadTxn, start: VersionPtr) ?VersionPtr {
        var cur = start;
        while (true) {
            if (cur.epoch <= self.epoch) return cur;
            if (cur.prev_page_no == NO_PREV_PAGE_NO and cur.prev_page_off == NO_PREV_PAGE_OFF) return null;
            // Follow the chain — in a real system this would read from the page store.
            // For the in-memory model we look up via the history list.
            const hist = self.chain.history.get(chainKey(cur.prev_page_no, cur.prev_page_off)) orelse return null;
            cur = hist;
        }
    }

    /// End the read transaction, unpinning the epoch.
    pub fn end(self: ReadTxn) void {
        // Advance min_active_epoch if we were the oldest reader.
        const current = self.chain.current_epoch.load(.acquire);
        _ = self.chain.min_active_epoch.cmpxchgStrong(self.epoch, current, .release, .monotonic);
    }
};

/// Combine page_no and page_off into a single u64 key for the history map.
fn chainKey(page_no: u32, page_off: u16) u64 {
    return (@as(u64, page_no) << 16) | @as(u64, page_off);
}

/// Append-only MVCC version chain manager.
///
/// Every mutation appends a new VersionPtr; old pages are never modified in place.
/// The indirection table (`latest`) is the single source of truth for the current version.
/// Epoch-based GC replaces Postgres-style vacuum.
/// Append-only MVCC version chain manager.
///
/// Every mutation appends a new VersionPtr; old pages are never modified in place.
/// The indirection table (`latest`) is the single source of truth for the current version.
/// Epoch-based GC replaces Postgres-style vacuum.
pub const VersionChain = struct {
    pub const VersionEntry = struct { doc_id: u64, ver: VersionPtr };

    latest: std.AutoHashMap(u64, VersionPtr),
    /// Secondary index: (page_no, page_off) → VersionPtr, used for chain traversal.
    history: std.AutoHashMap(u64, VersionPtr),
    current_epoch: std.atomic.Value(u64),
    min_active_epoch: std.atomic.Value(u64),
    /// Track all doc versions for GC (doc_id, epoch) pairs.
    all_versions: std.ArrayList(VersionEntry),

    pub fn init(alloc: Allocator) VersionChain {
        return .{
            .latest = std.AutoHashMap(u64, VersionPtr).init(alloc),
            .history = std.AutoHashMap(u64, VersionPtr).init(alloc),
            .current_epoch = std.atomic.Value(u64).init(1),
            .min_active_epoch = std.atomic.Value(u64).init(1),
            .all_versions = .empty,
        };
    }

    pub fn deinit(self: *VersionChain, alloc: Allocator) void {
        self.latest.deinit();
        self.history.deinit();
        self.all_versions.deinit(alloc);
    }

    /// Register a new version for `doc_id` at the given page location and epoch.
    /// Links the new version to the previous latest (if any) via prev pointers.
    pub fn appendVersion(self: *VersionChain, alloc: Allocator, doc_id: u64, page_no: u32, page_off: u16, epoch: u64) !void {
        var prev_page_no: u32 = NO_PREV_PAGE_NO;
        var prev_page_off: u16 = NO_PREV_PAGE_OFF;

        if (self.latest.get(doc_id)) |old| {
            prev_page_no = old.page_no;
            prev_page_off = old.page_off;
        }

        const ver = VersionPtr{
            .page_no = page_no,
            .page_off = page_off,
            .epoch = epoch,
            .prev_page_no = prev_page_no,
            .prev_page_off = prev_page_off,
        };

        try self.latest.put(doc_id, ver);
        try self.history.put(chainKey(page_no, page_off), ver);
        try self.all_versions.append(alloc, .{ .doc_id = doc_id, .ver = ver });
    }

    /// Get the latest version location for a document.
    pub fn getLatest(self: *VersionChain, doc_id: u64) ?VersionPtr {
        return self.latest.get(doc_id);
    }

    pub fn getAtEpoch(self: *VersionChain, doc_id: u64, epoch: u64) ?VersionPtr {
        const ptr = self.latest.get(doc_id) orelse return null;
        var cur = ptr;
        while (true) {
            if (cur.epoch <= epoch) return cur;
            if (cur.prev_page_no == NO_PREV_PAGE_NO and cur.prev_page_off == NO_PREV_PAGE_OFF) return null;
            cur = self.history.get(chainKey(cur.prev_page_no, cur.prev_page_off)) orelse return null;
        }
    }

    /// Begin a read transaction pinned at the current epoch.
    pub fn beginRead(self: *VersionChain) ReadTxn {
        const epoch = self.current_epoch.load(.acquire);
        // Pin: ensure min_active_epoch doesn't advance past us.
        const min = self.min_active_epoch.load(.acquire);
        if (epoch < min) {
            self.min_active_epoch.store(epoch, .release);
        }
        return ReadTxn{
            .epoch = epoch,
            .chain = self,
        };
    }

    /// Advance the global epoch counter. Returns the new epoch.
    pub fn advanceEpoch(self: *VersionChain) u64 {
        const old = self.current_epoch.load(.acquire);
        const new_epoch = old + 1;
        self.current_epoch.store(new_epoch, .release);
        return new_epoch;
    }

    /// Garbage-collect versions older than min_active_epoch.
    /// Returns the number of versions freed.
    pub fn gc(self: *VersionChain, alloc: Allocator) u64 {
        const min_epoch = self.min_active_epoch.load(.acquire);
        var freed: u64 = 0;
        var keep: std.ArrayList(VersionEntry) = .empty;

        var i: usize = 0;
        while (i < self.all_versions.items.len) : (i += 1) {
            const entry = self.all_versions.items[i];
            const is_latest = if (self.latest.get(entry.doc_id)) |cur|
                cur.page_no == entry.ver.page_no and cur.page_off == entry.ver.page_off
            else
                false;

            if (!is_latest and entry.ver.epoch < min_epoch) {
                _ = self.history.remove(chainKey(entry.ver.page_no, entry.ver.page_off));
                freed += 1;
            } else {
                keep.append(alloc, .{ .doc_id = entry.doc_id, .ver = entry.ver }) catch {};
            }
        }

        self.all_versions.deinit(alloc);
        self.all_versions = keep;
        return freed;
    }
};

// ============================================================================
// Tests
// ============================================================================

test "basic insert and get" {
    const alloc = testing.allocator;
    var chain = VersionChain.init(alloc);
    defer chain.deinit(alloc);

    try chain.appendVersion(alloc, 1, 10, 0, 1);
    try chain.appendVersion(alloc, 2, 20, 5, 1);

    const v1 = chain.getLatest(1).?;
    try testing.expectEqual(@as(u32, 10), v1.page_no);
    try testing.expectEqual(@as(u16, 0), v1.page_off);
    try testing.expectEqual(@as(u64, 1), v1.epoch);

    const v2 = chain.getLatest(2).?;
    try testing.expectEqual(@as(u32, 20), v2.page_no);
    try testing.expectEqual(@as(u16, 5), v2.page_off);

    try testing.expect(chain.getLatest(999) == null);
}

test "version chain traversal" {
    const alloc = testing.allocator;
    var chain = VersionChain.init(alloc);
    defer chain.deinit(alloc);

    // Insert doc 1 at epoch 1, then update at epoch 3.
    try chain.appendVersion(alloc, 1, 10, 0, 1);
    try chain.appendVersion(alloc, 1, 11, 1, 3);

    // Latest should be epoch 3.
    const latest = chain.getLatest(1).?;
    try testing.expectEqual(@as(u32, 11), latest.page_no);
    try testing.expectEqual(@as(u64, 3), latest.epoch);
    // Chain link should point back.
    try testing.expectEqual(@as(u32, 10), latest.prev_page_no);
    try testing.expectEqual(@as(u16, 0), latest.prev_page_off);
}

test "read transaction sees correct epoch" {
    const alloc = testing.allocator;
    var chain = VersionChain.init(alloc);
    defer chain.deinit(alloc);

    // Epoch starts at 1.
    try chain.appendVersion(alloc, 1, 10, 0, 1);

    // Advance to epoch 2 and insert a new version.
    _ = chain.advanceEpoch();
    try chain.appendVersion(alloc, 1, 11, 1, 2);

    // A read txn pinned at epoch 1 should see the old version.
    chain.current_epoch.store(1, .release);
    var rtx = chain.beginRead();
    const v = rtx.get(1).?;
    try testing.expectEqual(@as(u32, 10), v.page_no);
    try testing.expectEqual(@as(u64, 1), v.epoch);
    rtx.end();

    // A read txn pinned at epoch 2 should see the new version.
    chain.current_epoch.store(2, .release);
    var rtx2 = chain.beginRead();
    const v2 = rtx2.get(1).?;
    try testing.expectEqual(@as(u32, 11), v2.page_no);
    try testing.expectEqual(@as(u64, 2), v2.epoch);
    rtx2.end();
}

test "epoch-based GC" {
    const alloc = testing.allocator;
    var chain = VersionChain.init(alloc);
    defer chain.deinit(alloc);

    // Insert two versions of doc 1.
    try chain.appendVersion(alloc, 1, 10, 0, 1);
    _ = chain.advanceEpoch();
    try chain.appendVersion(alloc, 1, 11, 1, 2);

    // Before GC: two entries in all_versions.
    try testing.expectEqual(@as(usize, 2), chain.all_versions.items.len);

    // Set min_active_epoch to 2 — version at epoch 1 is collectible.
    chain.min_active_epoch.store(2, .release);
    const freed = chain.gc(alloc);
    try testing.expectEqual(@as(u64, 1), freed);
    // Only the latest version remains.
    try testing.expectEqual(@as(usize, 1), chain.all_versions.items.len);

    // Latest is still accessible.
    const v = chain.getLatest(1).?;
    try testing.expectEqual(@as(u32, 11), v.page_no);
}

test "concurrent reads with different epochs" {
    const alloc = testing.allocator;
    var chain = VersionChain.init(alloc);
    defer chain.deinit(alloc);

    // Build a three-version chain: epochs 1, 2, 3.
    try chain.appendVersion(alloc, 42, 100, 0, 1);
    _ = chain.advanceEpoch();
    try chain.appendVersion(alloc, 42, 101, 0, 2);
    _ = chain.advanceEpoch();
    try chain.appendVersion(alloc, 42, 102, 0, 3);

    // Simulate two concurrent readers at different epochs.
    chain.current_epoch.store(2, .release);
    var r1 = chain.beginRead();

    chain.current_epoch.store(3, .release);
    var r2 = chain.beginRead();

    const v1 = r1.get(42).?;
    try testing.expectEqual(@as(u32, 101), v1.page_no);
    try testing.expectEqual(@as(u64, 2), v1.epoch);

    const v2 = r2.get(42).?;
    try testing.expectEqual(@as(u32, 102), v2.page_no);
    try testing.expectEqual(@as(u64, 3), v2.epoch);

    r1.end();
    r2.end();
}
