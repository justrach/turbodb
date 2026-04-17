/// TurboDB — TTL (Time-To-Live) Document Expiry
///
/// Documents can be given an expiry timestamp at insert time.
/// A background reaper thread periodically scans for expired documents
/// and deletes them.
///
/// TTL is stored as a u64 Unix timestamp (seconds since epoch) in a
/// separate hash map keyed by doc_id. This avoids modifying the 32-byte
/// DocHeader format.
const std = @import("std");
const compat = @import("compat");
const Allocator = std.mem.Allocator;

/// A TTL entry: document ID → expiry timestamp.
const TTLEntry = struct {
    doc_id: u64,
    expires_at: u64, // Unix timestamp (seconds)
};

/// TTL index for a single collection.
pub const TTLIndex = struct {
    entries: std.ArrayListUnmanaged(TTLEntry) = .empty,
    lock: std.Thread.RwLock = .{},

    pub fn deinit(self: *TTLIndex, alloc: Allocator) void {
        self.entries.deinit(alloc);
    }

    /// Set TTL for a document. `ttl_seconds` is relative to now.
    pub fn setTTL(self: *TTLIndex, alloc: Allocator, doc_id: u64, ttl_seconds: u64) !void {
        const now: u64 = @intCast(@divFloor(compat.milliTimestamp(), 1000));
        const expires_at = now + ttl_seconds;

        self.lock.lock();
        defer self.lock.unlock();

        // Check if entry already exists for this doc_id
        for (self.entries.items) |*entry| {
            if (entry.doc_id == doc_id) {
                entry.expires_at = expires_at;
                return;
            }
        }
        try self.entries.append(alloc, .{ .doc_id = doc_id, .expires_at = expires_at });
    }

    /// Set TTL with absolute timestamp.
    pub fn setExpiry(self: *TTLIndex, alloc: Allocator, doc_id: u64, expires_at: u64) !void {
        self.lock.lock();
        defer self.lock.unlock();

        for (self.entries.items) |*entry| {
            if (entry.doc_id == doc_id) {
                entry.expires_at = expires_at;
                return;
            }
        }
        try self.entries.append(alloc, .{ .doc_id = doc_id, .expires_at = expires_at });
    }

    /// Remove TTL for a document (make it permanent).
    pub fn removeTTL(self: *TTLIndex, doc_id: u64) void {
        self.lock.lock();
        defer self.lock.unlock();

        var i: usize = 0;
        while (i < self.entries.items.len) {
            if (self.entries.items[i].doc_id == doc_id) {
                _ = self.entries.swapRemove(i);
                return;
            }
            i += 1;
        }
    }

    /// Collect all expired doc_ids. Caller owns the returned slice.
    pub fn collectExpired(self: *TTLIndex, alloc: Allocator) ![]u64 {
        const now: u64 = @intCast(@divFloor(compat.milliTimestamp(), 1000));
        var expired: std.ArrayListUnmanaged(u64) = .empty;

        self.lock.lockShared();
        defer self.lock.unlockShared();

        for (self.entries.items) |entry| {
            if (entry.expires_at <= now) {
                try expired.append(alloc, entry.doc_id);
            }
        }
        return expired.toOwnedSlice(alloc);
    }

    /// Purge expired entries from the index (call after deleting the docs).
    pub fn purgeExpired(self: *TTLIndex) void {
        const now: u64 = @intCast(@divFloor(compat.milliTimestamp(), 1000));

        self.lock.lock();
        defer self.lock.unlock();

        var i: usize = 0;
        while (i < self.entries.items.len) {
            if (self.entries.items[i].expires_at <= now) {
                _ = self.entries.swapRemove(i);
            } else {
                i += 1;
            }
        }
    }

    /// Number of active TTL entries.
    pub fn count(self: *TTLIndex) usize {
        self.lock.lockShared();
        defer self.lock.unlockShared();
        return self.entries.items.len;
    }
};

// ── Reaper config ────────────────────────────────────────────────────────────

pub const DEFAULT_REAP_INTERVAL_MS: u64 = 60_000; // 1 minute

// ── Tests ────────────────────────────────────────────────────────────────────

test "set and collect TTL" {
    const alloc = std.testing.allocator;
    var idx = TTLIndex{};
    defer idx.deinit(alloc);

    // Set TTL of 0 seconds (immediately expired)
    try idx.setTTL(alloc, 1, 0);
    try idx.setTTL(alloc, 2, 3600); // 1 hour from now

    const expired = try idx.collectExpired(alloc);
    defer alloc.free(expired);

    try std.testing.expectEqual(@as(usize, 1), expired.len);
    try std.testing.expectEqual(@as(u64, 1), expired[0]);
}

test "remove TTL" {
    const alloc = std.testing.allocator;
    var idx = TTLIndex{};
    defer idx.deinit(alloc);

    try idx.setTTL(alloc, 1, 0);
    idx.removeTTL(1);

    const expired = try idx.collectExpired(alloc);
    defer alloc.free(expired);

    try std.testing.expectEqual(@as(usize, 0), expired.len);
}

test "purge expired" {
    const alloc = std.testing.allocator;
    var idx = TTLIndex{};
    defer idx.deinit(alloc);

    try idx.setTTL(alloc, 1, 0);
    try idx.setTTL(alloc, 2, 3600);
    try idx.setTTL(alloc, 3, 0);

    idx.purgeExpired();
    try std.testing.expectEqual(@as(usize, 1), idx.count()); // only doc 2 remains
}

test "update TTL for existing doc" {
    const alloc = std.testing.allocator;
    var idx = TTLIndex{};
    defer idx.deinit(alloc);

    try idx.setTTL(alloc, 1, 0); // expired
    try idx.setTTL(alloc, 1, 3600); // now 1 hour

    const expired = try idx.collectExpired(alloc);
    defer alloc.free(expired);

    try std.testing.expectEqual(@as(usize, 0), expired.len); // no longer expired
}
