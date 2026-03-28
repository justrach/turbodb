/// TurboDB — Cursor-based Pagination
///
/// Opaque cursor tokens for stateless, consistent pagination.
/// Cursors encode the last-seen doc_id as a base64 token — stable even
/// when documents are inserted/deleted between pages.
///
/// Usage:
///   1. First page:  scan(limit=100, cursor=null) → results + next_cursor
///   2. Next page:   scan(limit=100, cursor=next_cursor) → results + next_cursor
///   3. Last page:   next_cursor = null when no more results
const std = @import("std");

/// A cursor token. Encodes the last-seen doc_id for stable pagination.
pub const Cursor = struct {
    last_doc_id: u64,

    /// Encode cursor to a URL-safe string (hex-encoded u64).
    pub fn encode(self: Cursor) [16]u8 {
        const hex = "0123456789abcdef";
        var out: [16]u8 = undefined;
        var v = self.last_doc_id;
        var i: usize = 16;
        while (i > 0) {
            i -= 1;
            out[i] = hex[@intCast(v & 0xf)];
            v >>= 4;
        }
        return out;
    }

    /// Decode cursor from hex string.
    pub fn decode(token: []const u8) ?Cursor {
        if (token.len != 16) return null;
        var val: u64 = 0;
        for (token) |c| {
            val <<= 4;
            if (c >= '0' and c <= '9') {
                val |= @as(u64, c - '0');
            } else if (c >= 'a' and c <= 'f') {
                val |= @as(u64, c - 'a' + 10);
            } else {
                return null;
            }
        }
        return .{ .last_doc_id = val };
    }
};

/// Result of a cursor-paginated scan.
pub const CursorPage = struct {
    /// Number of documents in this page.
    count: u32,
    /// Next cursor token (null = no more pages).
    next_cursor: ?[16]u8,
    /// Whether there are more results.
    has_more: bool,
};

/// Create a cursor page result from a scan.
/// `last_id`: doc_id of the last document in the current page.
/// `total_returned`: number of docs returned.
/// `limit`: requested limit.
pub fn makePage(last_id: u64, total_returned: u32, limit: u32) CursorPage {
    if (total_returned < limit) {
        return .{ .count = total_returned, .next_cursor = null, .has_more = false };
    }
    const cursor = Cursor{ .last_doc_id = last_id };
    return .{ .count = total_returned, .next_cursor = cursor.encode(), .has_more = true };
}

// ── Tests ────────────────────────────────────────────────────────────────────

test "cursor encode/decode round-trip" {
    const c = Cursor{ .last_doc_id = 12345678 };
    const encoded = c.encode();
    const decoded = Cursor.decode(&encoded).?;
    try std.testing.expectEqual(c.last_doc_id, decoded.last_doc_id);
}

test "cursor encode zero" {
    const c = Cursor{ .last_doc_id = 0 };
    const encoded = c.encode();
    try std.testing.expectEqualStrings("0000000000000000", &encoded);
}

test "cursor encode max" {
    const c = Cursor{ .last_doc_id = std.math.maxInt(u64) };
    const encoded = c.encode();
    try std.testing.expectEqualStrings("ffffffffffffffff", &encoded);
    const decoded = Cursor.decode(&encoded).?;
    try std.testing.expectEqual(c.last_doc_id, decoded.last_doc_id);
}

test "cursor decode invalid length" {
    try std.testing.expectEqual(@as(?Cursor, null), Cursor.decode("abc"));
}

test "cursor decode invalid chars" {
    try std.testing.expectEqual(@as(?Cursor, null), Cursor.decode("000000000000gggg"));
}

test "make page with more results" {
    const page = makePage(42, 100, 100);
    try std.testing.expect(page.has_more);
    try std.testing.expect(page.next_cursor != null);
}

test "make page last page" {
    const page = makePage(42, 50, 100);
    try std.testing.expect(!page.has_more);
    try std.testing.expectEqual(@as(?[16]u8, null), page.next_cursor);
}
