/// TurboDB — Structured Error Codes
///
/// Every error response includes a numeric code + human-readable message.
/// Codes are stable across versions for client SDK compatibility.
///
/// Wire protocol: STATUS byte + 2-byte error code + message.
/// HTTP: {"error": {"code": N, "message": "..."}}
const std = @import("std");

pub const ErrorCode = enum(u16) {
    // ── Success ──
    ok = 0,

    // ── Client errors (1xxx) ──
    bad_request = 1000,
    unauthorized = 1001,
    forbidden = 1002,
    not_found = 1003,
    method_not_allowed = 1004,
    conflict = 1005, // e.g., duplicate key
    payload_too_large = 1006,
    schema_violation = 1007,
    dimension_mismatch = 1008,
    invalid_cursor = 1009,
    rate_limited = 1010,

    // ── Server errors (2xxx) ──
    internal_error = 2000,
    storage_error = 2001,
    wal_error = 2002,
    index_error = 2003,
    alloc_error = 2004,
    timeout = 2005,

    // ── Collection errors (3xxx) ──
    collection_not_found = 3000,
    collection_exists = 3001,
    collection_dropped = 3002,

    pub fn message(self: ErrorCode) []const u8 {
        return switch (self) {
            .ok => "ok",
            .bad_request => "bad request",
            .unauthorized => "unauthorized: missing or invalid API key",
            .forbidden => "forbidden: insufficient permissions",
            .not_found => "document not found",
            .method_not_allowed => "method not allowed",
            .conflict => "conflict: duplicate key",
            .payload_too_large => "payload too large",
            .schema_violation => "schema validation failed",
            .dimension_mismatch => "vector dimension mismatch",
            .invalid_cursor => "invalid cursor token",
            .rate_limited => "rate limited",
            .internal_error => "internal server error",
            .storage_error => "storage layer error",
            .wal_error => "write-ahead log error",
            .index_error => "index error",
            .alloc_error => "memory allocation failed",
            .timeout => "operation timed out",
            .collection_not_found => "collection not found",
            .collection_exists => "collection already exists",
            .collection_dropped => "collection has been dropped",
        };
    }

    pub fn httpStatus(self: ErrorCode) []const u8 {
        return switch (self) {
            .ok => "200 OK",
            .bad_request, .payload_too_large, .schema_violation,
            .dimension_mismatch, .invalid_cursor => "400 Bad Request",
            .unauthorized => "401 Unauthorized",
            .forbidden => "403 Forbidden",
            .not_found, .collection_not_found => "404 Not Found",
            .method_not_allowed => "405 Method Not Allowed",
            .conflict, .collection_exists => "409 Conflict",
            .rate_limited => "429 Too Many Requests",
            .internal_error, .storage_error, .wal_error,
            .index_error, .alloc_error => "500 Internal Server Error",
            .timeout => "504 Gateway Timeout",
            .collection_dropped => "410 Gone",
        };
    }

    /// Wire protocol status byte.
    pub fn wireStatus(self: ErrorCode) u8 {
        return switch (self) {
            .ok => 0x00,
            .not_found, .collection_not_found => 0x01,
            .unauthorized, .forbidden => 0x03,
            else => 0x02, // generic error
        };
    }
};

/// Format a JSON error response into a buffer.
pub fn jsonError(buf: []u8, code: ErrorCode) []const u8 {
    var fbs = std.io.fixedBufferStream(buf);
    const w = fbs.writer();
    w.print("{{\"error\":{{\"code\":{d},\"message\":\"{s}\"}}}}", .{
        @intFromEnum(code), code.message(),
    }) catch return "{}";
    return buf[0..fbs.pos];
}

/// Format a JSON error with custom detail message.
pub fn jsonErrorDetail(buf: []u8, code: ErrorCode, detail: []const u8) []const u8 {
    var fbs = std.io.fixedBufferStream(buf);
    const w = fbs.writer();
    w.print("{{\"error\":{{\"code\":{d},\"message\":\"{s}\",\"detail\":\"", .{
        @intFromEnum(code), code.message(),
    }) catch return "{}";
    // Escape detail to prevent JSON injection
    for (detail) |ch| {
        if (ch == '"' or ch == '\\') w.writeByte('\\') catch {};
        if (ch == '\n') { w.writeAll("\\n") catch {}; continue; }
        w.writeByte(ch) catch {};
    }
    w.writeAll("\"}}}}") catch return "{}";
    return buf[0..fbs.pos];
}

// ── Tests ────────────────────────────────────────────────────────────────────

test "error code messages" {
    try std.testing.expectEqualStrings("document not found", ErrorCode.not_found.message());
    try std.testing.expectEqualStrings("unauthorized: missing or invalid API key", ErrorCode.unauthorized.message());
}

test "http status mapping" {
    try std.testing.expectEqualStrings("404 Not Found", ErrorCode.not_found.httpStatus());
    try std.testing.expectEqualStrings("401 Unauthorized", ErrorCode.unauthorized.httpStatus());
    try std.testing.expectEqualStrings("500 Internal Server Error", ErrorCode.internal_error.httpStatus());
}

test "wire status mapping" {
    try std.testing.expectEqual(@as(u8, 0x00), ErrorCode.ok.wireStatus());
    try std.testing.expectEqual(@as(u8, 0x01), ErrorCode.not_found.wireStatus());
    try std.testing.expectEqual(@as(u8, 0x03), ErrorCode.unauthorized.wireStatus());
    try std.testing.expectEqual(@as(u8, 0x02), ErrorCode.internal_error.wireStatus());
}

test "json error formatting" {
    var buf: [256]u8 = undefined;
    const json = jsonError(&buf, .not_found);
    try std.testing.expect(std.mem.indexOf(u8, json, "1003") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "document not found") != null);
}

test "json error with detail" {
    var buf: [256]u8 = undefined;
    const json = jsonErrorDetail(&buf, .schema_violation, "field 'name' is required");
    try std.testing.expect(std.mem.indexOf(u8, json, "1007") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "field 'name' is required") != null);
}
