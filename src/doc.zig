/// TurboDB — compact binary document format
///
/// On-disk layout for each document:
///
///  [DocHeader 32 bytes][key_len bytes key][value_len bytes value]
///
/// The "value" is raw bytes — typically JSON, but TurboDB doesn't care.
/// Documents are chained per-collection in a linked list inside a B-tree leaf.
///
/// Key hashing: FNV-1a 64-bit — 8 bytes vs BSON's 12-byte ObjectId.
/// No BSON overhead: every field name is hashed at write time.
const std = @import("std");

// ─── DocHeader (32 bytes, cache-friendly) ────────────────────────────────

pub const DocHeader = extern struct {
    doc_id: u64 align(8),  // 0  globally unique document ID (caller-assigned)
    key_hash: u64,          // 8  FNV-1a of the document key
    val_len: u32,           // 16 length of value bytes
    key_len: u16,           // 20 length of key bytes (≤ 1KB enforced)
    flags: u8,              // 22 bit 0 = deleted (tombstone)
    version: u8,            // 23 inline version counter (wraps at 255, use MVCC chain for more)
    next_ver: u64,          // 24 page+offset of previous version (MVCC chain; 0 = head)

    pub const DELETED: u8 = 0x01;
    pub const size = @sizeOf(DocHeader);
    comptime { std.debug.assert(size == 32); }
};

// ─── FNV-1a ──────────────────────────────────────────────────────────────

pub fn fnv1a(s: []const u8) u64 {
    var h: u64 = 14695981039346656037;
    for (s) |b| {
        h ^= @as(u64, b);
        h = h *% 1099511628211;
    }
    return h;
}

// ─── Document (in-memory, zero-copy view) ────────────────────────────────

/// A decoded document — slices point directly into a caller-managed buffer.
/// Do NOT free header/key/value — they are views into an mmap or read buffer.
pub const Doc = struct {
    header: DocHeader,
    key:   []const u8,   // raw bytes of the key
    value: []const u8,   // raw bytes of the value (JSON etc.)

    pub fn isDeleted(self: Doc) bool {
        return self.header.flags & DocHeader.DELETED != 0;
    }

    /// Encode this document into a writer.  Returns total bytes written.
    pub fn encode(self: Doc, writer: anytype) !usize {
        const total = DocHeader.size + self.key.len + self.value.len;
        try writer.writeAll(std.mem.asBytes(&self.header));
        try writer.writeAll(self.key);
        try writer.writeAll(self.value);
        return total;
    }

    /// Encode into a fixed buffer.  Returns slice written, or error if buffer too small.
    pub fn encodeBuf(self: Doc, buf: []u8) ![]u8 {
        const total = DocHeader.size + self.key.len + self.value.len;
        if (buf.len < total) return error.BufferTooSmall;
        @memcpy(buf[0..DocHeader.size], std.mem.asBytes(&self.header));
        @memcpy(buf[DocHeader.size..][0..self.key.len], self.key);
        @memcpy(buf[DocHeader.size + self.key.len ..][0..self.value.len], self.value);
        return buf[0..total];
    }
};

/// Decode a document from `buf` starting at offset 0.
/// Returns the Doc (zero-copy) and total bytes consumed.
pub fn decode(buf: []const u8) error{TooShort,Corrupt}!struct { doc: Doc, consumed: usize } {
    if (buf.len < DocHeader.size) return error.TooShort;
    const hdr: DocHeader = std.mem.bytesToValue(DocHeader, buf[0..DocHeader.size]);
    const total = DocHeader.size + hdr.key_len + hdr.val_len;
    if (buf.len < total) return error.TooShort;
    if (hdr.key_len > 1024) return error.Corrupt;
    return .{
        .doc = .{
            .header = hdr,
            .key    = buf[DocHeader.size..][0..hdr.key_len],
            .value  = buf[DocHeader.size + hdr.key_len ..][0..hdr.val_len],
        },
        .consumed = total,
    };
}

/// Build a DocHeader for a new document.
pub fn newHeader(
    doc_id: u64,
    key: []const u8,
    value: []const u8,
) DocHeader {
    return .{
        .doc_id   = doc_id,
        .key_hash = fnv1a(key),
        .val_len  = @intCast(value.len),
        .key_len  = @intCast(key.len),
        .flags    = 0,
        .version  = 0,
        .next_ver = 0,
    };
}

// ─── compact field accessor (zero-alloc property read) ────────────────────

/// Fast JSON field extractor — does NOT allocate; returns a slice into `json`.
/// Only handles scalar values (strings, numbers, booleans, null).
/// Returns null if the key is missing or the value is an object/array.
pub fn jsonGetField(json: []const u8, key: []const u8) ?[]const u8 {
    var search_buf: [128]u8 = undefined;
    const needle = std.fmt.bufPrint(&search_buf, "\"{s}\":", .{key}) catch return null;
    const pos = std.mem.indexOf(u8, json, needle) orelse return null;
    var i = pos + needle.len;
    while (i < json.len and (json[i] == ' ' or json[i] == '\t')) i += 1;
    if (i >= json.len) return null;
    if (json[i] == '{' or json[i] == '[') return null; // nested
    if (json[i] == '"') {
        // string value
        const start = i + 1;
        var j = start;
        while (j < json.len) : (j += 1) {
            if (json[j] == '\\') { j += 1; continue; }
            if (json[j] == '"') return json[start..j];
        }
        return null;
    }
    // number / bool / null
    var end = i;
    while (end < json.len and json[end] != ',' and json[end] != '}' and
           json[end] != '\n' and json[end] != ' ') end += 1;
    return json[i..end];
}

// ─── tests ────────────────────────────────────────────────────────────────

test "encode/decode round-trip" {
    const key = "user:1";
    const val = "{\"name\":\"Alice\",\"age\":30}";
    const hdr = newHeader(1, key, val);
    const doc = Doc{ .header = hdr, .key = key, .value = val };

    var buf: [256]u8 = undefined;
    const encoded = try doc.encodeBuf(&buf);
    const decoded = try decode(encoded);
    try std.testing.expectEqual(doc.header.doc_id, decoded.doc.header.doc_id);
    try std.testing.expectEqualStrings(key, decoded.doc.key);
    try std.testing.expectEqualStrings(val, decoded.doc.value);
}

test "jsonGetField" {
    const json = "{\"name\":\"Alice\",\"age\":30,\"active\":true}";
    try std.testing.expectEqualStrings("Alice", jsonGetField(json, "name") orelse "");
    try std.testing.expectEqualStrings("30", jsonGetField(json, "age") orelse "");
    try std.testing.expectEqualStrings("true", jsonGetField(json, "active") orelse "");
    try std.testing.expectEqual(@as(?[]const u8, null), jsonGetField(json, "missing"));
}
