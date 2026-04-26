//! SQL-style query post-processing for handleGet / handleScan.
//!
//! Composable query parameters layered on top of turbodb's collection scan:
//!   ?select=f1,f2          project only these fields in the response value
//!   ?where=field=value     equality filter (string or number compared lexically)
//!   ?order=field:asc|desc  sort scan results by the extracted field
//!   ?join=col:field[:as]   for each doc, look up `value.field` as a key in
//!                          collection `col` and inline the joined doc under
//!                          `joined.<as>` (default as=col).
//!
//! All filters operate on the JSON value bytes — no parse tree allocation.
//! Everything is best-effort: malformed JSON → predicate false, missing
//! field → predicate false, missing join target → null inline.
//!
//! These layer cleanly on the existing scan: the engine fetches up to
//! `limit*4` candidate rows so post-filter has room to satisfy `limit` after
//! WHERE rejects some. ORDER BY materializes into the alloc, sorts, then
//! re-applies limit.

const std = @import("std");
const doc_mod = @import("doc.zig");

pub const Spec = struct {
    select: ?[]const u8 = null,
    where_field: ?[]const u8 = null,
    where_value: ?[]const u8 = null,
    order_field: ?[]const u8 = null,
    order_desc: bool = false,
    /// Up to 4 join clauses; each is "col:field[:as]". Realistic limit; the
    /// per-doc cost grows linearly so we'd rather ratelimit than allow
    /// runaway 50-way joins.
    joins: [4]?Join = .{ null, null, null, null },

    pub const Join = struct {
        col: []const u8,
        field: []const u8,
        as: []const u8,
    };

    pub fn parse(query: []const u8) Spec {
        var s: Spec = .{};
        if (qparam(query, "select")) |v| s.select = v;
        if (qparam(query, "where")) |v| {
            if (std.mem.indexOfScalar(u8, v, '=')) |eq| {
                s.where_field = v[0..eq];
                s.where_value = v[eq + 1 ..];
            }
        }
        if (qparam(query, "order")) |v| {
            if (std.mem.indexOfScalar(u8, v, ':')) |sep| {
                s.order_field = v[0..sep];
                s.order_desc = std.mem.eql(u8, v[sep + 1 ..], "desc");
            } else {
                s.order_field = v;
            }
        }
        // Multiple joins: ?join=a:f1&join=b:f2 — naive scan finds first; for
        // multiple we walk the query string.
        var pos: usize = 0;
        var i: usize = 0;
        while (pos < query.len and i < s.joins.len) {
            const found = std.mem.indexOfPos(u8, query, pos, "join=") orelse break;
            const start = found + 5;
            var end = start;
            while (end < query.len and query[end] != '&') end += 1;
            const raw = query[start..end];
            s.joins[i] = parseJoin(raw);
            i += 1;
            pos = end + 1;
        }
        return s;
    }

    fn parseJoin(raw: []const u8) ?Join {
        // "col:field" or "col:field:as"
        const sep1 = std.mem.indexOfScalar(u8, raw, ':') orelse return null;
        const col = raw[0..sep1];
        const rest = raw[sep1 + 1 ..];
        if (std.mem.indexOfScalar(u8, rest, ':')) |sep2| {
            return .{ .col = col, .field = rest[0..sep2], .as = rest[sep2 + 1 ..] };
        }
        return .{ .col = col, .field = rest, .as = col };
    }

    pub fn anyActive(self: Spec) bool {
        if (self.select != null) return true;
        if (self.where_field != null) return true;
        if (self.order_field != null) return true;
        for (self.joins) |j| if (j != null) return true;
        return false;
    }
};

/// Lexically compare two JSON-extracted values. Numbers are compared
/// numerically; otherwise byte-wise. Returns std.math.Order.
pub fn compareValues(a: []const u8, b: []const u8) std.math.Order {
    const af = std.fmt.parseFloat(f64, a) catch null;
    const bf = std.fmt.parseFloat(f64, b) catch null;
    if (af != null and bf != null) {
        return std.math.order(af.?, bf.?);
    }
    return std.mem.order(u8, a, b);
}

/// Evaluate WHERE predicate on a JSON value. Returns true if pass.
pub fn passesWhere(value: []const u8, spec: Spec) bool {
    const f = spec.where_field orelse return true;
    const want = spec.where_value orelse return true;

    // Strip surrounding quotes from `want` so callers can pass quoted strings.
    const w = stripQuotes(want);

    const got = jsonExtract(value, f) orelse return false;
    const g = stripQuotes(got);
    return std.mem.eql(u8, g, w);
}

/// Extract the raw bytes of a JSON field at the top level of an object.
/// Returns the literal token (with quotes for strings, raw chars for numbers
/// and bools). Used for WHERE comparison and JOIN key lookup.
pub fn jsonExtract(json: []const u8, field: []const u8) ?[]const u8 {
    // Build pattern: "field":
    var key_buf: [128]u8 = undefined;
    if (field.len + 3 > key_buf.len) return null;
    key_buf[0] = '"';
    @memcpy(key_buf[1 .. 1 + field.len], field);
    key_buf[1 + field.len] = '"';
    key_buf[2 + field.len] = ':';
    const needle = key_buf[0 .. 3 + field.len];

    const at = std.mem.indexOf(u8, json, needle) orelse return null;
    var i = at + needle.len;
    while (i < json.len and (json[i] == ' ' or json[i] == '\t')) i += 1;
    if (i >= json.len) return null;

    // Token starts at i. Capture until comma/} at top level.
    if (json[i] == '"') {
        // String — find matching close quote (no nested escapes for simplicity)
        const start = i;
        i += 1;
        while (i < json.len and json[i] != '"') {
            if (json[i] == '\\' and i + 1 < json.len) i += 2 else i += 1;
        }
        if (i >= json.len) return null;
        return json[start .. i + 1];
    }
    if (json[i] == '{' or json[i] == '[') {
        const start = i;
        var depth: i32 = 0;
        const open: u8 = json[i];
        const close: u8 = if (open == '{') '}' else ']';
        while (i < json.len) : (i += 1) {
            if (json[i] == open) depth += 1;
            if (json[i] == close) {
                depth -= 1;
                if (depth == 0) return json[start .. i + 1];
            }
        }
        return null;
    }
    // Number / bool / null — terminate at , } ] whitespace.
    const start = i;
    while (i < json.len and json[i] != ',' and json[i] != '}' and json[i] != ']' and
        json[i] != ' ' and json[i] != '\t' and json[i] != '\n' and json[i] != '\r')
        i += 1;
    if (i == start) return null;
    return json[start..i];
}

/// Strip leading and trailing `"` if both present. Idempotent for non-quoted.
pub fn stripQuotes(s: []const u8) []const u8 {
    if (s.len >= 2 and s[0] == '"' and s[s.len - 1] == '"') return s[1 .. s.len - 1];
    return s;
}

/// Write a projected JSON object containing only the fields in `select`.
/// `select` is a comma-separated list. If null/empty, writes the value as-is.
pub fn writeProjected(w: *std.Io.Writer, value: []const u8, select: ?[]const u8) void {
    const sel = select orelse {
        // Pass-through. Caller is responsible for quoting strings appropriately
        // — values from the engine are already valid JSON.
        w.writeAll(value) catch {};
        return;
    };
    w.writeByte('{') catch {};
    var first = true;
    var it = std.mem.splitScalar(u8, sel, ',');
    while (it.next()) |raw_field| {
        const field = std.mem.trim(u8, raw_field, " \t");
        if (field.len == 0) continue;
        const got = jsonExtract(value, field) orelse continue;
        if (!first) w.writeByte(',') catch {};
        first = false;
        w.print("\"{s}\":", .{field}) catch {};
        w.writeAll(got) catch {};
    }
    w.writeByte('}') catch {};
}

// ---------- helpers (kept private; tested in server.zig for now) ----------

fn qparam(query: []const u8, key: []const u8) ?[]const u8 {
    var kbuf: [64]u8 = undefined;
    if (key.len + 1 > kbuf.len) return null;
    @memcpy(kbuf[0..key.len], key);
    kbuf[key.len] = '=';
    const needle = kbuf[0 .. key.len + 1];
    const pos = std.mem.indexOf(u8, query, needle) orelse return null;
    const start = pos + needle.len;
    var end = start;
    while (end < query.len and query[end] != '&') end += 1;
    if (end == start) return null;
    return query[start..end];
}

// ---------- tests ---------------------------------------------------------

test "Spec.parse picks up select/where/order/join" {
    const s = Spec.parse("select=id,amount&where=amount=50&order=amount:desc&join=accounts:debit_account_id:debit");
    try std.testing.expectEqualStrings("id,amount", s.select.?);
    try std.testing.expectEqualStrings("amount", s.where_field.?);
    try std.testing.expectEqualStrings("50", s.where_value.?);
    try std.testing.expectEqualStrings("amount", s.order_field.?);
    try std.testing.expect(s.order_desc);
    try std.testing.expectEqualStrings("accounts", s.joins[0].?.col);
    try std.testing.expectEqualStrings("debit_account_id", s.joins[0].?.field);
    try std.testing.expectEqualStrings("debit", s.joins[0].?.as);
}

test "jsonExtract handles strings, numbers, nested" {
    const v = "{\"id\":42,\"name\":\"alice\",\"sub\":{\"x\":1},\"tags\":[1,2]}";
    try std.testing.expectEqualStrings("42", jsonExtract(v, "id").?);
    try std.testing.expectEqualStrings("\"alice\"", jsonExtract(v, "name").?);
    try std.testing.expectEqualStrings("{\"x\":1}", jsonExtract(v, "sub").?);
    try std.testing.expectEqualStrings("[1,2]", jsonExtract(v, "tags").?);
    try std.testing.expect(jsonExtract(v, "missing") == null);
}

test "passesWhere matches and rejects" {
    const v = "{\"id\":1,\"amount\":100}";
    var s: Spec = .{ .where_field = "amount", .where_value = "100" };
    try std.testing.expect(passesWhere(v, s));
    s.where_value = "99";
    try std.testing.expect(!passesWhere(v, s));
}

test "compareValues numeric vs lex" {
    try std.testing.expect(compareValues("10", "9") == .gt);  // numeric (lex would say lt)
    try std.testing.expect(compareValues("\"b\"", "\"a\"") == .gt);
}

test "writeProjected emits only requested fields" {
    var buf: [256]u8 = undefined;
    var w = std.Io.Writer.fixed(&buf);
    writeProjected(&w, "{\"id\":1,\"amount\":100,\"who\":\"x\"}", "id,amount");
    try std.testing.expectEqualStrings("{\"id\":1,\"amount\":100}", buf[0..w.end]);
}

test "writeProjected null select is pass-through" {
    var buf: [256]u8 = undefined;
    var w = std.Io.Writer.fixed(&buf);
    writeProjected(&w, "{\"id\":1}", null);
    try std.testing.expectEqualStrings("{\"id\":1}", buf[0..w.end]);
}
