/// TurboDB — MongoDB-style query filter engine
///
/// Parses JSON filter expressions like {"age": {"$gt": 25}} and evaluates
/// them against raw JSON document bytes. The filter parser allocates, but
/// field extraction and matching are zero-alloc.
///
/// Supports: $eq $ne $gt $gte $lt $lte $contains $exists $in $and $or $not
/// Nested field access via dot notation: "address.city"
const std = @import("std");

// ─── Operators ─────────────────────────────────────────────────────────────

pub const Op = enum {
    eq,
    ne,
    gt,
    gte,
    lt,
    lte,
    contains,
    exists,
    in_set,
    // Logical
    op_and,
    op_or,
    op_not,
};

// ─── Value ─────────────────────────────────────────────────────────────────

pub const Value = union(enum) {
    string: []const u8,
    integer: i64,
    float: f64,
    boolean: bool,
    null_val: void,
    array: []const Value, // for $in operator

    /// Parse a std.json.Value into our Value type.
    fn fromJson(jv: std.json.Value, alloc: std.mem.Allocator) FilterError!Value {
        return switch (jv) {
            .string => |s| .{ .string = s },
            .integer => |n| .{ .integer = n },
            .float => |f| .{ .float = f },
            .bool => |b| .{ .boolean = b },
            .null => .{ .null_val = {} },
            .array => |arr| {
                const vals = try alloc.alloc(Value, arr.items.len);
                for (arr.items, 0..) |item, i| {
                    vals[i] = try fromJson(item, alloc);
                }
                return .{ .array = vals };
            },
            else => error.UnsupportedValueType,
        };
    }
};

// ─── Filter ────────────────────────────────────────────────────────────────

pub const Filter = struct {
    field: []const u8, // JSON field name (dot notation for nested: "address.city")
    op: Op,
    value: Value, // comparison value
    children: ?[]Filter, // for and/or/not
};

/// Owns all memory from parseFilter(). Call deinit() when done.
pub const ParsedFilter = struct {
    filter: Filter,
    arena: std.heap.ArenaAllocator,

    pub fn deinit(self: *ParsedFilter) void {
        self.arena.deinit();
    }
};

// ─── Filter Parser ─────────────────────────────────────────────────────────

const FilterError = error{
    InvalidFilter,
    UnknownOperator,
    UnsupportedValueType,
    OutOfMemory,
};

/// Parse a MongoDB-style JSON query into a Filter tree.
/// Returns a ParsedFilter that owns all allocated memory.
/// Call .deinit() when done.
///
/// Examples:
///   {"age": {"$gt": 25}}
///   {"$and": [{"status": "active"}, {"age": {"$gte": 18}}]}
///   {"name": {"$contains": "john"}}
pub fn parseFilter(json: []const u8, alloc: std.mem.Allocator) !ParsedFilter {
    var arena = std.heap.ArenaAllocator.init(alloc);
    errdefer arena.deinit();
    const arena_alloc = arena.allocator();
    const parsed = try std.json.parseFromSlice(std.json.Value, arena_alloc, json, .{});
    const filter = try buildFilter(parsed.value, arena_alloc);
    return .{ .filter = filter, .arena = arena };
}

fn buildFilter(val: std.json.Value, alloc: std.mem.Allocator) FilterError!Filter {
    if (val != .object) return error.InvalidFilter;
    const obj = val.object;

    // Check for top-level logical operators
    if (obj.get("$and")) |arr| {
        return buildLogical(.op_and, arr, alloc);
    }
    if (obj.get("$or")) |arr| {
        return buildLogical(.op_or, arr, alloc);
    }
    if (obj.get("$not")) |inner| {
        const child = try alloc.alloc(Filter, 1);
        child[0] = try buildFilter(inner, alloc);
        return .{ .field = "", .op = .op_not, .value = .{ .null_val = {} }, .children = child };
    }

    // Single field condition — take the first (and only expected) key
    var it = obj.iterator();
    const entry = it.next() orelse return error.InvalidFilter;
    const field = entry.key_ptr.*;

    // Shorthand equality: {"name": "Alice"} => eq
    if (entry.value_ptr.* != .object) {
        return .{
            .field = field,
            .op = .eq,
            .value = try Value.fromJson(entry.value_ptr.*, alloc),
            .children = null,
        };
    }

    // Operator form: {"age": {"$gt": 25}}
    const op_obj = entry.value_ptr.object;
    var op_it = op_obj.iterator();
    const op_entry = op_it.next() orelse return error.InvalidFilter;
    const op_str = op_entry.key_ptr.*;
    const op = parseOp(op_str) orelse return error.UnknownOperator;

    return .{
        .field = field,
        .op = op,
        .value = try Value.fromJson(op_entry.value_ptr.*, alloc),
        .children = null,
    };
}

fn buildLogical(op: Op, arr_val: std.json.Value, alloc: std.mem.Allocator) FilterError!Filter {
    if (arr_val != .array) return error.InvalidFilter;
    const items = arr_val.array.items;
    const children = try alloc.alloc(Filter, items.len);
    for (items, 0..) |item, i| {
        children[i] = try buildFilter(item, alloc);
    }
    return .{ .field = "", .op = op, .value = .{ .null_val = {} }, .children = children };
}

fn parseOp(s: []const u8) ?Op {
    const map = .{
        .{ "$eq", Op.eq },
        .{ "$ne", Op.ne },
        .{ "$gt", Op.gt },
        .{ "$gte", Op.gte },
        .{ "$lt", Op.lt },
        .{ "$lte", Op.lte },
        .{ "$contains", Op.contains },
        .{ "$exists", Op.exists },
        .{ "$in", Op.in_set },
    };
    inline for (map) |pair| {
        if (std.mem.eql(u8, s, pair[0])) return pair[1];
    }
    return null;
}

// ─── Zero-alloc JSON field extractor ───────────────────────────────────────

/// Extract a field value from raw JSON bytes without allocating.
/// Supports dot notation for nested fields: "address.city"
/// Returns the raw value bytes (including quotes for strings).
pub fn extractField(json: []const u8, field: []const u8) ?[]const u8 {
    // Split on dots for nested access
    var remaining = field;
    var data = json;

    while (true) {
        const dot = std.mem.indexOfScalar(u8, remaining, '.') orelse {
            // Last (or only) segment
            return extractRawValue(data, remaining);
        };
        const segment = remaining[0..dot];
        remaining = remaining[dot + 1 ..];

        // Find nested object value for this segment
        const nested = extractRawValue(data, segment) orelse return null;
        if (nested.len == 0 or nested[0] != '{') return null; // not an object
        data = nested;
    }
}

/// Core scanner: find a key in a JSON object and return raw value bytes.
/// Handles strings, numbers, booleans, null, objects, and arrays.
fn extractRawValue(json: []const u8, key: []const u8) ?[]const u8 {
    // Build needle: "key"
    var needle_buf: [256]u8 = undefined;
    const needle = std.fmt.bufPrint(&needle_buf, "\"{s}\"", .{key}) catch return null;

    var pos: usize = 0;
    while (pos < json.len) {
        const found = std.mem.indexOfPos(u8, json, pos, needle) orelse return null;
        // Skip past the key and find the colon
        var i = found + needle.len;
        i = skipWhitespace(json, i);
        if (i >= json.len or json[i] != ':') {
            pos = found + 1;
            continue;
        }
        i += 1; // skip ':'
        i = skipWhitespace(json, i);
        if (i >= json.len) return null;

        // Now i points at the value start
        return extractValueSpan(json, i);
    }
    return null;
}

/// Given json[start..] pointing at the beginning of a JSON value,
/// return the slice covering the entire value.
fn extractValueSpan(json: []const u8, start: usize) ?[]const u8 {
    if (start >= json.len) return null;
    const c = json[start];

    if (c == '"') {
        // String: scan to closing quote
        var j = start + 1;
        while (j < json.len) : (j += 1) {
            if (json[j] == '\\') {
                j += 1;
                continue;
            }
            if (json[j] == '"') return json[start .. j + 1];
        }
        return null;
    }

    if (c == '{' or c == '[') {
        // Object/Array: count brackets
        const open = c;
        const close: u8 = if (c == '{') '}' else ']';
        var depth: usize = 1;
        var j = start + 1;
        var in_str = false;
        while (j < json.len) : (j += 1) {
            if (in_str) {
                if (json[j] == '\\') {
                    j += 1;
                    continue;
                }
                if (json[j] == '"') in_str = false;
                continue;
            }
            if (json[j] == '"') {
                in_str = true;
                continue;
            }
            if (json[j] == open) depth += 1;
            if (json[j] == close) {
                depth -= 1;
                if (depth == 0) return json[start .. j + 1];
            }
        }
        return null;
    }

    // Number, bool, null: scan until delimiter
    var end = start;
    while (end < json.len and json[end] != ',' and json[end] != '}' and
        json[end] != ']' and json[end] != ' ' and json[end] != '\n' and
        json[end] != '\r' and json[end] != '\t') : (end += 1)
    {}
    if (end == start) return null;
    return json[start..end];
}

fn skipWhitespace(json: []const u8, start: usize) usize {
    var i = start;
    while (i < json.len and (json[i] == ' ' or json[i] == '\t' or
        json[i] == '\n' or json[i] == '\r')) : (i += 1)
    {}
    return i;
}

// ─── Filter evaluator ──────────────────────────────────────────────────────

/// Check if a JSON document matches a filter. Zero-alloc.
pub fn matches(doc_json: []const u8, filter: *const Filter) bool {
    switch (filter.op) {
        .op_and => {
            const kids = filter.children orelse return true;
            for (kids) |*child| {
                if (!matches(doc_json, child)) return false;
            }
            return true;
        },
        .op_or => {
            const kids = filter.children orelse return false;
            for (kids) |*child| {
                if (matches(doc_json, child)) return true;
            }
            return false;
        },
        .op_not => {
            const kids = filter.children orelse return true;
            return !matches(doc_json, &kids[0]);
        },
        .exists => {
            const found = extractField(doc_json, filter.field) != null;
            return switch (filter.value) {
                .boolean => |b| found == b,
                else => found,
            };
        },
        else => {
            const raw = extractField(doc_json, filter.field) orelse return false;
            return evalOp(filter.op, raw, filter.value);
        },
    }
}

/// Compare extracted raw JSON value against a filter value.
fn evalOp(op: Op, raw: []const u8, val: Value) bool {
    switch (op) {
        .eq => return valueEq(raw, val),
        .ne => return !valueEq(raw, val),
        .gt, .gte, .lt, .lte => return compareTo(op, raw, val),
        .contains => {
            // Substring match on string values
            const str_val = switch (val) {
                .string => |s| s,
                else => return false,
            };
            const raw_str = unquote(raw) orelse return false;
            return std.mem.indexOf(u8, raw_str, str_val) != null;
        },
        .in_set => {
            const arr = switch (val) {
                .array => |a| a,
                else => return false,
            };
            for (arr) |item| {
                if (valueEq(raw, item)) return true;
            }
            return false;
        },
        .exists, .op_and, .op_or, .op_not => unreachable,
    }
}

/// Check if a raw JSON token equals a Value.
fn valueEq(raw: []const u8, val: Value) bool {
    switch (val) {
        .string => |s| {
            const raw_str = unquote(raw) orelse return false;
            return std.mem.eql(u8, raw_str, s);
        },
        .integer => |n| {
            const parsed = parseNumber(raw) orelse return false;
            return switch (parsed) {
                .int => |i| i == n,
                .flt => |f| f == @as(f64, @floatFromInt(n)),
            };
        },
        .float => |f| {
            const parsed = parseNumber(raw) orelse return false;
            return switch (parsed) {
                .int => |i| @as(f64, @floatFromInt(i)) == f,
                .flt => |pf| pf == f,
            };
        },
        .boolean => |b| {
            if (std.mem.eql(u8, raw, "true")) return b;
            if (std.mem.eql(u8, raw, "false")) return !b;
            return false;
        },
        .null_val => return std.mem.eql(u8, raw, "null"),
        .array => return false,
    }
}

const Num = union(enum) { int: i64, flt: f64 };

fn parseNumber(raw: []const u8) ?Num {
    if (std.fmt.parseInt(i64, raw, 10)) |i| {
        return .{ .int = i };
    } else |_| {}
    if (std.fmt.parseFloat(f64, raw)) |f| {
        return .{ .flt = f };
    } else |_| {}
    return null;
}

fn compareTo(op: Op, raw: []const u8, val: Value) bool {
    const raw_num = parseNumber(raw) orelse return false;
    const raw_f: f64 = switch (raw_num) {
        .int => |i| @floatFromInt(i),
        .flt => |f| f,
    };
    const val_f: f64 = switch (val) {
        .integer => |n| @floatFromInt(n),
        .float => |f| f,
        else => return false,
    };
    return switch (op) {
        .gt => raw_f > val_f,
        .gte => raw_f >= val_f,
        .lt => raw_f < val_f,
        .lte => raw_f <= val_f,
        else => unreachable,
    };
}

/// Strip surrounding quotes from a raw JSON string token.
fn unquote(raw: []const u8) ?[]const u8 {
    if (raw.len >= 2 and raw[0] == '"' and raw[raw.len - 1] == '"') {
        return raw[1 .. raw.len - 1];
    }
    return null;
}

// ─── Aggregation ───────────────────────────────────────────────────────────

pub const AggOp = enum { count, sum, avg, min, max };

pub const Aggregation = struct {
    op: AggOp,
    field: []const u8, // field to aggregate (empty for count)
};

pub const AggResult = struct {
    count: u64,
    sum: f64,
    min: f64,
    max: f64,

    pub fn avg(self: AggResult) f64 {
        if (self.count == 0) return 0.0;
        return self.sum / @as(f64, @floatFromInt(self.count));
    }
};

/// Run an aggregation over a slice of raw JSON document bytes.
pub fn aggregate(docs: []const []const u8, agg: Aggregation) AggResult {
    var result = AggResult{
        .count = 0,
        .sum = 0.0,
        .min = std.math.inf(f64),
        .max = -std.math.inf(f64),
    };

    for (docs) |doc_json| {
        if (agg.op == .count and agg.field.len == 0) {
            result.count += 1;
            continue;
        }

        const raw = extractField(doc_json, agg.field) orelse continue;
        const num = parseNumber(raw) orelse continue;
        const f: f64 = switch (num) {
            .int => |i| @floatFromInt(i),
            .flt => |v| v,
        };

        result.count += 1;
        result.sum += f;
        if (f < result.min) result.min = f;
        if (f > result.max) result.max = f;
    }

    // Normalize min/max when no values found
    if (result.count == 0) {
        result.min = 0.0;
        result.max = 0.0;
    }

    return result;
}

// ─── Tests ─────────────────────────────────────────────────────────────────

test "parse simple equality filter" {
    const alloc = std.testing.allocator;
    var pf = try parseFilter("{\"name\": \"Alice\"}", alloc);
    defer pf.deinit();
    try std.testing.expectEqualStrings("name", pf.filter.field);
    try std.testing.expectEqual(Op.eq, pf.filter.op);
    try std.testing.expectEqualStrings("Alice", pf.filter.value.string);
}

test "parse comparison operators" {
    const alloc = std.testing.allocator;

    var gt = try parseFilter("{\"age\": {\"$gt\": 25}}", alloc);
    defer gt.deinit();
    try std.testing.expectEqual(Op.gt, gt.filter.op);
    try std.testing.expectEqual(@as(i64, 25), gt.filter.value.integer);

    var lte = try parseFilter("{\"score\": {\"$lte\": 100}}", alloc);
    defer lte.deinit();
    try std.testing.expectEqual(Op.lte, lte.filter.op);
}

test "parse logical operators ($and, $or, $not)" {
    const alloc = std.testing.allocator;

    var pf = try parseFilter(
        "{\"$and\": [{\"status\": \"active\"}, {\"age\": {\"$gte\": 18}}]}",
        alloc,
    );
    defer pf.deinit();
    try std.testing.expectEqual(Op.op_and, pf.filter.op);
    try std.testing.expectEqual(@as(usize, 2), pf.filter.children.?.len);
    try std.testing.expectEqual(Op.eq, pf.filter.children.?[0].op);
    try std.testing.expectEqual(Op.gte, pf.filter.children.?[1].op);

    var or_pf = try parseFilter(
        "{\"$or\": [{\"x\": 1}, {\"y\": 2}]}",
        alloc,
    );
    defer or_pf.deinit();
    try std.testing.expectEqual(Op.op_or, or_pf.filter.op);

    var not_pf = try parseFilter(
        "{\"$not\": {\"status\": \"banned\"}}",
        alloc,
    );
    defer not_pf.deinit();
    try std.testing.expectEqual(Op.op_not, not_pf.filter.op);
}

test "parse $in and $exists" {
    const alloc = std.testing.allocator;

    var in_pf = try parseFilter("{\"tags\": {\"$in\": [\"a\", \"b\", \"c\"]}}", alloc);
    defer in_pf.deinit();
    try std.testing.expectEqual(Op.in_set, in_pf.filter.op);
    try std.testing.expectEqual(@as(usize, 3), in_pf.filter.value.array.len);

    var ex_pf = try parseFilter("{\"email\": {\"$exists\": true}}", alloc);
    defer ex_pf.deinit();
    try std.testing.expectEqual(Op.exists, ex_pf.filter.op);
    try std.testing.expectEqual(true, ex_pf.filter.value.boolean);
}

test "extractField on flat JSON" {
    const json = "{\"name\": \"Alice\", \"age\": 30, \"active\": true}";
    {
        const val = extractField(json, "name") orelse unreachable;
        try std.testing.expectEqualStrings("\"Alice\"", val);
    }
    {
        const val = extractField(json, "age") orelse unreachable;
        try std.testing.expectEqualStrings("30", val);
    }
    {
        const val = extractField(json, "active") orelse unreachable;
        try std.testing.expectEqualStrings("true", val);
    }
    try std.testing.expectEqual(@as(?[]const u8, null), extractField(json, "missing"));
}

test "extractField on nested JSON (dot notation)" {
    const json =
        \\{"user": {"address": {"city": "Portland", "zip": 97201}}, "id": 1}
    ;
    {
        const val = extractField(json, "user.address.city") orelse unreachable;
        try std.testing.expectEqualStrings("\"Portland\"", val);
    }
    {
        const val = extractField(json, "user.address.zip") orelse unreachable;
        try std.testing.expectEqualStrings("97201", val);
    }
    {
        const val = extractField(json, "id") orelse unreachable;
        try std.testing.expectEqualStrings("1", val);
    }
    try std.testing.expectEqual(@as(?[]const u8, null), extractField(json, "user.phone"));
}

test "matches() with various operators" {
    const doc = "{\"name\": \"Alice\", \"age\": 30, \"status\": \"active\"}";
    const alloc = std.testing.allocator;

    // eq
    {
        var pf = try parseFilter("{\"name\": \"Alice\"}", alloc);
        defer pf.deinit();
        try std.testing.expect(matches(doc, &pf.filter));
    }
    // ne
    {
        var pf = try parseFilter("{\"name\": {\"$ne\": \"Bob\"}}", alloc);
        defer pf.deinit();
        try std.testing.expect(matches(doc, &pf.filter));
    }
    // gt
    {
        var pf = try parseFilter("{\"age\": {\"$gt\": 25}}", alloc);
        defer pf.deinit();
        try std.testing.expect(matches(doc, &pf.filter));
    }
    // lt (should not match)
    {
        var pf = try parseFilter("{\"age\": {\"$lt\": 25}}", alloc);
        defer pf.deinit();
        try std.testing.expect(!matches(doc, &pf.filter));
    }
    // contains
    {
        var pf = try parseFilter("{\"name\": {\"$contains\": \"lic\"}}", alloc);
        defer pf.deinit();
        try std.testing.expect(matches(doc, &pf.filter));
    }
    // $and
    {
        var pf = try parseFilter(
            "{\"$and\": [{\"status\": \"active\"}, {\"age\": {\"$gte\": 18}}]}",
            alloc,
        );
        defer pf.deinit();
        try std.testing.expect(matches(doc, &pf.filter));
    }
    // $or
    {
        var pf = try parseFilter(
            "{\"$or\": [{\"name\": \"Bob\"}, {\"age\": {\"$gt\": 20}}]}",
            alloc,
        );
        defer pf.deinit();
        try std.testing.expect(matches(doc, &pf.filter));
    }
    // $not
    {
        var pf = try parseFilter("{\"$not\": {\"status\": \"banned\"}}", alloc);
        defer pf.deinit();
        try std.testing.expect(matches(doc, &pf.filter));
    }
    // $in
    {
        var pf = try parseFilter("{\"status\": {\"$in\": [\"active\", \"pending\"]}}", alloc);
        defer pf.deinit();
        try std.testing.expect(matches(doc, &pf.filter));
    }
    // $exists
    {
        var pf = try parseFilter("{\"name\": {\"$exists\": true}}", alloc);
        defer pf.deinit();
        try std.testing.expect(matches(doc, &pf.filter));
    }
    {
        var pf = try parseFilter("{\"email\": {\"$exists\": true}}", alloc);
        defer pf.deinit();
        try std.testing.expect(!matches(doc, &pf.filter));
    }
}

test "aggregation: count, sum, avg, min, max" {
    const docs = [_][]const u8{
        "{\"val\": 10, \"name\": \"a\"}",
        "{\"val\": 20, \"name\": \"b\"}",
        "{\"val\": 30, \"name\": \"c\"}",
        "{\"val\": 40, \"name\": \"d\"}",
    };

    // count (field-less)
    {
        const r = aggregate(&docs, .{ .op = .count, .field = "" });
        try std.testing.expectEqual(@as(u64, 4), r.count);
    }
    // sum
    {
        const r = aggregate(&docs, .{ .op = .sum, .field = "val" });
        try std.testing.expectEqual(@as(f64, 100.0), r.sum);
    }
    // avg
    {
        const r = aggregate(&docs, .{ .op = .avg, .field = "val" });
        try std.testing.expectEqual(@as(f64, 25.0), r.avg());
    }
    // min
    {
        const r = aggregate(&docs, .{ .op = .min, .field = "val" });
        try std.testing.expectEqual(@as(f64, 10.0), r.min);
    }
    // max
    {
        const r = aggregate(&docs, .{ .op = .max, .field = "val" });
        try std.testing.expectEqual(@as(f64, 40.0), r.max);
    }
}

test "edge cases: missing fields, null values, type mismatches" {
    const alloc = std.testing.allocator;

    // Missing field returns no match
    {
        const doc = "{\"x\": 1}";
        var pf = try parseFilter("{\"y\": {\"$gt\": 0}}", alloc);
        defer pf.deinit();
        try std.testing.expect(!matches(doc, &pf.filter));
    }
    // Null value
    {
        const doc = "{\"val\": null}";
        var pf = try parseFilter("{\"val\": {\"$eq\": null}}", alloc);
        defer pf.deinit();
        try std.testing.expect(matches(doc, &pf.filter));
    }
    // Type mismatch: comparing string field with numeric op
    {
        const doc = "{\"name\": \"Alice\"}";
        var pf = try parseFilter("{\"name\": {\"$gt\": 10}}", alloc);
        defer pf.deinit();
        try std.testing.expect(!matches(doc, &pf.filter));
    }
    // Aggregation on empty slice
    {
        const empty: []const []const u8 = &.{};
        const r = aggregate(empty, .{ .op = .avg, .field = "val" });
        try std.testing.expectEqual(@as(u64, 0), r.count);
        try std.testing.expectEqual(@as(f64, 0.0), r.avg());
    }
    // Aggregation with missing fields in some docs
    {
        const docs = [_][]const u8{
            "{\"val\": 10}",
            "{\"other\": 99}",
            "{\"val\": 30}",
        };
        const r = aggregate(&docs, .{ .op = .sum, .field = "val" });
        try std.testing.expectEqual(@as(u64, 2), r.count);
        try std.testing.expectEqual(@as(f64, 40.0), r.sum);
    }
    // Boolean equality
    {
        const doc = "{\"active\": true}";
        var pf = try parseFilter("{\"active\": true}", alloc);
        defer pf.deinit();
        try std.testing.expect(matches(doc, &pf.filter));
    }
}
