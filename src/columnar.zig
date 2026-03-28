// columnar.zig — Columnar projections for analytical queries
//
// Stores per-field append-only columns with null bitmaps, enabling
// analytical queries to read only the fields they need instead of
// materializing entire JSON documents. Includes dictionary encoding
// for low-cardinality strings and run-length encoding for sorted /
// repeated integer columns.

const std = @import("std");
const Allocator = std.mem.Allocator;

// ---------------------------------------------------------------------------
// Column types and values
// ---------------------------------------------------------------------------

pub const ColumnType = enum(u8) {
    string,
    integer,
    float,
    boolean,
    null_type,
};

pub const ColumnValue = union(enum) {
    string: []const u8,
    integer: i64,
    float: f64,
    boolean: bool,
    null_val: void,
};

// ---------------------------------------------------------------------------
// FilterOp — predicate operators for vectorized column scans
// ---------------------------------------------------------------------------

pub const FilterOp = enum { eq, ne, gt, gte, lt, lte };

// ---------------------------------------------------------------------------
// Column — per-field append-only store with null bitmap
// ---------------------------------------------------------------------------

pub const Column = struct {
    field_name: [64]u8,
    field_name_len: u8,
    col_type: ColumnType,

    // Type-specific value stores
    values_i64: std.ArrayListUnmanaged(i64) = .empty,
    values_f64: std.ArrayListUnmanaged(f64) = .empty,
    values_bool: std.ArrayListUnmanaged(bool) = .empty,

    // String storage: offset/length pairs into contiguous buffer
    string_offsets: std.ArrayListUnmanaged(u32) = .empty,
    string_data: std.ArrayListUnmanaged(u8) = .empty,

    // Null bitmap — 1 bit per row, bit=1 means null
    null_bitmap: std.ArrayListUnmanaged(u8) = .empty,
    row_count: u64 = 0,

    pub fn init(field_name: []const u8, col_type: ColumnType) Column {
        var name_buf: [64]u8 = .{0} ** 64;
        const copy_len: u8 = @intCast(@min(field_name.len, 64));
        @memcpy(name_buf[0..copy_len], field_name[0..copy_len]);

        return .{
            .field_name = name_buf,
            .field_name_len = copy_len,
            .col_type = col_type,
        };
    }

    pub fn deinit(self: *Column, alloc: Allocator) void {
        self.values_i64.deinit(alloc);
        self.values_f64.deinit(alloc);
        self.values_bool.deinit(alloc);
        self.string_offsets.deinit(alloc);
        self.string_data.deinit(alloc);
        self.null_bitmap.deinit(alloc);
    }

    pub fn append(self: *Column, alloc: Allocator, value: ColumnValue) !void {
        // Grow null bitmap when we cross a byte boundary
        const byte_idx = self.row_count / 8;
        if (byte_idx >= self.null_bitmap.items.len) {
            try self.null_bitmap.append(alloc, 0);
        }

        const is_null = value == .null_val;
        if (is_null) {
            // Set null bit
            const bit: u3 = @intCast(self.row_count % 8);
            self.null_bitmap.items[byte_idx] |= @as(u8, 1) << bit;
        }

        switch (self.col_type) {
            .integer => {
                const v: i64 = if (is_null) 0 else switch (value) {
                    .integer => |i| i,
                    else => 0,
                };
                try self.values_i64.append(alloc, v);
            },
            .float => {
                const v: f64 = if (is_null) 0.0 else switch (value) {
                    .float => |f| f,
                    else => 0.0,
                };
                try self.values_f64.append(alloc, v);
            },
            .boolean => {
                const v: bool = if (is_null) false else switch (value) {
                    .boolean => |b| b,
                    else => false,
                };
                try self.values_bool.append(alloc, v);
            },
            .string => {
                if (is_null) {
                    const cur: u32 = @intCast(self.string_data.items.len);
                    try self.string_offsets.append(alloc, cur);
                    try self.string_offsets.append(alloc, cur);
                } else {
                    const s: []const u8 = switch (value) {
                        .string => |sl| sl,
                        else => "",
                    };
                    const start: u32 = @intCast(self.string_data.items.len);
                    try self.string_data.appendSlice(alloc, s);
                    const end: u32 = @intCast(self.string_data.items.len);
                    try self.string_offsets.append(alloc, start);
                    try self.string_offsets.append(alloc, end);
                }
            },
            .null_type => {},
        }

        self.row_count += 1;
    }

    pub fn get(self: *const Column, row_idx: u64) ?ColumnValue {
        if (row_idx >= self.row_count) return null;
        if (self.isNull(row_idx)) return .{ .null_val = {} };

        return switch (self.col_type) {
            .integer => .{ .integer = self.values_i64.items[row_idx] },
            .float => .{ .float = self.values_f64.items[row_idx] },
            .boolean => .{ .boolean = self.values_bool.items[row_idx] },
            .string => blk: {
                const off_idx = row_idx * 2;
                const start = self.string_offsets.items[off_idx];
                const end = self.string_offsets.items[off_idx + 1];
                break :blk .{ .string = self.string_data.items[start..end] };
            },
            .null_type => .{ .null_val = {} },
        };
    }

    pub fn isNull(self: *const Column, row_idx: u64) bool {
        const byte_idx = row_idx / 8;
        if (byte_idx >= self.null_bitmap.items.len) return false;
        const bit: u3 = @intCast(row_idx % 8);
        return (self.null_bitmap.items[byte_idx] & (@as(u8, 1) << bit)) != 0;
    }
};

// ---------------------------------------------------------------------------
// DictEncoded — dictionary encoding for low-cardinality string columns
// ---------------------------------------------------------------------------

pub const DictEncoded = struct {
    dictionary: std.ArrayListUnmanaged([]const u8) = .empty,
    codes: std.ArrayListUnmanaged(u32) = .empty,
    dict_map: std.StringHashMapUnmanaged(u32) = .empty,

    pub fn init() DictEncoded {
        return .{};
    }

    pub fn deinit(self: *DictEncoded, alloc: Allocator) void {
        for (self.dictionary.items) |s| {
            alloc.free(s);
        }
        self.dictionary.deinit(alloc);
        self.codes.deinit(alloc);
        self.dict_map.deinit(alloc);
    }

    /// Encode a string value, returning its dictionary code.
    /// Inserts into the dictionary if not already present.
    pub fn encode(self: *DictEncoded, alloc: Allocator, value: []const u8) !u32 {
        if (self.dict_map.get(value)) |code| {
            try self.codes.append(alloc, code);
            return code;
        }
        const owned = try alloc.dupe(u8, value);
        const code: u32 = @intCast(self.dictionary.items.len);
        try self.dictionary.append(alloc, owned);
        try self.dict_map.put(alloc, owned, code);
        try self.codes.append(alloc, code);
        return code;
    }

    pub fn decode(self: *const DictEncoded, code: u32) ?[]const u8 {
        if (code >= self.dictionary.items.len) return null;
        return self.dictionary.items[code];
    }

    pub fn cardinality(self: *const DictEncoded) usize {
        return self.dictionary.items.len;
    }
};

// ---------------------------------------------------------------------------
// RLEEncoded — run-length encoding for sorted / repeated integer columns
// ---------------------------------------------------------------------------

pub const RLEEncoded = struct {
    values: std.ArrayListUnmanaged(i64) = .empty,
    run_lengths: std.ArrayListUnmanaged(u32) = .empty,
    total_count: u64 = 0,

    pub fn init() RLEEncoded {
        return .{};
    }

    pub fn deinit(self: *RLEEncoded, alloc: Allocator) void {
        self.values.deinit(alloc);
        self.run_lengths.deinit(alloc);
    }

    pub fn append(self: *RLEEncoded, alloc: Allocator, value: i64) !void {
        if (self.values.items.len > 0 and self.values.items[self.values.items.len - 1] == value) {
            self.run_lengths.items[self.run_lengths.items.len - 1] += 1;
        } else {
            try self.values.append(alloc, value);
            try self.run_lengths.append(alloc, 1);
        }
        self.total_count += 1;
    }

    pub fn get(self: *const RLEEncoded, idx: u64) ?i64 {
        if (idx >= self.total_count) return null;
        var remaining = idx;
        for (self.values.items, 0..) |val, i| {
            const run: u64 = self.run_lengths.items[i];
            if (remaining < run) return val;
            remaining -= run;
        }
        return null;
    }

    /// Ratio of logical rows to physical (value, run_length) pairs.
    pub fn compressionRatio(self: *const RLEEncoded) f64 {
        if (self.values.items.len == 0) return 1.0;
        return @as(f64, @floatFromInt(self.total_count)) /
            @as(f64, @floatFromInt(self.values.items.len));
    }
};

// ---------------------------------------------------------------------------
// Projection — manages a set of columns for a collection
// ---------------------------------------------------------------------------

pub const Projection = struct {
    columns: std.StringHashMapUnmanaged(*Column) = .empty,
    doc_count: u64 = 0,
    alloc: Allocator,

    pub fn init(alloc: Allocator) Projection {
        return .{ .alloc = alloc };
    }

    pub fn deinit(self: *Projection) void {
        var it = self.columns.iterator();
        while (it.next()) |entry| {
            entry.value_ptr.*.deinit(self.alloc);
            self.alloc.destroy(entry.value_ptr.*);
        }
        self.columns.deinit(self.alloc);
    }

    pub fn addField(self: *Projection, field_name: []const u8, col_type: ColumnType) !void {
        const col = try self.alloc.create(Column);
        col.* = Column.init(field_name, col_type);
        const stable_name = col.field_name[0..col.field_name_len];
        try self.columns.put(self.alloc, stable_name, col);
    }

    /// Append a row by extracting projected fields from a JSON document.
    pub fn appendRow(self: *Projection, json_value: []const u8) !void {
        var it = self.columns.iterator();
        while (it.next()) |entry| {
            const col = entry.value_ptr.*;
            const name = col.field_name[0..col.field_name_len];
            const cv = extractJsonField(json_value, name);
            if (cv) |v| {
                try col.append(self.alloc, v);
            } else {
                try col.append(self.alloc, .{ .null_val = {} });
            }
        }
        self.doc_count += 1;
    }

    pub fn getColumn(self: *const Projection, field_name: []const u8) ?*Column {
        return self.columns.get(field_name);
    }

    /// Return a slice of all values in the named column.
    /// Caller owns the returned slice and must free via alloc.
    pub fn columnScan(self: *const Projection, field_name: []const u8) ?[]ColumnValue {
        const col = self.columns.get(field_name) orelse return null;
        const out = self.alloc.alloc(ColumnValue, col.row_count) catch return null;
        for (0..col.row_count) |i| {
            out[i] = col.get(i) orelse .{ .null_val = {} };
        }
        return out;
    }

    /// Return a list of projected field names.
    /// Caller owns the returned slice and must free via alloc.
    pub fn fieldNames(self: *const Projection) ?[][]const u8 {
        const count = self.columns.count();
        if (count == 0) return null;
        const names = self.alloc.alloc([]const u8, count) catch return null;
        var it = self.columns.iterator();
        var idx: usize = 0;
        while (it.next()) |entry| {
            const col = entry.value_ptr.*;
            names[idx] = col.field_name[0..col.field_name_len];
            idx += 1;
        }
        return names;
    }
};

// ---------------------------------------------------------------------------
// JSON field extraction — parse a typed ColumnValue from raw JSON bytes
// ---------------------------------------------------------------------------

/// Extract a field from flat JSON and return its typed ColumnValue.
/// Handles strings, integers, floats, booleans, and null.
pub fn extractJsonField(json: []const u8, field: []const u8) ?ColumnValue {
    var search_buf: [128]u8 = undefined;
    const needle = std.fmt.bufPrint(&search_buf, "\"{s}\":", .{field}) catch return null;
    const pos = std.mem.indexOf(u8, json, needle) orelse return null;
    var i = pos + needle.len;

    // Skip whitespace
    while (i < json.len and (json[i] == ' ' or json[i] == '\t')) i += 1;
    if (i >= json.len) return null;

    // Nested objects/arrays — skip
    if (json[i] == '{' or json[i] == '[') return null;

    // String value
    if (json[i] == '"') {
        const start = i + 1;
        var j = start;
        while (j < json.len) : (j += 1) {
            if (json[j] == '\\') {
                j += 1;
                continue;
            }
            if (json[j] == '"') return .{ .string = json[start..j] };
        }
        return null;
    }

    // Determine end of token
    var end = i;
    while (end < json.len and json[end] != ',' and json[end] != '}' and
        json[end] != '\n' and json[end] != ' ' and json[end] != ']') end += 1;
    const token = json[i..end];

    // null
    if (std.mem.eql(u8, token, "null")) return .{ .null_val = {} };
    // boolean
    if (std.mem.eql(u8, token, "true")) return .{ .boolean = true };
    if (std.mem.eql(u8, token, "false")) return .{ .boolean = false };
    // Try integer first, then float
    if (std.fmt.parseInt(i64, token, 10)) |iv| {
        return .{ .integer = iv };
    } else |_| {}
    if (std.fmt.parseFloat(f64, token)) |fv| {
        return .{ .float = fv };
    } else |_| {}

    return null;
}

// ---------------------------------------------------------------------------
// Vectorized predicate evaluation
// ---------------------------------------------------------------------------

/// Evaluate `column[row] <op> value` for every row; set bits in result_bitmap
/// where the predicate is true.
pub fn filterColumn(column: *const Column, op: FilterOp, value: ColumnValue, result_bitmap: []u8) void {
    @memset(result_bitmap, 0);

    var row: u64 = 0;
    while (row < column.row_count) : (row += 1) {
        if (column.isNull(row)) continue;
        const cv = column.get(row) orelse continue;
        if (evalPredicate(cv, op, value)) {
            const byte_idx = row / 8;
            const bit: u3 = @intCast(row % 8);
            if (byte_idx < result_bitmap.len) {
                result_bitmap[byte_idx] |= @as(u8, 1) << bit;
            }
        }
    }
}

/// Compare two ColumnValues with the given operator.
fn evalPredicate(lhs: ColumnValue, op: FilterOp, rhs: ColumnValue) bool {
    switch (lhs) {
        .integer => |a| {
            const b: i64 = switch (rhs) {
                .integer => |v| v,
                else => return false,
            };
            return switch (op) {
                .eq => a == b,
                .ne => a != b,
                .gt => a > b,
                .gte => a >= b,
                .lt => a < b,
                .lte => a <= b,
            };
        },
        .float => |a| {
            const b: f64 = switch (rhs) {
                .float => |v| v,
                else => return false,
            };
            return switch (op) {
                .eq => a == b,
                .ne => a != b,
                .gt => a > b,
                .gte => a >= b,
                .lt => a < b,
                .lte => a <= b,
            };
        },
        .boolean => |a| {
            const b: bool = switch (rhs) {
                .boolean => |v| v,
                else => return false,
            };
            const ai: u1 = @intFromBool(a);
            const bi: u1 = @intFromBool(b);
            return switch (op) {
                .eq => ai == bi,
                .ne => ai != bi,
                .gt => ai > bi,
                .gte => ai >= bi,
                .lt => ai < bi,
                .lte => ai <= bi,
            };
        },
        .string => |a| {
            const b: []const u8 = switch (rhs) {
                .string => |v| v,
                else => return false,
            };
            const order = std.mem.order(u8, a, b);
            return switch (op) {
                .eq => order == .eq,
                .ne => order != .eq,
                .gt => order == .gt,
                .gte => order != .lt,
                .lt => order == .lt,
                .lte => order != .gt,
            };
        },
        .null_val => return op == .eq and rhs == .null_val,
    }
}

// ===========================================================================
// Tests
// ===========================================================================

test "column append and get — integer" {
    const alloc = std.testing.allocator;
    var col = Column.init("age", .integer);
    defer col.deinit(alloc);

    try col.append(alloc, .{ .integer = 25 });
    try col.append(alloc, .{ .integer = 30 });
    try col.append(alloc, .{ .integer = -7 });

    try std.testing.expectEqual(@as(i64, 25), col.get(0).?.integer);
    try std.testing.expectEqual(@as(i64, 30), col.get(1).?.integer);
    try std.testing.expectEqual(@as(i64, -7), col.get(2).?.integer);
    try std.testing.expectEqual(@as(?ColumnValue, null), col.get(3));
    try std.testing.expectEqual(@as(u64, 3), col.row_count);
}

test "column append and get — float" {
    const alloc = std.testing.allocator;
    var col = Column.init("price", .float);
    defer col.deinit(alloc);

    try col.append(alloc, .{ .float = 3.14 });
    try col.append(alloc, .{ .float = -0.5 });

    try std.testing.expectApproxEqAbs(@as(f64, 3.14), col.get(0).?.float, 1e-12);
    try std.testing.expectApproxEqAbs(@as(f64, -0.5), col.get(1).?.float, 1e-12);
}

test "column append and get — string" {
    const alloc = std.testing.allocator;
    var col = Column.init("name", .string);
    defer col.deinit(alloc);

    try col.append(alloc, .{ .string = "alice" });
    try col.append(alloc, .{ .string = "bob" });

    try std.testing.expectEqualStrings("alice", col.get(0).?.string);
    try std.testing.expectEqualStrings("bob", col.get(1).?.string);
}

test "column append and get — boolean" {
    const alloc = std.testing.allocator;
    var col = Column.init("active", .boolean);
    defer col.deinit(alloc);

    try col.append(alloc, .{ .boolean = true });
    try col.append(alloc, .{ .boolean = false });

    try std.testing.expect(col.get(0).?.boolean == true);
    try std.testing.expect(col.get(1).?.boolean == false);
}

test "null bitmap handling" {
    const alloc = std.testing.allocator;
    var col = Column.init("score", .integer);
    defer col.deinit(alloc);

    try col.append(alloc, .{ .integer = 10 });
    try col.append(alloc, .{ .null_val = {} });
    try col.append(alloc, .{ .integer = 30 });
    try col.append(alloc, .{ .null_val = {} });

    try std.testing.expect(!col.isNull(0));
    try std.testing.expect(col.isNull(1));
    try std.testing.expect(!col.isNull(2));
    try std.testing.expect(col.isNull(3));

    try std.testing.expectEqual(@as(i64, 10), col.get(0).?.integer);
    try std.testing.expect(col.get(1).? == .null_val);
    try std.testing.expectEqual(@as(i64, 30), col.get(2).?.integer);
    try std.testing.expect(col.get(3).? == .null_val);
}

test "DictEncoded encode/decode" {
    const alloc = std.testing.allocator;
    var dict = DictEncoded.init();
    defer dict.deinit(alloc);

    const c0 = try dict.encode(alloc, "red");
    const c1 = try dict.encode(alloc, "green");
    const c2 = try dict.encode(alloc, "red");

    try std.testing.expectEqual(@as(u32, 0), c0);
    try std.testing.expectEqual(@as(u32, 1), c1);
    try std.testing.expectEqual(@as(u32, 0), c2);

    try std.testing.expectEqualStrings("red", dict.decode(0).?);
    try std.testing.expectEqualStrings("green", dict.decode(1).?);
    try std.testing.expectEqual(@as(?[]const u8, null), dict.decode(99));
}

test "DictEncoded cardinality" {
    const alloc = std.testing.allocator;
    var dict = DictEncoded.init();
    defer dict.deinit(alloc);

    _ = try dict.encode(alloc, "a");
    _ = try dict.encode(alloc, "b");
    _ = try dict.encode(alloc, "a");
    _ = try dict.encode(alloc, "c");
    _ = try dict.encode(alloc, "b");

    try std.testing.expectEqual(@as(usize, 3), dict.cardinality());
}

test "RLEEncoded append/get with repeated values" {
    const alloc = std.testing.allocator;
    var rle = RLEEncoded.init();
    defer rle.deinit(alloc);

    try rle.append(alloc, 100);
    try rle.append(alloc, 100);
    try rle.append(alloc, 100);
    try rle.append(alloc, 200);
    try rle.append(alloc, 200);
    try rle.append(alloc, 300);

    try std.testing.expectEqual(@as(u64, 6), rle.total_count);
    try std.testing.expectEqual(@as(usize, 3), rle.values.items.len);

    try std.testing.expectEqual(@as(i64, 100), rle.get(0).?);
    try std.testing.expectEqual(@as(i64, 100), rle.get(2).?);
    try std.testing.expectEqual(@as(i64, 200), rle.get(3).?);
    try std.testing.expectEqual(@as(i64, 200), rle.get(4).?);
    try std.testing.expectEqual(@as(i64, 300), rle.get(5).?);
    try std.testing.expectEqual(@as(?i64, null), rle.get(6));
}

test "RLE compression ratio" {
    const alloc = std.testing.allocator;
    var rle = RLEEncoded.init();
    defer rle.deinit(alloc);

    for (0..10) |_| {
        try rle.append(alloc, 42);
    }
    try std.testing.expectApproxEqAbs(@as(f64, 10.0), rle.compressionRatio(), 1e-12);

    var rle2 = RLEEncoded.init();
    defer rle2.deinit(alloc);
    for (0..5) |i| {
        try rle2.append(alloc, @intCast(i));
    }
    try std.testing.expectApproxEqAbs(@as(f64, 1.0), rle2.compressionRatio(), 1e-12);
}

test "Projection addField + appendRow from JSON" {
    const alloc = std.testing.allocator;
    var proj = Projection.init(alloc);
    defer proj.deinit();

    try proj.addField("age", .integer);
    try proj.addField("name", .string);

    try proj.appendRow("{\"name\":\"alice\",\"age\":25,\"extra\":true}");
    try proj.appendRow("{\"name\":\"bob\",\"age\":30}");

    const age_col = proj.getColumn("age").?;
    try std.testing.expectEqual(@as(i64, 25), age_col.get(0).?.integer);
    try std.testing.expectEqual(@as(i64, 30), age_col.get(1).?.integer);

    const name_col = proj.getColumn("name").?;
    try std.testing.expectEqualStrings("alice", name_col.get(0).?.string);
    try std.testing.expectEqualStrings("bob", name_col.get(1).?.string);

    try std.testing.expectEqual(@as(u64, 2), proj.doc_count);
}

test "extractJsonField for various JSON types" {
    const json = "{\"s\":\"hello\",\"i\":42,\"f\":3.14,\"bt\":true,\"bf\":false,\"n\":null}";

    const sv = extractJsonField(json, "s").?;
    try std.testing.expectEqualStrings("hello", sv.string);

    const iv = extractJsonField(json, "i").?;
    try std.testing.expectEqual(@as(i64, 42), iv.integer);

    const fv = extractJsonField(json, "f").?;
    try std.testing.expectApproxEqAbs(@as(f64, 3.14), fv.float, 1e-12);

    const bt = extractJsonField(json, "bt").?;
    try std.testing.expect(bt.boolean == true);

    const bf = extractJsonField(json, "bf").?;
    try std.testing.expect(bf.boolean == false);

    const nv = extractJsonField(json, "n").?;
    try std.testing.expect(nv == .null_val);

    try std.testing.expectEqual(@as(?ColumnValue, null), extractJsonField(json, "missing"));
}

test "filterColumn with comparison operators" {
    const alloc = std.testing.allocator;
    var col = Column.init("val", .integer);
    defer col.deinit(alloc);

    for ([_]i64{ 10, 20, 30, 40, 50 }) |v| {
        try col.append(alloc, .{ .integer = v });
    }

    var bitmap: [1]u8 = .{0};

    // eq 30 -> only row 2
    filterColumn(&col, .eq, .{ .integer = 30 }, &bitmap);
    try std.testing.expectEqual(@as(u8, 0b00000100), bitmap[0]);

    // gt 25 -> rows 2,3,4 (30,40,50)
    filterColumn(&col, .gt, .{ .integer = 25 }, &bitmap);
    try std.testing.expectEqual(@as(u8, 0b00011100), bitmap[0]);

    // lte 20 -> rows 0,1 (10,20)
    filterColumn(&col, .lte, .{ .integer = 20 }, &bitmap);
    try std.testing.expectEqual(@as(u8, 0b00000011), bitmap[0]);

    // ne 40 -> rows 0,1,2,4
    filterColumn(&col, .ne, .{ .integer = 40 }, &bitmap);
    try std.testing.expectEqual(@as(u8, 0b00010111), bitmap[0]);
}

test "end-to-end: projection -> insert JSON -> query column" {
    const alloc = std.testing.allocator;
    var proj = Projection.init(alloc);
    defer proj.deinit();

    try proj.addField("status", .string);
    try proj.addField("count", .integer);

    const docs = [_][]const u8{
        "{\"status\":\"active\",\"count\":5}",
        "{\"status\":\"inactive\",\"count\":0}",
        "{\"status\":\"active\",\"count\":12}",
        "{\"status\":\"pending\",\"count\":3}",
    };
    for (docs) |d| {
        try proj.appendRow(d);
    }

    const count_col = proj.getColumn("count").?;
    try std.testing.expectEqual(@as(u64, 4), count_col.row_count);

    // Filter: count > 3
    var bitmap: [1]u8 = .{0};
    filterColumn(count_col, .gt, .{ .integer = 3 }, &bitmap);
    // Rows 0 (5) and 2 (12) match -> bits 0 and 2
    try std.testing.expectEqual(@as(u8, 0b00000101), bitmap[0]);

    const status_col = proj.getColumn("status").?;
    try std.testing.expectEqualStrings("active", status_col.get(0).?.string);
    try std.testing.expectEqualStrings("inactive", status_col.get(1).?.string);
    try std.testing.expectEqualStrings("active", status_col.get(2).?.string);
    try std.testing.expectEqualStrings("pending", status_col.get(3).?.string);

    // columnScan returns all values
    const scan = proj.columnScan("count");
    if (scan) |s| {
        defer alloc.free(s);
        try std.testing.expectEqual(@as(usize, 4), s.len);
        try std.testing.expectEqual(@as(i64, 5), s[0].integer);
        try std.testing.expectEqual(@as(i64, 0), s[1].integer);
    } else {
        return error.TestUnexpectedResult;
    }
}
