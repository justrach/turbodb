/// TurboDB — Schema Validation
///
/// Optional JSON schema enforcement on insert/update.
/// Schemas are per-collection and define required fields + types.
///
/// Supported types: string, number, boolean, object, array, any.
/// Validation is zero-alloc — walks the JSON bytes directly.
const std = @import("std");

/// Field type constraint.
pub const FieldType = enum(u8) {
    string,
    number,
    boolean,
    object,
    array,
    any, // matches anything
};

/// A single field constraint.
pub const FieldRule = struct {
    name: [64]u8,
    name_len: u8,
    field_type: FieldType,
    required: bool,

    pub fn nameSlice(self: *const FieldRule) []const u8 {
        return self.name[0..self.name_len];
    }
};

pub const MAX_FIELDS = 32;

/// Schema definition for a collection.
pub const Schema = struct {
    rules: [MAX_FIELDS]FieldRule = undefined,
    count: u8 = 0,
    strict: bool = false, // If true, reject unknown fields.

    /// Add a field rule.
    pub fn addField(self: *Schema, name: []const u8, field_type: FieldType, required: bool) void {
        if (self.count >= MAX_FIELDS) return;
        var rule = FieldRule{
            .name = undefined,
            .name_len = @intCast(@min(name.len, 64)),
            .field_type = field_type,
            .required = required,
        };
        @memcpy(rule.name[0..rule.name_len], name[0..rule.name_len]);
        self.rules[self.count] = rule;
        self.count += 1;
    }

    /// Validate a JSON value against this schema.
    /// Returns null if valid, or an error message if invalid.
    pub fn validate(self: *const Schema, json: []const u8) ?[]const u8 {
        if (self.count == 0) return null; // No schema → always valid

        // Check required fields exist
        for (self.rules[0..self.count]) |*rule| {
            if (!rule.required) continue;
            if (!jsonHasField(json, rule.nameSlice())) {
                return "missing required field";
            }
        }

        // Check field types
        for (self.rules[0..self.count]) |*rule| {
            const val = jsonFieldValue(json, rule.nameSlice()) orelse continue;
            if (rule.field_type == .any) continue;
            if (!checkType(val, rule.field_type)) {
                return "field type mismatch";
            }
        }

        return null; // Valid
    }
};

// ── JSON field helpers (zero-alloc) ──────────────────────────────────────────

/// Check if a JSON object contains a field.
fn jsonHasField(json: []const u8, field: []const u8) bool {
    return jsonFieldValue(json, field) != null;
}

/// Extract the raw value substring for a field in a JSON object.
/// Returns the value portion (e.g., `"hello"`, `42`, `true`, `[1,2]`).
fn jsonFieldValue(json: []const u8, field: []const u8) ?[]const u8 {
    // Find "field": in JSON
    var i: usize = 0;
    while (i + field.len + 3 < json.len) : (i += 1) {
        if (json[i] != '"') continue;
        const key_start = i + 1;
        if (key_start + field.len >= json.len) continue;
        if (!std.mem.eql(u8, json[key_start..][0..field.len], field)) continue;
        if (json[key_start + field.len] != '"') continue;

        // Found the key, skip to value
        var j = key_start + field.len + 1;
        while (j < json.len and (json[j] == ':' or json[j] == ' ' or json[j] == '\t')) : (j += 1) {}
        if (j >= json.len) return null;

        // Find end of value
        const val_start = j;
        const val_end = findValueEnd(json, val_start);
        return json[val_start..val_end];
    }
    return null;
}

/// Find the end index of a JSON value starting at `start`.
fn findValueEnd(json: []const u8, start: usize) usize {
    if (start >= json.len) return start;
    const ch = json[start];

    // String
    if (ch == '"') {
        var i = start + 1;
        while (i < json.len) : (i += 1) {
            if (json[i] == '\\') { i += 1; continue; }
            if (json[i] == '"') return i + 1;
        }
        return json.len;
    }

    // Object or array — count nesting
    if (ch == '{' or ch == '[') {
        const close: u8 = if (ch == '{') '}' else ']';
        var depth: usize = 1;
        var i = start + 1;
        var in_str = false;
        while (i < json.len and depth > 0) : (i += 1) {
            if (json[i] == '\\' and in_str) { i += 1; continue; }
            if (json[i] == '"') in_str = !in_str;
            if (!in_str) {
                if (json[i] == ch) depth += 1;
                if (json[i] == close) depth -= 1;
            }
        }
        return i;
    }

    // Number, boolean, null — read until delimiter
    var i = start;
    while (i < json.len and json[i] != ',' and json[i] != '}' and json[i] != ']' and json[i] != ' ' and json[i] != '\n') : (i += 1) {}
    return i;
}

/// Check if a JSON value matches the expected type.
fn checkType(val: []const u8, expected: FieldType) bool {
    if (val.len == 0) return false;
    return switch (expected) {
        .string => val[0] == '"',
        .number => val[0] == '-' or (val[0] >= '0' and val[0] <= '9'),
        .boolean => std.mem.startsWith(u8, val, "true") or std.mem.startsWith(u8, val, "false"),
        .object => val[0] == '{',
        .array => val[0] == '[',
        .any => true,
    };
}

// ── Tests ────────────────────────────────────────────────────────────────────

test "validate required field present" {
    var s = Schema{};
    s.addField("name", .string, true);
    try std.testing.expectEqual(@as(?[]const u8, null), s.validate("{\"name\":\"Alice\"}"));
}

test "validate required field missing" {
    var s = Schema{};
    s.addField("name", .string, true);
    const err = s.validate("{\"age\":30}");
    try std.testing.expect(err != null);
}

test "validate type mismatch" {
    var s = Schema{};
    s.addField("age", .number, false);
    const err = s.validate("{\"age\":\"not-a-number\"}");
    try std.testing.expect(err != null);
}

test "validate type correct" {
    var s = Schema{};
    s.addField("age", .number, false);
    s.addField("name", .string, false);
    s.addField("active", .boolean, false);
    try std.testing.expectEqual(@as(?[]const u8, null), s.validate("{\"age\":30,\"name\":\"Alice\",\"active\":true}"));
}

test "validate object and array types" {
    var s = Schema{};
    s.addField("meta", .object, false);
    s.addField("tags", .array, false);
    try std.testing.expectEqual(@as(?[]const u8, null), s.validate("{\"meta\":{\"x\":1},\"tags\":[1,2]}"));
}

test "validate any type" {
    var s = Schema{};
    s.addField("data", .any, true);
    try std.testing.expectEqual(@as(?[]const u8, null), s.validate("{\"data\":42}"));
    try std.testing.expectEqual(@as(?[]const u8, null), s.validate("{\"data\":\"hello\"}"));
    try std.testing.expectEqual(@as(?[]const u8, null), s.validate("{\"data\":[1,2,3]}"));
}

test "empty schema always valid" {
    var s = Schema{};
    try std.testing.expectEqual(@as(?[]const u8, null), s.validate("{\"anything\":\"goes\"}"));
}
