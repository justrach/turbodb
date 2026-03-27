/// ZagDB — Package manifest parser/serializer
///
/// Parses and serializes zag.json manifests (JSON-based for MVP).
/// Follows the structure of Zig's build.zig.zon but uses JSON for
/// native TurboDB compatibility.
///
/// Example zag.json:
/// {
///   "name": "my-framework",
///   "version": "0.3.0",
///   "description": "A fast web framework for Zag",
///   "author": "Alice",
///   "license": "MIT",
///   "tags": ["web", "http", "framework"],
///   "zig_version": "0.15.0",
///   "dependencies": {
///     "router": "^1.0.0",
///     "json": {"version": "^0.5.0", "hash": "a3f9c8..."}
///   }
/// }
const std = @import("std");

pub const Dependency = struct {
    name: []const u8,
    version_constraint: []const u8, // semver range like "^1.2.0"
    hash: ?[]const u8 = null, // optional pinned blake3 hash
};

pub const Manifest = struct {
    name: []const u8,
    version: []const u8,
    description: []const u8 = "",
    author: []const u8 = "",
    author_pubkey: []const u8 = "",
    repository: []const u8 = "",
    license: []const u8 = "",
    tags: []const []const u8 = &.{},
    dependencies: []const Dependency = &.{},
    dev_dependencies: []const Dependency = &.{},
    zig_version: []const u8 = "0.15.0",
    visibility: []const u8 = "public", // "public" | "private"
    org: ?[]const u8 = null, // optional org scope (@org/name)
    /// Serialize manifest to JSON for TurboDB storage.
    /// Returns a slice into buf.
    pub fn toJson(self: Manifest, buf: []u8) ![]const u8 {
        var fbs = std.io.fixedBufferStream(buf);
        const w = fbs.writer();

        try w.writeAll("{");
        try w.print("\"name\":\"{s}\"", .{self.name});
        try w.print(",\"version\":\"{s}\"", .{self.version});
        if (self.description.len > 0) try w.print(",\"description\":\"{s}\"", .{self.description});
        if (self.author.len > 0) try w.print(",\"author\":\"{s}\"", .{self.author});
        if (self.author_pubkey.len > 0) try w.print(",\"author_pubkey\":\"{s}\"", .{self.author_pubkey});
        if (self.repository.len > 0) try w.print(",\"repository\":\"{s}\"", .{self.repository});
        if (self.license.len > 0) try w.print(",\"license\":\"{s}\"", .{self.license});
        if (self.zig_version.len > 0) try w.print(",\"zig_version\":\"{s}\"", .{self.zig_version});
        try w.print(",\"visibility\":\"{s}\"", .{self.visibility});
        if (self.org) |org| {
            try w.print(",\"org\":\"{s}\"", .{org});
        }
        // Tags
        if (self.tags.len > 0) {
            try w.writeAll(",\"tags\":[");
            for (self.tags, 0..) |tag, i| {
                if (i > 0) try w.writeAll(",");
                try w.print("\"{s}\"", .{tag});
            }
            try w.writeAll("]");
        }

        // Dependencies
        if (self.dependencies.len > 0) {
            try w.writeAll(",\"dependencies\":{");
            for (self.dependencies, 0..) |dep, i| {
                if (i > 0) try w.writeAll(",");
                if (dep.hash) |h| {
                    try w.print("\"{s}\":{{\"version\":\"{s}\",\"hash\":\"{s}\"}}", .{ dep.name, dep.version_constraint, h });
                } else {
                    try w.print("\"{s}\":\"{s}\"", .{ dep.name, dep.version_constraint });
                }
            }
            try w.writeAll("}");
        }

        // Dev dependencies
        if (self.dev_dependencies.len > 0) {
            try w.writeAll(",\"dev_dependencies\":{");
            for (self.dev_dependencies, 0..) |dep, i| {
                if (i > 0) try w.writeAll(",");
                if (dep.hash) |h| {
                    try w.print("\"{s}\":{{\"version\":\"{s}\",\"hash\":\"{s}\"}}", .{ dep.name, dep.version_constraint, h });
                } else {
                    try w.print("\"{s}\":\"{s}\"", .{ dep.name, dep.version_constraint });
                }
            }
            try w.writeAll("}");
        }

        try w.writeAll("}");
        return fbs.getWritten();
    }
};

/// Generate a template zag.json for `zag init`.
pub fn template(name: []const u8, buf: []u8) ![]const u8 {
    return std.fmt.bufPrint(buf,
        \\{{
        \\  "name": "{s}",
        \\  "version": "0.1.0",
        \\  "description": "",
        \\  "author": "",
        \\  "license": "MIT",
        \\  "zig_version": "0.15.0",
        \\  "tags": [],
        \\  "dependencies": {{}},
        \\  "dev_dependencies": {{}}
        \\}}
    , .{name});
}

// ─── JSON parser (minimal, zero-alloc field extraction) ─────────────────────

/// Extract a string field from JSON. Returns slice into the json buffer.
/// Uses the same approach as doc.zig's jsonGetField.
fn jsonGetStr(json: []const u8, key: []const u8) ?[]const u8 {
    var search_buf: [128]u8 = undefined;
    const needle = std.fmt.bufPrint(&search_buf, "\"{s}\":", .{key}) catch return null;
    const pos = std.mem.indexOf(u8, json, needle) orelse return null;
    var i = pos + needle.len;
    while (i < json.len and (json[i] == ' ' or json[i] == '\t' or json[i] == '\n' or json[i] == '\r')) i += 1;
    if (i >= json.len) return null;
    if (json[i] != '"') return null;
    const start = i + 1;
    var j = start;
    while (j < json.len) : (j += 1) {
        if (json[j] == '\\') {
            j += 1;
            continue;
        }
        if (json[j] == '"') return json[start..j];
    }
    return null;
}

/// Parse a manifest from JSON source.
/// Note: This is a simplified parser for the MVP — it extracts top-level
/// string fields. Full dependency parsing uses the deps parser below.
pub fn parse(alloc: std.mem.Allocator, source: []const u8) !Manifest {
    _ = alloc; // deps parsing will need alloc in future
    return .{
        .name = jsonGetStr(source, "name") orelse return error.MissingName,
        .version = jsonGetStr(source, "version") orelse return error.MissingVersion,
        .description = jsonGetStr(source, "description") orelse "",
        .author = jsonGetStr(source, "author") orelse "",
        .author_pubkey = jsonGetStr(source, "author_pubkey") orelse "",
        .repository = jsonGetStr(source, "repository") orelse "",
        .license = jsonGetStr(source, "license") orelse "",
        .zig_version = jsonGetStr(source, "zig_version") orelse "0.15.0",
        .visibility = jsonGetStr(source, "visibility") orelse "public",
        .org = jsonGetStr(source, "org"),
    };
}

// ─── Tests ──────────────────────────────────────────────────────────────────

test "template generation" {
    var buf: [1024]u8 = undefined;
    const json = try template("my-package", &buf);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"my-package\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"0.1.0\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"dependencies\"") != null);
}

test "manifest toJson" {
    const tags = [_][]const u8{ "web", "http" };
    const deps = [_]Dependency{
        .{ .name = "router", .version_constraint = "^1.0.0" },
        .{ .name = "json", .version_constraint = "^0.5.0", .hash = "abc123" },
    };

    const m = Manifest{
        .name = "test-pkg",
        .version = "1.0.0",
        .description = "A test package",
        .author = "Alice",
        .license = "MIT",
        .tags = &tags,
        .dependencies = &deps,
    };

    var buf: [4096]u8 = undefined;
    const json = try m.toJson(&buf);

    try std.testing.expect(std.mem.indexOf(u8, json, "\"test-pkg\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"1.0.0\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"router\":\"^1.0.0\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"abc123\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"web\"") != null);
}

test "parse manifest" {
    const json =
        \\{"name":"my-pkg","version":"2.1.0","description":"Cool pkg","author":"Bob","license":"Apache-2.0"}
    ;
    const m = try parse(std.testing.allocator, json);
    try std.testing.expectEqualStrings("my-pkg", m.name);
    try std.testing.expectEqualStrings("2.1.0", m.version);
    try std.testing.expectEqualStrings("Cool pkg", m.description);
    try std.testing.expectEqualStrings("Bob", m.author);
    try std.testing.expectEqualStrings("Apache-2.0", m.license);
}

test "parse manifest missing name" {
    const json = "{\"version\":\"1.0.0\"}";
    try std.testing.expectError(error.MissingName, parse(std.testing.allocator, json));
}

test "parse manifest missing version" {
    const json = "{\"name\":\"test\"}";
    try std.testing.expectError(error.MissingVersion, parse(std.testing.allocator, json));
}

test "round-trip: toJson then parse" {
    const m = Manifest{
        .name = "round-trip",
        .version = "3.0.0",
        .description = "Testing round trip",
        .author = "Charlie",
        .license = "MIT",
    };

    var buf: [4096]u8 = undefined;
    const json = try m.toJson(&buf);
    const parsed = try parse(std.testing.allocator, json);

    try std.testing.expectEqualStrings(m.name, parsed.name);
    try std.testing.expectEqualStrings(m.version, parsed.version);
    try std.testing.expectEqualStrings(m.description, parsed.description);
    try std.testing.expectEqualStrings(m.author, parsed.author);
}
