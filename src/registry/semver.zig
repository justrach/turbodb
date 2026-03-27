/// ZagDB — Semantic version parsing and constraint matching
///
/// Follows Cargo/npm conventions:
///   ^1.2.3 → >=1.2.3, <2.0.0  (caret = compatible changes)
///   ~1.2.3 → >=1.2.3, <1.3.0  (tilde = patch-level)
///   >=, <=, =, >, < operators
///   Compound ranges: ">=1.0.0, <2.0.0"
const std = @import("std");

pub const Version = struct {
    major: u32,
    minor: u32,
    patch: u32,
    pre_release: ?[]const u8 = null,

    /// Parse a semver string like "1.2.3" or "1.2.3-alpha.1"
    pub fn parse(s: []const u8) !Version {
        var rest = s;

        // Parse major
        const dot1 = std.mem.indexOfScalar(u8, rest, '.') orelse return error.InvalidVersion;
        const major = std.fmt.parseInt(u32, rest[0..dot1], 10) catch return error.InvalidVersion;
        rest = rest[dot1 + 1 ..];

        // Parse minor
        const dot2 = std.mem.indexOfScalar(u8, rest, '.') orelse {
            // Allow "1.2" as "1.2.0"
            const minor = std.fmt.parseInt(u32, rest, 10) catch return error.InvalidVersion;
            return .{ .major = major, .minor = minor, .patch = 0 };
        };
        const minor = std.fmt.parseInt(u32, rest[0..dot2], 10) catch return error.InvalidVersion;
        rest = rest[dot2 + 1 ..];

        // Parse patch + optional pre-release
        const hyphen = std.mem.indexOfScalar(u8, rest, '-');
        const patch_str = if (hyphen) |h| rest[0..h] else rest;
        const patch = std.fmt.parseInt(u32, patch_str, 10) catch return error.InvalidVersion;
        const pre = if (hyphen) |h| rest[h + 1 ..] else null;

        return .{
            .major = major,
            .minor = minor,
            .patch = patch,
            .pre_release = if (pre) |p| (if (p.len > 0) p else null) else null,
        };
    }

    /// Format version as "major.minor.patch[-pre]"
    pub fn format(self: Version, buf: []u8) ![]const u8 {
        if (self.pre_release) |pre| {
            return std.fmt.bufPrint(buf, "{d}.{d}.{d}-{s}", .{ self.major, self.minor, self.patch, pre });
        }
        return std.fmt.bufPrint(buf, "{d}.{d}.{d}", .{ self.major, self.minor, self.patch });
    }

    /// Compare two versions. Pre-release versions are less than release versions.
    pub fn order(a: Version, b: Version) std.math.Order {
        if (a.major != b.major) return std.math.order(a.major, b.major);
        if (a.minor != b.minor) return std.math.order(a.minor, b.minor);
        if (a.patch != b.patch) return std.math.order(a.patch, b.patch);
        // Pre-release ordering: no pre > has pre
        if (a.pre_release == null and b.pre_release == null) return .eq;
        if (a.pre_release == null) return .gt; // release > pre-release
        if (b.pre_release == null) return .lt;
        // Both have pre-release: lexicographic
        return std.mem.order(u8, a.pre_release.?, b.pre_release.?);
    }

    pub fn eql(a: Version, b: Version) bool {
        return a.order(b) == .eq;
    }

    /// Returns true if a > b
    pub fn greaterThan(a: Version, b: Version) bool {
        return a.order(b) == .gt;
    }

    /// Returns true if a >= b
    pub fn greaterThanOrEqual(a: Version, b: Version) bool {
        const o = a.order(b);
        return o == .gt or o == .eq;
    }

    /// Returns true if a < b
    pub fn lessThan(a: Version, b: Version) bool {
        return a.order(b) == .lt;
    }

    pub fn lessThanOrEqual(a: Version, b: Version) bool {
        const o = a.order(b);
        return o == .lt or o == .eq;
    }
};

// ─── Constraints ────────────────────────────────────────────────────────────

pub const Op = enum {
    eq, // =1.2.3
    gt, // >1.2.3
    gte, // >=1.2.3
    lt, // <1.2.3
    lte, // <=1.2.3
    caret, // ^1.2.3 → >=1.2.3, <2.0.0
    tilde, // ~1.2.3 → >=1.2.3, <1.3.0
};

pub const Constraint = struct {
    op: Op,
    version: Version,

    /// Parse a constraint string: "^1.2.0", "~1.2.0", ">=1.0.0", "=1.2.3", "1.2.3" (implicit =)
    pub fn parse(s: []const u8) !Constraint {
        var rest = s;

        // Skip leading whitespace
        while (rest.len > 0 and rest[0] == ' ') rest = rest[1..];

        const op: Op = blk: {
            if (rest.len >= 2 and rest[0] == '>' and rest[1] == '=') {
                rest = rest[2..];
                break :blk .gte;
            }
            if (rest.len >= 2 and rest[0] == '<' and rest[1] == '=') {
                rest = rest[2..];
                break :blk .lte;
            }
            if (rest.len >= 1 and rest[0] == '>') {
                rest = rest[1..];
                break :blk .gt;
            }
            if (rest.len >= 1 and rest[0] == '<') {
                rest = rest[1..];
                break :blk .lt;
            }
            if (rest.len >= 1 and rest[0] == '=') {
                rest = rest[1..];
                break :blk .eq;
            }
            if (rest.len >= 1 and rest[0] == '^') {
                rest = rest[1..];
                break :blk .caret;
            }
            if (rest.len >= 1 and rest[0] == '~') {
                rest = rest[1..];
                break :blk .tilde;
            }
            break :blk .caret; // default: caret (compatible)
        };

        // Skip whitespace between op and version
        while (rest.len > 0 and rest[0] == ' ') rest = rest[1..];

        const ver = try Version.parse(rest);
        return .{ .op = op, .version = ver };
    }

    /// Check if a version satisfies this constraint.
    pub fn matches(self: Constraint, v: Version) bool {
        return switch (self.op) {
            .eq => v.eql(self.version),
            .gt => v.greaterThan(self.version),
            .gte => v.greaterThanOrEqual(self.version),
            .lt => v.lessThan(self.version),
            .lte => v.lessThanOrEqual(self.version),
            .caret => caretMatch(self.version, v),
            .tilde => tildeMatch(self.version, v),
        };
    }
};

/// Caret: ^1.2.3 → >=1.2.3, <2.0.0
///         ^0.2.3 → >=0.2.3, <0.3.0
///         ^0.0.3 → >=0.0.3, <0.0.4
fn caretMatch(constraint: Version, candidate: Version) bool {
    if (!candidate.greaterThanOrEqual(constraint)) return false;
    if (constraint.major > 0) {
        return candidate.major == constraint.major;
    }
    if (constraint.minor > 0) {
        return candidate.major == 0 and candidate.minor == constraint.minor;
    }
    // ^0.0.x
    return candidate.major == 0 and candidate.minor == 0 and candidate.patch == constraint.patch;
}

/// Tilde: ~1.2.3 → >=1.2.3, <1.3.0
fn tildeMatch(constraint: Version, candidate: Version) bool {
    if (!candidate.greaterThanOrEqual(constraint)) return false;
    return candidate.major == constraint.major and candidate.minor == constraint.minor;
}

/// Parse a compound constraint range: ">=1.0.0, <2.0.0"
/// Returns a list of constraints. All must match (AND).
pub fn parseRange(alloc: std.mem.Allocator, s: []const u8) ![]Constraint {
    var constraints: std.ArrayList(Constraint) = .empty;
    errdefer constraints.deinit(alloc);

    var iter = std.mem.splitScalar(u8, s, ',');
    while (iter.next()) |part| {
        var trimmed = part;
        while (trimmed.len > 0 and trimmed[0] == ' ') trimmed = trimmed[1..];
        while (trimmed.len > 0 and trimmed[trimmed.len - 1] == ' ') trimmed = trimmed[0 .. trimmed.len - 1];
        if (trimmed.len == 0) continue;
        try constraints.append(alloc, try Constraint.parse(trimmed));
    }

    return constraints.toOwnedSlice(alloc);
}

/// Check if version satisfies all constraints in a range (AND).
pub fn satisfiesAll(v: Version, constraints: []const Constraint) bool {
    for (constraints) |c| {
        if (!c.matches(v)) return false;
    }
    return true;
}

// ─── Tests ──────────────────────────────────────────────────────────────────

test "parse version" {
    const v = try Version.parse("1.2.3");
    try std.testing.expectEqual(@as(u32, 1), v.major);
    try std.testing.expectEqual(@as(u32, 2), v.minor);
    try std.testing.expectEqual(@as(u32, 3), v.patch);
    try std.testing.expectEqual(@as(?[]const u8, null), v.pre_release);
}

test "parse version with pre-release" {
    const v = try Version.parse("1.0.0-alpha.1");
    try std.testing.expectEqual(@as(u32, 1), v.major);
    try std.testing.expectEqual(@as(u32, 0), v.minor);
    try std.testing.expectEqual(@as(u32, 0), v.patch);
    try std.testing.expectEqualStrings("alpha.1", v.pre_release.?);
}

test "parse two-part version" {
    const v = try Version.parse("1.2");
    try std.testing.expectEqual(@as(u32, 1), v.major);
    try std.testing.expectEqual(@as(u32, 2), v.minor);
    try std.testing.expectEqual(@as(u32, 0), v.patch);
}

test "version ordering" {
    const v1 = try Version.parse("1.0.0");
    const v2 = try Version.parse("1.0.1");
    const v3 = try Version.parse("1.1.0");
    const v4 = try Version.parse("2.0.0");
    const v5 = try Version.parse("1.0.0-alpha");

    try std.testing.expect(v1.lessThan(v2));
    try std.testing.expect(v2.lessThan(v3));
    try std.testing.expect(v3.lessThan(v4));
    try std.testing.expect(v5.lessThan(v1)); // pre-release < release
    try std.testing.expect(v1.eql(v1));
}

test "format version" {
    const v = try Version.parse("1.2.3");
    var buf: [64]u8 = undefined;
    const s = try v.format(&buf);
    try std.testing.expectEqualStrings("1.2.3", s);
}

test "format version with pre-release" {
    const v = try Version.parse("1.0.0-beta.2");
    var buf: [64]u8 = undefined;
    const s = try v.format(&buf);
    try std.testing.expectEqualStrings("1.0.0-beta.2", s);
}

test "caret constraint" {
    const c = try Constraint.parse("^1.2.3");
    try std.testing.expect(c.matches(try Version.parse("1.2.3")));
    try std.testing.expect(c.matches(try Version.parse("1.2.4")));
    try std.testing.expect(c.matches(try Version.parse("1.9.0")));
    try std.testing.expect(!c.matches(try Version.parse("2.0.0")));
    try std.testing.expect(!c.matches(try Version.parse("1.2.2")));
}

test "caret zero major" {
    const c = try Constraint.parse("^0.2.3");
    try std.testing.expect(c.matches(try Version.parse("0.2.3")));
    try std.testing.expect(c.matches(try Version.parse("0.2.9")));
    try std.testing.expect(!c.matches(try Version.parse("0.3.0")));
    try std.testing.expect(!c.matches(try Version.parse("1.0.0")));
}

test "tilde constraint" {
    const c = try Constraint.parse("~1.2.3");
    try std.testing.expect(c.matches(try Version.parse("1.2.3")));
    try std.testing.expect(c.matches(try Version.parse("1.2.9")));
    try std.testing.expect(!c.matches(try Version.parse("1.3.0")));
    try std.testing.expect(!c.matches(try Version.parse("2.0.0")));
}

test "comparison operators" {
    const gte = try Constraint.parse(">=1.0.0");
    try std.testing.expect(gte.matches(try Version.parse("1.0.0")));
    try std.testing.expect(gte.matches(try Version.parse("2.5.0")));
    try std.testing.expect(!gte.matches(try Version.parse("0.9.9")));

    const lt = try Constraint.parse("<2.0.0");
    try std.testing.expect(lt.matches(try Version.parse("1.9.9")));
    try std.testing.expect(!lt.matches(try Version.parse("2.0.0")));
}

test "compound range" {
    const alloc = std.testing.allocator;
    const constraints = try parseRange(alloc, ">=1.0.0, <2.0.0");
    defer alloc.free(constraints);

    try std.testing.expect(satisfiesAll(try Version.parse("1.5.0"), constraints));
    try std.testing.expect(satisfiesAll(try Version.parse("1.0.0"), constraints));
    try std.testing.expect(!satisfiesAll(try Version.parse("2.0.0"), constraints));
    try std.testing.expect(!satisfiesAll(try Version.parse("0.9.0"), constraints));
}

test "bare version defaults to caret" {
    const c = try Constraint.parse("1.2.3");
    try std.testing.expect(c.matches(try Version.parse("1.2.3")));
    try std.testing.expect(c.matches(try Version.parse("1.9.0")));
    try std.testing.expect(!c.matches(try Version.parse("2.0.0")));
}
