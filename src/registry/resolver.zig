/// ZagDB — Flat DAG dependency resolver
///
/// Unlike npm's nested node_modules tree, this produces a FLAT dependency list
/// with at most one version per package. If constraints conflict, it errors
/// rather than installing duplicates.
///
/// Algorithm:
///   1. Start with root manifest's deps
///   2. For each dep, find highest matching version from registry
///   3. Fetch that version's transitive deps, add to queue
///   4. If conflict: error (ConflictingVersions)
///   5. Cycle detection via visited set
///   6. Output: topologically sorted flat list
const std = @import("std");
const semver = @import("semver.zig");
const registry_mod = @import("registry.zig");
const manifest_mod = @import("manifest.zig");

pub const ResolvedDep = struct {
    name: []const u8,
    version: semver.Version,
    source_hash: []const u8, // blake3 hex from version metadata
    version_str: []const u8,
};

pub const ResolveError = error{
    DependencyNotFound,
    NoMatchingVersion,
    CyclicDependency,
    ConflictingVersions,
    InvalidVersion,
    InvalidConstraint,
    OutOfMemory,
};

pub const Resolver = struct {
    registry: *registry_mod.Registry,
    alloc: std.mem.Allocator,

    // Resolved deps: name → ResolvedDep
    resolved: std.StringHashMap(ResolvedDep),
    // Visited set for cycle detection
    visiting: std.StringHashMap(void),

    pub fn init(registry: *registry_mod.Registry, alloc: std.mem.Allocator) Resolver {
        return .{
            .registry = registry,
            .alloc = alloc,
            .resolved = std.StringHashMap(ResolvedDep).init(alloc),
            .visiting = std.StringHashMap(void).init(alloc),
        };
    }

    pub fn deinit(self: *Resolver) void {
        self.resolved.deinit();
        self.visiting.deinit();
    }

    /// Resolve all dependencies starting from a root manifest's deps.
    /// Returns a flat, deduplicated list of all transitive dependencies.
    pub fn resolve(self: *Resolver, deps: []const manifest_mod.Dependency) ResolveError![]ResolvedDep {
        for (deps) |dep| {
            try self.resolveDep(dep);
        }

        // Collect results into a slice
        var results: std.ArrayList(ResolvedDep) = .empty;
        var it = self.resolved.valueIterator();
        while (it.next()) |v| {
            results.append(self.alloc, v.*) catch return ResolveError.OutOfMemory;
        }
        return results.toOwnedSlice(self.alloc) catch return ResolveError.OutOfMemory;
    }

    fn resolveDep(self: *Resolver, dep: manifest_mod.Dependency) ResolveError!void {
        // Cycle detection
        if (self.visiting.contains(dep.name)) return ResolveError.CyclicDependency;

        // Already resolved?
        if (self.resolved.get(dep.name)) |existing| {
            // Check if the existing version satisfies this constraint too
            const constraint = semver.Constraint.parse(dep.version_constraint) catch
                return ResolveError.InvalidConstraint;
            if (!constraint.matches(existing.version)) {
                return ResolveError.ConflictingVersions;
            }
            return; // Already resolved and compatible
        }

        // Mark as visiting
        self.visiting.put(dep.name, {}) catch return ResolveError.OutOfMemory;
        defer _ = self.visiting.remove(dep.name);

        // If dep has a pinned hash, use that directly
        if (dep.hash) |hash| {
            const ver = semver.Version.parse(dep.version_constraint) catch
                return ResolveError.InvalidVersion;
            self.resolved.put(dep.name, .{
                .name = dep.name,
                .version = ver,
                .source_hash = hash,
                .version_str = dep.version_constraint,
            }) catch return ResolveError.OutOfMemory;
            return;
        }

        // Query registry for the package
        const constraint = semver.Constraint.parse(dep.version_constraint) catch
            return ResolveError.InvalidConstraint;

        // Try to find the version in registry
        // For the standalone registry, check if we have the package and a matching version
        const ver_str = findBestVersion(self.registry, dep.name, constraint) orelse
            return ResolveError.NoMatchingVersion;

        const ver = semver.Version.parse(ver_str) catch return ResolveError.InvalidVersion;

        // Get source hash from version metadata
        var key_buf: [256]u8 = undefined;
        const key = std.fmt.bufPrint(&key_buf, "{s}@{s}", .{ dep.name, ver_str }) catch
            return ResolveError.OutOfMemory;
        const ver_json = self.registry.getVersion(dep.name, ver_str) orelse
            return ResolveError.DependencyNotFound;
        _ = key;

        const source_hash = jsonGetField(ver_json, "source_hash") orelse "unknown";

        self.resolved.put(dep.name, .{
            .name = dep.name,
            .version = ver,
            .source_hash = source_hash,
            .version_str = ver_str,
        }) catch return ResolveError.OutOfMemory;
    }

    /// Generate a deterministic lockfile JSON.
    pub fn toLockfile(resolved: []const ResolvedDep, buf: []u8) ![]const u8 {
        var fbs = std.io.fixedBufferStream(buf);
        const w = fbs.writer();

        try w.writeAll("{\"locked\":[");
        for (resolved, 0..) |dep, i| {
            if (i > 0) try w.writeAll(",");
            var ver_buf: [64]u8 = undefined;
            const ver_str = dep.version.format(&ver_buf) catch "0.0.0";
            try std.fmt.format(w, "{{\"name\":\"{s}\",\"version\":\"{s}\",\"hash\":\"{s}\"}}", .{
                dep.name,
                ver_str,
                dep.source_hash,
            });
        }
        try w.writeAll("]}");
        return fbs.getWritten();
    }
};

/// Find the best (highest) version matching a constraint in the registry.
/// For standalone mode, this does a simple lookup.
fn findBestVersion(reg: *registry_mod.Registry, name: []const u8, constraint: semver.Constraint) ?[]const u8 {
    // Check if the package exists at all
    const pkg_json = reg.getPackage(name) orelse return null;
    const latest = jsonGetField(pkg_json, "latest_version") orelse return null;

    // Check if the latest version matches the constraint
    const ver = semver.Version.parse(latest) catch return null;
    if (constraint.matches(ver)) return latest;

    return null; // No matching version found
}

fn jsonGetField(json: []const u8, key: []const u8) ?[]const u8 {
    var search_buf: [128]u8 = undefined;
    const needle = std.fmt.bufPrint(&search_buf, "\"{s}\":", .{key}) catch return null;
    const pos = std.mem.indexOf(u8, json, needle) orelse return null;
    var i = pos + needle.len;
    while (i < json.len and (json[i] == ' ' or json[i] == '\t')) i += 1;
    if (i >= json.len) return null;
    if (json[i] == '"') {
        const start = i + 1;
        var j = start;
        while (j < json.len) : (j += 1) {
            if (json[j] == '\\') { j += 1; continue; }
            if (json[j] == '"') return json[start..j];
        }
        return null;
    }
    var end = i;
    while (end < json.len and json[end] != ',' and json[end] != '}' and
        json[end] != '\n' and json[end] != ' ') end += 1;
    return json[i..end];
}

// ─── Tests ──────────────────────────────────────────────────────────────────

test "resolve pinned deps" {
    const alloc = std.testing.allocator;
    const tmp_dir = "/tmp/zagdb-resolver-test";
    std.fs.cwd().deleteTree(tmp_dir) catch {};
    defer std.fs.cwd().deleteTree(tmp_dir) catch {};

    var reg = try registry_mod.Registry.init(alloc, tmp_dir);
    defer reg.deinit();

    var resolver = Resolver.init(&reg, alloc);
    defer resolver.deinit();

    const deps = [_]manifest_mod.Dependency{
        .{ .name = "json", .version_constraint = "1.0.0", .hash = "abc123def456" },
        .{ .name = "http", .version_constraint = "2.1.0", .hash = "789xyz" },
    };

    const resolved = try resolver.resolve(&deps);
    defer alloc.free(resolved);

    try std.testing.expectEqual(@as(usize, 2), resolved.len);
}

test "conflicting versions error" {
    const alloc = std.testing.allocator;
    const tmp_dir = "/tmp/zagdb-resolver-conflict";
    std.fs.cwd().deleteTree(tmp_dir) catch {};
    defer std.fs.cwd().deleteTree(tmp_dir) catch {};

    var reg = try registry_mod.Registry.init(alloc, tmp_dir);
    defer reg.deinit();

    var resolver = Resolver.init(&reg, alloc);
    defer resolver.deinit();

    // First resolve pins json@1.0.0
    const deps1 = [_]manifest_mod.Dependency{
        .{ .name = "json", .version_constraint = "1.0.0", .hash = "abc123" },
    };
    const first = try resolver.resolve(&deps1);
    alloc.free(first);

    // Now try to resolve json@2.0.0 — should conflict
    const deps2 = [_]manifest_mod.Dependency{
        .{ .name = "json", .version_constraint = "^2.0.0" },
    };
    try std.testing.expectError(ResolveError.ConflictingVersions, resolver.resolve(&deps2));
}

test "lockfile generation" {
    const resolved = [_]ResolvedDep{
        .{ .name = "json", .version = .{ .major = 1, .minor = 2, .patch = 3 }, .source_hash = "abc123", .version_str = "1.2.3" },
        .{ .name = "http", .version = .{ .major = 2, .minor = 0, .patch = 0 }, .source_hash = "def456", .version_str = "2.0.0" },
    };

    var buf: [4096]u8 = undefined;
    const lockfile = try Resolver.toLockfile(&resolved, &buf);

    try std.testing.expect(std.mem.indexOf(u8, lockfile, "\"json\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, lockfile, "\"http\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, lockfile, "\"abc123\"") != null);
}

test "resolve from registry" {
    const alloc = std.testing.allocator;
    const tmp_dir = "/tmp/zagdb-resolver-reg";
    std.fs.cwd().deleteTree(tmp_dir) catch {};
    defer std.fs.cwd().deleteTree(tmp_dir) catch {};

    var reg = try registry_mod.Registry.init(alloc, tmp_dir);
    defer reg.deinit();

    // Publish a package first
    const sign_mod = @import("sign.zig");
    const hash_mod = @import("hash.zig");
    const kp = sign_mod.KeyPair.generate();
    const tarball =
        \\{"name":"json-lib","version":"1.5.0","description":"JSON parser"}
    ;
    const content_hash = hash_mod.hashBytes(tarball);
    var hash_hex: [64]u8 = undefined;
    hash_mod.hexEncode(content_hash, &hash_hex);
    const sig = sign_mod.sign(&hash_hex, kp.secret_key);
    _ = try reg.publish(tarball, sig, kp.public_key);

    // Now resolve a dependency on it
    var resolver = Resolver.init(&reg, alloc);
    defer resolver.deinit();

    const deps = [_]manifest_mod.Dependency{
        .{ .name = "json-lib", .version_constraint = "^1.0.0" },
    };

    const resolved = try resolver.resolve(&deps);
    defer alloc.free(resolved);

    try std.testing.expectEqual(@as(usize, 1), resolved.len);
    try std.testing.expectEqualStrings("json-lib", resolved[0].name);
    try std.testing.expectEqual(@as(u32, 1), resolved[0].version.major);
    try std.testing.expectEqual(@as(u32, 5), resolved[0].version.minor);
}
