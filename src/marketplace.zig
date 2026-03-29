const std = @import("std");
const branching = @import("branching.zig");
const collection = @import("collection.zig");
const doc_mod = @import("doc.zig");

pub const Release = struct {
    dataset_name: []const u8,
    branch_name: []const u8,
    description: []const u8,
    base_dir: []const u8,
    overlay_dir: []const u8,
    version: u32,
    created_at_ms: i64,

    pub fn deinit(self: *Release, alloc: std.mem.Allocator) void {
        alloc.free(self.dataset_name);
        alloc.free(self.branch_name);
        alloc.free(self.description);
        alloc.free(self.base_dir);
        alloc.free(self.overlay_dir);
    }
};

pub const Marketplace = struct {
    alloc: std.mem.Allocator,
    root_dir: []const u8,

    pub fn init(alloc: std.mem.Allocator, root_dir: []const u8) !Marketplace {
        try ensureDir(root_dir);
        return .{
            .alloc = alloc,
            .root_dir = try alloc.dupe(u8, root_dir),
        };
    }

    pub fn deinit(self: *Marketplace) void {
        self.alloc.free(self.root_dir);
    }

    pub fn publish(self: *Marketplace, snapshot: *const branching.PublishedSnapshot, description: []const u8) !Release {
        const dataset_dir = try std.fmt.allocPrint(self.alloc, "{s}/{s}", .{ self.root_dir, snapshot.dataset_name });
        defer self.alloc.free(dataset_dir);
        try ensureDir(dataset_dir);

        const version = try self.nextVersion(snapshot.dataset_name);
        const release_root = try std.fmt.allocPrint(self.alloc, "{s}/v{d}", .{ dataset_dir, version });
        defer self.alloc.free(release_root);
        const base_dir = try std.fmt.allocPrint(self.alloc, "{s}/base", .{release_root});
        defer self.alloc.free(base_dir);
        const overlay_dir = try std.fmt.allocPrint(self.alloc, "{s}/overlay", .{release_root});
        defer self.alloc.free(overlay_dir);

        try copyDirRecursive(self.alloc, snapshot.parent_path, base_dir);
        try copyDirRecursive(self.alloc, snapshot.overlay_dir, overlay_dir);

        const created_at_ms = std.time.milliTimestamp();
        try self.writeManifest(release_root, snapshot, description, base_dir, overlay_dir, version, created_at_ms);
        return .{
            .dataset_name = try self.alloc.dupe(u8, snapshot.dataset_name),
            .branch_name = try self.alloc.dupe(u8, snapshot.branch_name),
            .description = try self.alloc.dupe(u8, description),
            .base_dir = try self.alloc.dupe(u8, base_dir),
            .overlay_dir = try self.alloc.dupe(u8, overlay_dir),
            .version = version,
            .created_at_ms = created_at_ms,
        };
    }

    pub fn getRelease(self: *Marketplace, dataset_name: []const u8, version: u32) !Release {
        const manifest_path = try std.fmt.allocPrint(self.alloc, "{s}/{s}/v{d}/release.meta", .{ self.root_dir, dataset_name, version });
        defer self.alloc.free(manifest_path);
        const raw = try readFileAbsoluteAlloc(self.alloc, manifest_path, 8192);
        defer self.alloc.free(raw);
        return try parseManifest(self.alloc, raw);
    }

    pub fn listReleases(self: *Marketplace, dataset_name: []const u8, alloc: std.mem.Allocator) ![]u32 {
        const dataset_dir = try std.fmt.allocPrint(self.alloc, "{s}/{s}", .{ self.root_dir, dataset_name });
        defer self.alloc.free(dataset_dir);

        var dir = try std.fs.openDirAbsolute(dataset_dir, .{ .iterate = true });
        defer dir.close();

        var versions: std.ArrayList(u32) = .empty;
        errdefer versions.deinit(alloc);

        var it = dir.iterate();
        while (try it.next()) |entry| {
            if (entry.kind != .directory) continue;
            if (entry.name.len < 2 or entry.name[0] != 'v') continue;
            const version = std.fmt.parseInt(u32, entry.name[1..], 10) catch continue;
            try versions.append(alloc, version);
        }

        std.mem.sort(u32, versions.items, {}, comptime std.sort.asc(u32));
        return try versions.toOwnedSlice(alloc);
    }

    pub fn install(self: *Marketplace, dataset_name: []const u8, version: u32, target_root: []const u8) !branching.BranchedDatabase {
        var release = try self.getRelease(dataset_name, version);
        defer release.deinit(self.alloc);

        try ensureDir(target_root);
        const base_dir = try std.fmt.allocPrint(self.alloc, "{s}/base", .{target_root});
        defer self.alloc.free(base_dir);
        const overlay_dir = try std.fmt.allocPrint(self.alloc, "{s}/overlay", .{target_root});
        defer self.alloc.free(overlay_dir);
        const manifest_path = try std.fmt.allocPrint(self.alloc, "{s}/manifest.txt", .{target_root});
        defer self.alloc.free(manifest_path);

        try copyDirRecursive(self.alloc, release.base_dir, base_dir);
        try copyDirRecursive(self.alloc, release.overlay_dir, overlay_dir);

        var manifest = try std.fs.createFileAbsolute(manifest_path, .{ .truncate = true });
        defer manifest.close();
        try manifest.writeAll(base_dir);

        return branching.BranchedDatabase.open(self.alloc, target_root);
    }

    fn nextVersion(self: *Marketplace, dataset_name: []const u8) !u32 {
        const versions = self.listReleases(dataset_name, self.alloc) catch |err| switch (err) {
            error.FileNotFound => return 1,
            else => return err,
        };
        defer self.alloc.free(versions);
        if (versions.len == 0) return 1;
        return versions[versions.len - 1] + 1;
    }

    fn writeManifest(
        self: *Marketplace,
        release_root: []const u8,
        snapshot: *const branching.PublishedSnapshot,
        description: []const u8,
        base_dir: []const u8,
        overlay_dir: []const u8,
        version: u32,
        created_at_ms: i64,
    ) !void {
        const manifest_path = try std.fmt.allocPrint(self.alloc, "{s}/release.meta", .{release_root});
        defer self.alloc.free(manifest_path);
        const content = try std.fmt.allocPrint(
            self.alloc,
            "dataset={s}\nbranch={s}\ndescription={s}\nversion={d}\nbase_dir={s}\noverlay_dir={s}\ncreated_at_ms={d}\n",
            .{
                snapshot.dataset_name,
                snapshot.branch_name,
                description,
                version,
                base_dir,
                overlay_dir,
                created_at_ms,
            },
        );
        defer self.alloc.free(content);

        var file = try std.fs.createFileAbsolute(manifest_path, .{ .truncate = true });
        defer file.close();
        try file.writeAll(content);
    }
};

fn parseManifest(alloc: std.mem.Allocator, raw: []const u8) !Release {
    var dataset_name: ?[]const u8 = null;
    var branch_name: ?[]const u8 = null;
    var description: ?[]const u8 = null;
    var base_dir: ?[]const u8 = null;
    var overlay_dir: ?[]const u8 = null;
    var version: ?u32 = null;
    var created_at_ms: ?i64 = null;

    var lines = std.mem.tokenizeScalar(u8, raw, '\n');
    while (lines.next()) |line| {
        if (std.mem.indexOfScalar(u8, line, '=')) |sep| {
            const key = line[0..sep];
            const value = line[sep + 1 ..];
            if (std.mem.eql(u8, key, "dataset")) dataset_name = try alloc.dupe(u8, value)
            else if (std.mem.eql(u8, key, "branch")) branch_name = try alloc.dupe(u8, value)
            else if (std.mem.eql(u8, key, "description")) description = try alloc.dupe(u8, value)
            else if (std.mem.eql(u8, key, "base_dir")) base_dir = try alloc.dupe(u8, value)
            else if (std.mem.eql(u8, key, "overlay_dir")) overlay_dir = try alloc.dupe(u8, value)
            else if (std.mem.eql(u8, key, "version")) version = try std.fmt.parseInt(u32, value, 10)
            else if (std.mem.eql(u8, key, "created_at_ms")) created_at_ms = try std.fmt.parseInt(i64, value, 10);
        }
    }

    return .{
        .dataset_name = dataset_name orelse return error.InvalidManifest,
        .branch_name = branch_name orelse return error.InvalidManifest,
        .description = description orelse return error.InvalidManifest,
        .base_dir = base_dir orelse return error.InvalidManifest,
        .overlay_dir = overlay_dir orelse return error.InvalidManifest,
        .version = version orelse return error.InvalidManifest,
        .created_at_ms = created_at_ms orelse return error.InvalidManifest,
    };
}

fn ensureDir(path: []const u8) !void {
    if (std.fs.path.isAbsolute(path)) {
        var root = try std.fs.openDirAbsolute("/", .{});
        defer root.close();
        const rel = std.mem.trimLeft(u8, path, "/");
        if (rel.len > 0) try root.makePath(rel);
    } else {
        try std.fs.cwd().makePath(path);
    }
}

fn copyDirRecursive(alloc: std.mem.Allocator, src_dir: []const u8, dst_dir: []const u8) !void {
    try ensureDir(dst_dir);

    var dir = try std.fs.openDirAbsolute(src_dir, .{ .iterate = true });
    defer dir.close();

    var it = dir.iterate();
    while (try it.next()) |entry| {
        const src_path = try std.fmt.allocPrint(alloc, "{s}/{s}", .{ src_dir, entry.name });
        defer alloc.free(src_path);
        const dst_path = try std.fmt.allocPrint(alloc, "{s}/{s}", .{ dst_dir, entry.name });
        defer alloc.free(dst_path);

        switch (entry.kind) {
            .directory => try copyDirRecursive(alloc, src_path, dst_path),
            .file => {
                if (!std.mem.endsWith(u8, entry.name, ".pages")) continue;
                try copyFileAbsolute(src_path, dst_path);
            },
            else => {},
        }
    }
}

fn copyFileAbsolute(src_path: []const u8, dst_path: []const u8) !void {
    var src = try std.fs.openFileAbsolute(src_path, .{});
    defer src.close();
    var dst = try std.fs.createFileAbsolute(dst_path, .{ .truncate = true });
    defer dst.close();

    const stat = try src.stat();
    _ = try src.copyRangeAll(0, dst, 0, stat.size);
}

fn readFileAbsoluteAlloc(alloc: std.mem.Allocator, path: []const u8, max_bytes: usize) ![]u8 {
    const file = try std.fs.openFileAbsolute(path, .{});
    defer file.close();
    return try file.readToEndAlloc(alloc, max_bytes);
}

test "marketplace publish and install keeps snapshot immutable" {
    const alloc = std.testing.allocator;
    const tmp_dir = "/tmp/turbodb_marketplace_publish";
    const market_dir = "/tmp/turbodb_marketplace_catalog";
    const install_dir = "/tmp/turbodb_marketplace_install";
    std.fs.cwd().deleteTree(tmp_dir) catch {};
    std.fs.cwd().deleteTree(market_dir) catch {};
    std.fs.cwd().deleteTree(install_dir) catch {};
    try ensureDir(tmp_dir);
    try ensureDir(market_dir);
    defer std.fs.cwd().deleteTree(tmp_dir) catch {};
    defer std.fs.cwd().deleteTree(market_dir) catch {};
    defer std.fs.cwd().deleteTree(install_dir) catch {};

    const base = try collection.Database.open(alloc, tmp_dir);
    defer base.close();
    const base_users = try base.collection("users");
    _ = try base_users.insert("u1", "{\"name\":\"base\"}");

    var branch = try branching.BranchedDatabase.fork(alloc, tmp_dir, "feature-market");
    defer branch.close();
    var branch_users = try branch.collection("users");
    _ = try branch_users.update("u1", "{\"name\":\"branch\"}");
    _ = try branch_users.insert("u2", "{\"name\":\"published\"}");

    var snapshot = try branch.publishSnapshot("dataset-users");
    defer snapshot.deinit(alloc);

    var marketplace = try Marketplace.init(alloc, market_dir);
    defer marketplace.deinit();

    var release = try marketplace.publish(&snapshot, "first release");
    defer release.deinit(alloc);
    try std.testing.expectEqual(@as(u32, 1), release.version);

    _ = try branch_users.update("u1", "{\"name\":\"mutated-late\"}");
    _ = try branch_users.insert("u3", "{\"name\":\"not-published\"}");

    var installed = try marketplace.install("dataset-users", 1, install_dir);
    var installed_users = try installed.collection("users");

    try std.testing.expectEqualStrings("{\"name\":\"branch\"}", installed_users.get("u1").?.value);
    try std.testing.expectEqualStrings("{\"name\":\"published\"}", installed_users.get("u2").?.value);
    try std.testing.expectEqual(@as(?doc_mod.Doc, null), installed_users.get("u3"));
    installed.close();
}

test "marketplace keeps multiple release versions installable" {
    const alloc = std.testing.allocator;
    const tmp_dir = "/tmp/turbodb_marketplace_versions";
    const market_dir = "/tmp/turbodb_marketplace_versions_catalog";
    const install_v1_dir = "/tmp/turbodb_marketplace_versions_install_v1";
    const install_v2_dir = "/tmp/turbodb_marketplace_versions_install_v2";
    std.fs.cwd().deleteTree(tmp_dir) catch {};
    std.fs.cwd().deleteTree(market_dir) catch {};
    std.fs.cwd().deleteTree(install_v1_dir) catch {};
    std.fs.cwd().deleteTree(install_v2_dir) catch {};
    try ensureDir(tmp_dir);
    try ensureDir(market_dir);
    defer std.fs.cwd().deleteTree(tmp_dir) catch {};
    defer std.fs.cwd().deleteTree(market_dir) catch {};
    defer std.fs.cwd().deleteTree(install_v1_dir) catch {};
    defer std.fs.cwd().deleteTree(install_v2_dir) catch {};

    const base = try collection.Database.open(alloc, tmp_dir);
    defer base.close();
    const items = try base.collection("items");
    _ = try items.insert("i1", "{\"name\":\"base\"}");

    var branch = try branching.BranchedDatabase.fork(alloc, tmp_dir, "catalog");
    defer branch.close();
    var branch_items = try branch.collection("items");

    var marketplace = try Marketplace.init(alloc, market_dir);
    defer marketplace.deinit();

    _ = try branch_items.update("i1", "{\"name\":\"v1\"}");
    var snap1 = try branch.publishSnapshot("dataset-items");
    defer snap1.deinit(alloc);
    var rel1 = try marketplace.publish(&snap1, "v1");
    defer rel1.deinit(alloc);

    _ = try branch_items.update("i1", "{\"name\":\"v2\"}");
    var snap2 = try branch.publishSnapshot("dataset-items");
    defer snap2.deinit(alloc);
    var rel2 = try marketplace.publish(&snap2, "v2");
    defer rel2.deinit(alloc);

    const versions = try marketplace.listReleases("dataset-items", alloc);
    defer alloc.free(versions);
    try std.testing.expectEqualSlices(u32, &.{ 1, 2 }, versions);

    var installed_v1 = try marketplace.install("dataset-items", rel1.version, install_v1_dir);
    var installed_v2 = try marketplace.install("dataset-items", rel2.version, install_v2_dir);
    const installed_v1_items = try installed_v1.collection("items");
    const installed_v2_items = try installed_v2.collection("items");

    try std.testing.expectEqualStrings("{\"name\":\"v1\"}", installed_v1_items.get("i1").?.value);
    try std.testing.expectEqualStrings("{\"name\":\"v2\"}", installed_v2_items.get("i1").?.value);
    installed_v2.close();
    installed_v1.close();
}

test "marketplace rejects missing releases" {
    const alloc = std.testing.allocator;
    const market_dir = "/tmp/turbodb_marketplace_missing_catalog";
    const install_dir = "/tmp/turbodb_marketplace_missing_install";
    std.fs.cwd().deleteTree(market_dir) catch {};
    std.fs.cwd().deleteTree(install_dir) catch {};
    try ensureDir(market_dir);
    defer std.fs.cwd().deleteTree(market_dir) catch {};
    defer std.fs.cwd().deleteTree(install_dir) catch {};

    var marketplace = try Marketplace.init(alloc, market_dir);
    defer marketplace.deinit();

    try std.testing.expectError(error.FileNotFound, marketplace.install("missing", 1, install_dir));
}
