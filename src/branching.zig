const std = @import("std");
const collection = @import("collection.zig");
const cdc_mod = @import("cdc.zig");
const doc_mod = @import("doc.zig");
const compat = @import("compat");

const Database = collection.Database;
const Doc = doc_mod.Doc;

pub const BranchManifest = struct {
    name: []const u8,
    parent_path: []const u8,
};

pub const BranchedCollection = struct {
    overlay: *collection.Collection,
    base: ?*collection.Collection,
    alloc: std.mem.Allocator,

    pub fn insert(self: *BranchedCollection, key: []const u8, value: []const u8) !u64 {
        return self.overlay.insert(key, value);
    }

    pub fn update(self: *BranchedCollection, key: []const u8, value: []const u8) !bool {
        const updated = try self.overlay.update(key, value);
        if (updated) return true;
        if (self.base) |base| {
            if (base.get(key) != null) {
                _ = try self.overlay.insert(key, value);
                return true;
            }
        }
        return false;
    }

    pub fn delete(self: *BranchedCollection, key: []const u8) !bool {
        const deleted = try self.overlay.delete(key);
        if (deleted) return true;
        if (self.base) |base| {
            if (base.get(key) != null) {
                _ = try self.overlay.insert(key, "");
                const entry = self.overlay.get(key) orelse return false;
                _ = entry;
                return try self.overlay.delete(key);
            }
        }
        return false;
    }

    pub fn get(self: *const BranchedCollection, key: []const u8) ?Doc {
        if (self.overlay.get(key)) |doc| return doc;
        if (self.base) |base| return base.get(key);
        return null;
    }

    pub fn scan(self: *const BranchedCollection, limit: u32, offset: u32, alloc: std.mem.Allocator) !collection.Collection.ScanResult {
        var result: std.ArrayList(Doc) = .empty;
        errdefer result.deinit(alloc);
        var seen = std.StringHashMap(void).init(alloc);
        defer {
            var it = seen.keyIterator();
            while (it.next()) |k| alloc.free(k.*);
            seen.deinit();
        }

        const overlay_scan = try self.overlay.scan(limit + offset + 1024, 0, alloc);
        defer overlay_scan.deinit();
        for (overlay_scan.docs) |doc| {
            const key = try alloc.dupe(u8, doc.key);
            try seen.put(key, {});
            if (result.items.len >= offset and result.items.len - offset >= limit) break;
            try result.append(alloc, doc);
        }

        if (self.base) |base| {
            const base_scan = try base.scan(limit + offset + 1024, 0, alloc);
            defer base_scan.deinit();
            for (base_scan.docs) |doc| {
                if (seen.contains(doc.key)) continue;
                if (result.items.len >= offset and result.items.len - offset >= limit) break;
                try result.append(alloc, doc);
            }
        }

        if (offset > 0 and offset < result.items.len) {
            const trimmed = try alloc.alloc(Doc, @min(limit, result.items.len - offset));
            @memcpy(trimmed, result.items[offset .. offset + trimmed.len]);
            result.deinit(alloc);
            return .{ .docs = trimmed, .alloc = alloc };
        }

        if (result.items.len > limit) {
            result.shrinkRetainingCapacity(limit);
        }

        return .{ .docs = try result.toOwnedSlice(alloc), .alloc = alloc };
    }
};

pub const BranchedDatabase = struct {
    alloc: std.mem.Allocator,
    branch_name: []const u8,
    branch_root: []const u8,
    overlay_dir: []const u8,
    parent_path: []const u8,
    overlay: *Database,
    parent: *Database,
    cdc: *cdc_mod.CDCManager,

    pub fn fork(alloc: std.mem.Allocator, base_data_dir: []const u8, branch_name: []const u8) !BranchedDatabase {
        const branch_root = try std.fmt.allocPrint(alloc, "{s}/branches/{s}", .{ base_data_dir, branch_name });
        errdefer alloc.free(branch_root);
        const overlay_dir = try std.fmt.allocPrint(alloc, "{s}/overlay", .{branch_root});
        errdefer alloc.free(overlay_dir);
        const manifest_path = try std.fmt.allocPrint(alloc, "{s}/manifest.txt", .{branch_root});
        defer alloc.free(manifest_path);

        const branches_dir = try std.fmt.allocPrint(alloc, "{s}/branches", .{base_data_dir});
        defer alloc.free(branches_dir);
        compat.fs.cwdMakePath(branches_dir) catch |e| switch (e) {
            error.PathAlreadyExists => {},
            else => return e,
        };
        compat.fs.cwdMakePath(branch_root) catch |e| switch (e) {
            error.PathAlreadyExists => {},
            else => return e,
        };
        compat.fs.cwdMakePath(overlay_dir) catch |e| switch (e) {
            error.PathAlreadyExists => {},
            else => return e,
        };

        var file = try compat.fs.createFileAbsolute(manifest_path, .{ .truncate = true });
        defer file.close();
        try file.writeAll(base_data_dir);

        const opened = try open(alloc, branch_root);
        alloc.free(branch_root);
        alloc.free(overlay_dir);
        return opened;
    }

    pub fn open(alloc: std.mem.Allocator, branch_root: []const u8) !BranchedDatabase {
        const manifest_path = try std.fmt.allocPrint(alloc, "{s}/manifest.txt", .{branch_root});
        defer alloc.free(manifest_path);
        const parent_path = try compat.fs.cwdReadFileAlloc(alloc, manifest_path, 4096);
        errdefer alloc.free(parent_path);
        const overlay_dir = try std.fmt.allocPrint(alloc, "{s}/overlay", .{branch_root});
        errdefer alloc.free(overlay_dir);
        const branch_name = branchNameFromRoot(branch_root);

        const parent = try Database.open(alloc, std.mem.trim(u8, parent_path, "\r\n\t "));
        errdefer parent.close();
        const overlay = try Database.open(alloc, overlay_dir);
        errdefer overlay.close();

        return .{
            .alloc = alloc,
            .branch_name = try alloc.dupe(u8, branch_name),
            .branch_root = try alloc.dupe(u8, branch_root),
            .overlay_dir = overlay_dir,
            .parent_path = parent_path,
            .overlay = overlay,
            .parent = parent,
            .cdc = &overlay.cdc,
        };
    }

    pub fn close(self: *BranchedDatabase) void {
        self.overlay.close();
        self.parent.close();
        self.alloc.free(self.branch_name);
        self.alloc.free(self.branch_root);
        self.alloc.free(self.overlay_dir);
        self.alloc.free(self.parent_path);
    }

    pub fn collection(self: *BranchedDatabase, name: []const u8) !BranchedCollection {
        const overlay_col = try self.overlay.collection(name);
        const base_col = self.parent.collection(name) catch null;
        return .{ .overlay = overlay_col, .base = base_col, .alloc = self.alloc };
    }

    pub fn publishSnapshot(self: *BranchedDatabase, dataset_name: []const u8) !PublishedSnapshot {
        try self.parent.checkpoint();
        try self.overlay.checkpoint();
        const snapshot_root = try std.fmt.allocPrint(self.alloc, "{s}/snapshots/{s}", .{ self.branch_root, dataset_name });
        errdefer self.alloc.free(snapshot_root);
        const snapshot_overlay = try std.fmt.allocPrint(self.alloc, "{s}/overlay", .{snapshot_root});
        errdefer self.alloc.free(snapshot_overlay);

        try compat.fs.cwdMakePath(snapshot_overlay);
        try copyDirFilesAbsolute(self.overlay_dir, snapshot_overlay);

        return .{
            .dataset_name = try self.alloc.dupe(u8, dataset_name),
            .branch_name = try self.alloc.dupe(u8, self.branch_name),
            .snapshot_root = snapshot_root,
            .overlay_dir = snapshot_overlay,
            .parent_path = try self.alloc.dupe(u8, self.parent_path),
        };
    }
};

pub const PublishedSnapshot = struct {
    dataset_name: []const u8,
    branch_name: []const u8,
    snapshot_root: []const u8,
    overlay_dir: []const u8,
    parent_path: []const u8,

    pub fn deinit(self: *PublishedSnapshot, alloc: std.mem.Allocator) void {
        alloc.free(self.dataset_name);
        alloc.free(self.branch_name);
        alloc.free(self.snapshot_root);
        alloc.free(self.overlay_dir);
        alloc.free(self.parent_path);
    }

    pub fn materializeReplica(self: *const PublishedSnapshot, alloc: std.mem.Allocator, replica_root: []const u8) !BranchedDatabase {
        const replica_overlay = try std.fmt.allocPrint(alloc, "{s}/overlay", .{replica_root});
        defer alloc.free(replica_overlay);
        const manifest_path = try std.fmt.allocPrint(alloc, "{s}/manifest.txt", .{replica_root});
        defer alloc.free(manifest_path);

        try compat.fs.cwdMakePath(replica_overlay);
        try writeFileAbsolute(manifest_path, self.parent_path);
        try copyDirFilesAbsolute(self.overlay_dir, replica_overlay);

        return try BranchedDatabase.open(alloc, replica_root);
    }
};

fn branchNameFromRoot(branch_root: []const u8) []const u8 {
    const idx = std.mem.lastIndexOfScalar(u8, branch_root, '/') orelse return branch_root;
    return branch_root[idx + 1 ..];
}

fn copyDirFilesAbsolute(src_dir: []const u8, dest_dir: []const u8) !void {
    var src = try std.fs.openDirAbsolute(src_dir, .{ .iterate = true });
    defer src.close();

    var it = src.iterate();
    while (try it.next()) |entry| {
        if (entry.kind != .file) continue;
        if (!std.mem.endsWith(u8, entry.name, ".pages")) continue;

        const src_path = try std.fmt.allocPrint(std.heap.page_allocator, "{s}/{s}", .{ src_dir, entry.name });
        defer std.heap.page_allocator.free(src_path);
        const dest_path = try std.fmt.allocPrint(std.heap.page_allocator, "{s}/{s}", .{ dest_dir, entry.name });
        defer std.heap.page_allocator.free(dest_path);
        try copyFileAbsolute(src_path, dest_path);
    }
}

fn copyFileAbsolute(src_path: []const u8, dest_path: []const u8) !void {
    var src_file = try std.fs.openFileAbsolute(src_path, .{});
    defer src_file.close();
    var dst_file = try std.fs.createFileAbsolute(dest_path, .{ .truncate = true });
    defer dst_file.close();

    const stat = try src_file.stat();
    _ = try src_file.copyRangeAll(0, dst_file, 0, stat.size);
}

fn writeFileAbsolute(path: []const u8, content: []const u8) !void {
    var file = try std.fs.createFileAbsolute(path, .{ .truncate = true });
    defer file.close();
    try file.writeAll(content);
}

test "branched database reads from base and writes to overlay" {
    const alloc = std.testing.allocator;
    const tmp_dir = "/tmp/turbodb_branching_test";
    compat.fs.cwdDeleteTree(tmp_dir) catch {};
    try compat.fs.cwdMakePath(tmp_dir);
    defer compat.fs.cwdDeleteTree(tmp_dir) catch {};

    const base = try Database.open(alloc, tmp_dir);
    defer base.close();
    const users = try base.collection("users");
    _ = try users.insert("u1", "{\"name\":\"base\"}");

    var branch = try BranchedDatabase.fork(alloc, tmp_dir, "feature-x");
    defer branch.close();
    var branch_users = try branch.collection("users");

    try std.testing.expectEqualStrings("{\"name\":\"base\"}", branch_users.get("u1").?.value);
    _ = try branch_users.update("u1", "{\"name\":\"branch\"}");
    _ = try branch_users.insert("u2", "{\"name\":\"overlay\"}");

    try std.testing.expectEqualStrings("{\"name\":\"branch\"}", branch_users.get("u1").?.value);
    try std.testing.expectEqualStrings("{\"name\":\"base\"}", users.get("u1").?.value);
    try std.testing.expectEqual(@as(?Doc, null), users.get("u2"));
}

test "published snapshot retains branch source metadata" {
    const alloc = std.testing.allocator;
    const tmp_dir = "/tmp/turbodb_branch_publish";
    compat.fs.cwdDeleteTree(tmp_dir) catch {};
    try compat.fs.cwdMakePath(tmp_dir);
    defer compat.fs.cwdDeleteTree(tmp_dir) catch {};

    const base = try Database.open(alloc, tmp_dir);
    defer base.close();

    var branch = try BranchedDatabase.fork(alloc, tmp_dir, "market-ready");
    defer branch.close();

    var snapshot = try branch.publishSnapshot("dataset-1");
    defer snapshot.deinit(alloc);
    try std.testing.expectEqualStrings("dataset-1", snapshot.dataset_name);
    try std.testing.expectEqualStrings("market-ready", snapshot.branch_name);
    try std.testing.expect(std.mem.endsWith(u8, snapshot.snapshot_root, "/snapshots/dataset-1"));
}

test "published snapshot materializes embedded replica state" {
    const alloc = std.testing.allocator;
    const tmp_dir = "/tmp/turbodb_branch_replica";
    const replica_root = "/tmp/turbodb_branch_replica_embedded";
    compat.fs.cwdDeleteTree(tmp_dir) catch {};
    compat.fs.cwdDeleteTree(replica_root) catch {};
    try compat.fs.cwdMakePath(tmp_dir);
    defer compat.fs.cwdDeleteTree(tmp_dir) catch {};
    defer compat.fs.cwdDeleteTree(replica_root) catch {};

    const base = try Database.open(alloc, tmp_dir);
    defer base.close();
    const users = try base.collection("users");
    _ = try users.insert("u1", "{\"name\":\"base\"}");

    var branch = try BranchedDatabase.fork(alloc, tmp_dir, "edge-sg");
    defer branch.close();
    var branch_users = try branch.collection("users");
    _ = try branch_users.update("u1", "{\"name\":\"replicated\"}");
    _ = try branch_users.insert("u2", "{\"name\":\"overlay\"}");

    var snapshot = try branch.publishSnapshot("dataset-edge");
    defer snapshot.deinit(alloc);

    var replica = try snapshot.materializeReplica(alloc, replica_root);
    defer replica.close();
    var replica_users = try replica.collection("users");

    try std.testing.expectEqualStrings("{\"name\":\"replicated\"}", replica_users.get("u1").?.value);
    try std.testing.expectEqualStrings("{\"name\":\"overlay\"}", replica_users.get("u2").?.value);
}
