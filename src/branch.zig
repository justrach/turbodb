//! TurboDB Branch Manager — Git-like branching for agent swarms
//!
//! A branch is a copy-on-write view. Writes are isolated to the branch.
//! Reads check branch-local data first, then fall through to main.
//! Merging applies branch changes to main with conflict detection.

const std = @import("std");
const Allocator = std.mem.Allocator;

pub const BranchStatus = enum(u8) {
    active,
    merged,
    abandoned,
};

pub const Branch = struct {
    name: [64]u8,
    name_len: u8,
    base_epoch: u64, // forked from main at this epoch
    created_at: i64, // timestamp
    status: BranchStatus,
    agent_id: [64]u8, // which agent owns this branch
    agent_id_len: u8,

    // Branch-local storage: key_hash -> value (only modified keys)
    // This is the CoW layer — unmodified keys fall through to main
    writes: std.AutoHashMap(u64, BranchWrite),
    allocator: Allocator,

    pub const BranchWrite = struct {
        key: []const u8, // owned copy
        value: []const u8, // owned copy
        deleted: bool, // true = tombstone (deleted on branch)
        epoch: u64, // when this write happened
    };

    pub fn getName(self: *const Branch) []const u8 {
        return self.name[0..self.name_len];
    }

    pub fn getAgentId(self: *const Branch) []const u8 {
        return self.agent_id[0..self.agent_id_len];
    }

    pub fn deinit(self: *Branch) void {
        var it = self.writes.iterator();
        while (it.next()) |entry| {
            if (entry.value_ptr.key.len > 0) self.allocator.free(entry.value_ptr.key);
            if (entry.value_ptr.value.len > 0) self.allocator.free(entry.value_ptr.value);
        }
        self.writes.deinit();
    }

    /// Write a key-value pair on this branch (CoW — only stores the delta)
    pub fn write(self: *Branch, key: []const u8, value: []const u8, epoch: u64) !void {
        const key_hash = fnv1a(key);
        // Free old write if exists
        if (self.writes.getPtr(key_hash)) |old| {
            if (old.key.len > 0) self.allocator.free(old.key);
            if (old.value.len > 0) self.allocator.free(old.value);
        }
        const owned_key = try self.allocator.dupe(u8, key);
        errdefer self.allocator.free(owned_key);
        const owned_val = try self.allocator.dupe(u8, value);
        try self.writes.put(key_hash, .{
            .key = owned_key,
            .value = owned_val,
            .deleted = false,
            .epoch = epoch,
        });
    }

    /// Mark a key as deleted on this branch
    pub fn delete(self: *Branch, key: []const u8, epoch: u64) !void {
        const key_hash = fnv1a(key);
        if (self.writes.getPtr(key_hash)) |old| {
            if (old.key.len > 0) self.allocator.free(old.key);
            if (old.value.len > 0) self.allocator.free(old.value);
        }
        const owned_key = try self.allocator.dupe(u8, key);
        try self.writes.put(key_hash, .{
            .key = owned_key,
            .value = &.{},
            .deleted = true,
            .epoch = epoch,
        });
    }

    /// Read a key on this branch. Returns branch-local value or null (fall through to main).
    pub fn read(self: *const Branch, key: []const u8) ?BranchRead {
        const key_hash = fnv1a(key);
        if (self.writes.get(key_hash)) |w| {
            if (w.deleted) return .{ .deleted = true, .value = null };
            return .{ .deleted = false, .value = w.value };
        }
        return null; // not on branch — caller should fall through to main
    }

    pub const BranchRead = struct {
        value: ?[]const u8,
        deleted: bool,
    };

    /// Get all modified keys on this branch (for diff)
    pub fn modifiedKeys(self: *const Branch, alloc: Allocator) ![]DiffEntry {
        var entries: std.ArrayList(DiffEntry) = .empty;
        var it = self.writes.iterator();
        while (it.next()) |entry| {
            const w = entry.value_ptr.*;
            try entries.append(alloc, .{
                .key = w.key,
                .value = w.value,
                .deleted = w.deleted,
                .epoch = w.epoch,
            });
        }
        return entries.toOwnedSlice(alloc);
    }

    /// Resolve a conflict by providing the final value for a key.
    /// Just overwrites the branch's value — same as write().
    pub fn resolveConflict(self: *Branch, key: []const u8, resolved_value: []const u8, epoch: u64) !void {
        try self.write(key, resolved_value, epoch);
    }
};

pub const DiffEntry = struct {
    key: []const u8,
    value: []const u8,
    deleted: bool,
    epoch: u64,
};

pub const MergeConflict = struct {
    key: []const u8,
    branch_value: []const u8,
    main_value: []const u8,
};

pub const MergeResult = struct {
    applied: u32,
    conflicts: []MergeConflict,
    alloc: Allocator,

    pub fn deinit(self: *MergeResult) void {
        self.alloc.free(self.conflicts);
    }
};

/// Branch manager — tracks all branches for a collection
pub const BranchManager = struct {
    branches: std.StringHashMap(*Branch),
    allocator: Allocator,
    next_epoch: *std.atomic.Value(u64), // shared with collection's epoch counter

    pub fn init(allocator: Allocator, epoch_counter: *std.atomic.Value(u64)) BranchManager {
        return .{
            .branches = std.StringHashMap(*Branch).init(allocator),
            .allocator = allocator,
            .next_epoch = epoch_counter,
        };
    }

    pub fn deinit(self: *BranchManager) void {
        var it = self.branches.iterator();
        while (it.next()) |entry| {
            entry.value_ptr.*.deinit();
            self.allocator.destroy(entry.value_ptr.*);
            self.allocator.free(@constCast(entry.key_ptr.*));
        }
        self.branches.deinit();
    }

    /// Create a new branch forked from the current epoch
    pub fn createBranch(self: *BranchManager, name_arg: []const u8, agent_id: []const u8) !*Branch {
        if (self.branches.contains(name_arg)) return error.BranchExists;

        const branch = try self.allocator.create(Branch);
        const name_len: u8 = @intCast(@min(name_arg.len, 64));
        const aid_len: u8 = @intCast(@min(agent_id.len, 64));
        branch.* = Branch{
            .name = undefined,
            .name_len = name_len,
            .base_epoch = self.next_epoch.load(.acquire),
            .created_at = std.time.milliTimestamp(),
            .status = .active,
            .agent_id = undefined,
            .agent_id_len = aid_len,
            .writes = std.AutoHashMap(u64, Branch.BranchWrite).init(self.allocator),
            .allocator = self.allocator,
        };
        @memcpy(branch.name[0..name_len], name_arg[0..name_len]);
        @memcpy(branch.agent_id[0..aid_len], agent_id[0..aid_len]);

        // Store branch — need owned key for the hashmap
        const owned_name = try self.allocator.dupe(u8, name_arg);
        try self.branches.put(owned_name, branch);
        return branch;
    }

    pub fn getBranch(self: *const BranchManager, name_arg: []const u8) ?*Branch {
        return self.branches.get(name_arg);
    }

    pub fn deleteBranch(self: *BranchManager, name_arg: []const u8) void {
        if (self.branches.fetchRemove(name_arg)) |removed| {
            removed.value.deinit();
            self.allocator.destroy(removed.value);
            self.allocator.free(@constCast(removed.key));
        }
    }

    pub fn listBranches(self: *const BranchManager, alloc: Allocator) ![][]const u8 {
        var names: std.ArrayList([]const u8) = .empty;
        var it = self.branches.keyIterator();
        while (it.next()) |key| {
            try names.append(alloc, key.*);
        }
        return names.toOwnedSlice(alloc);
    }
};

// ─── FNV-1a hash (same as doc.zig) ──────────────────────────────────────

fn fnv1a(data: []const u8) u64 {
    var h: u64 = 0xcbf29ce484222325;
    for (data) |byte| {
        h ^= byte;
        h *%= 0x100000001b3;
    }
    return h;
}

// ─── Tests ──────────────────────────────────────────────────────────────

test "branch write and read" {
    const alloc = std.testing.allocator;
    var epoch = std.atomic.Value(u64).init(100);
    var mgr = BranchManager.init(alloc, &epoch);
    defer mgr.deinit();

    const br = try mgr.createBranch("test-branch", "agent-1");

    // Write on branch
    try br.write("src/main.zig", "const x = 42;", 101);

    // Read from branch
    const r = br.read("src/main.zig");
    try std.testing.expect(r != null);
    try std.testing.expect(!r.?.deleted);
    try std.testing.expectEqualStrings("const x = 42;", r.?.value.?);

    // Read non-existent key falls through
    const r2 = br.read("src/other.zig");
    try std.testing.expect(r2 == null);
}

test "branch delete marks tombstone" {
    const alloc = std.testing.allocator;
    var epoch = std.atomic.Value(u64).init(100);
    var mgr = BranchManager.init(alloc, &epoch);
    defer mgr.deinit();

    const br = try mgr.createBranch("del-test", "agent-2");
    try br.write("file.txt", "hello", 101);
    try br.delete("file.txt", 102);

    const r = br.read("file.txt");
    try std.testing.expect(r != null);
    try std.testing.expect(r.?.deleted);
}

test "branch manager create and list" {
    const alloc = std.testing.allocator;
    var epoch = std.atomic.Value(u64).init(50);
    var mgr = BranchManager.init(alloc, &epoch);
    defer mgr.deinit();

    _ = try mgr.createBranch("feat-auth", "agent-a");
    _ = try mgr.createBranch("fix-bug", "agent-b");

    const names = try mgr.listBranches(alloc);
    defer alloc.free(names);
    try std.testing.expectEqual(@as(usize, 2), names.len);

    // Duplicate branch should fail
    const err = mgr.createBranch("feat-auth", "agent-c");
    try std.testing.expect(err == error.BranchExists);
}

test "branch modified keys for diff" {
    const alloc = std.testing.allocator;
    var epoch = std.atomic.Value(u64).init(200);
    var mgr = BranchManager.init(alloc, &epoch);
    defer mgr.deinit();

    const br = try mgr.createBranch("diff-test", "agent-x");
    try br.write("a.zig", "aaa", 201);
    try br.write("b.zig", "bbb", 202);
    try br.delete("c.zig", 203);

    const diff = try br.modifiedKeys(alloc);
    defer alloc.free(diff);
    try std.testing.expectEqual(@as(usize, 3), diff.len);
}
