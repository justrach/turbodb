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

    // Branch-local storage: actual key string -> value (only modified keys)
    // This is the CoW layer — unmodified keys fall through to main
    writes: std.StringHashMap(BranchWrite),
    allocator: Allocator,

    pub const BranchWrite = struct {
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
            if (entry.value_ptr.value.len > 0) self.allocator.free(entry.value_ptr.value);
            self.allocator.free(@constCast(entry.key_ptr.*));
        }
        self.writes.deinit();
    }

    /// Write a key-value pair on this branch (CoW — only stores the delta)
    pub fn write(self: *Branch, key: []const u8, value: []const u8, epoch: u64) !void {
        // Allocate new copies BEFORE freeing old ones — if alloc fails,
        // the existing entry stays valid.
        const owned_key = try self.allocator.dupe(u8, key);
        errdefer self.allocator.free(owned_key);
        const owned_val = try self.allocator.dupe(u8, value);
        errdefer self.allocator.free(owned_val);
        // Free old write if exists (safe — new copies already allocated)
        if (self.writes.getPtr(key)) |old| {
            if (old.value.len > 0) self.allocator.free(old.value);
            // Free the old key that was used as the map key.
            const old_map_key = self.writes.getKey(key).?;
            self.allocator.free(@constCast(old_map_key));
            // Remove old entry so we can insert with new owned key.
            _ = self.writes.remove(key);
        }
        try self.writes.put(owned_key, .{
            .value = owned_val,
            .deleted = false,
            .epoch = epoch,
        });
    }

    /// Mark a key as deleted on this branch
    pub fn delete(self: *Branch, key: []const u8, epoch: u64) !void {
        if (self.writes.getPtr(key)) |old| {
            if (old.value.len > 0) self.allocator.free(old.value);
            const old_map_key = self.writes.getKey(key).?;
            self.allocator.free(@constCast(old_map_key));
            _ = self.writes.remove(key);
        }
        const owned_key = try self.allocator.dupe(u8, key);
        try self.writes.put(owned_key, .{
            .value = &.{},
            .deleted = true,
            .epoch = epoch,
        });
    }

    /// Read a key on this branch. Returns branch-local value or null (fall through to main).
    pub fn read(self: *const Branch, key: []const u8) ?BranchRead {
        if (self.writes.get(key)) |w| {
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
                .key = entry.key_ptr.*,
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

/// Result of comparing multiple branches on the same key.
pub const BranchFileView = struct {
    branch_name: []const u8,
    agent_id: []const u8,
    value: ?[]const u8, // null = not modified on this branch
    deleted: bool,
    epoch: u64,
};

/// A single file's view across all branches.
pub const CompareEntry = struct {
    key: []const u8,
    main_value: ?[]const u8, // current value on main (null if doesn't exist)
    branch_views: []BranchFileView, // one per branch that modified this key
};

/// Compare result: all files touched by any branch, with per-branch views.
pub const CompareResult = struct {
    entries: []CompareEntry,
    branch_names: [][]const u8,
    alloc: Allocator,

    pub fn deinit(self: *CompareResult) void {
        for (self.entries) |e| {
            self.alloc.free(e.branch_views);
        }
        self.alloc.free(self.entries);
        self.alloc.free(self.branch_names);
    }
};

/// Compare multiple branches side-by-side.
/// Returns every file touched by ANY branch, with each branch's version.
pub fn compareBranches(branches: []*const Branch, alloc: Allocator) !CompareResult {
    // Collect all unique keys across all branches
    var all_keys = std.StringHashMap(void).init(alloc);
    defer all_keys.deinit();

    for (branches) |br| {
        var it = br.writes.iterator();
        while (it.next()) |entry| {
            try all_keys.put(entry.key_ptr.*, {});
        }
    }

    // Build entries
    var entries: std.ArrayList(CompareEntry) = .empty;
    var key_iter = all_keys.keyIterator();
    while (key_iter.next()) |key_ptr| {
        const key = key_ptr.*;
        var views: std.ArrayList(BranchFileView) = .empty;

        for (branches) |br| {
            if (br.read(key)) |r| {
                try views.append(alloc, .{
                    .branch_name = br.getName(),
                    .agent_id = br.getAgentId(),
                    .value = r.value,
                    .deleted = r.deleted,
                    .epoch = if (br.writes.get(key)) |w| w.epoch else 0,
                });
            }
        }

        try entries.append(alloc, .{
            .key = key,
            .main_value = null, // caller fills this from Collection.get()
            .branch_views = try views.toOwnedSlice(alloc),
        });
    }

    // Collect branch names
    var names: std.ArrayList([]const u8) = .empty;
    for (branches) |br| try names.append(alloc, br.getName());

    return .{
        .entries = try entries.toOwnedSlice(alloc),
        .branch_names = try names.toOwnedSlice(alloc),
        .alloc = alloc,
    };
}

pub const DiffEntry = struct {
    key: []const u8,
    value: []const u8,
    deleted: bool,
    epoch: u64,
};

/// A single line change in a diff.
pub const LineDiff = struct {
    line_no: u32, // 1-indexed line number in the original (for removals) or new (for additions)
    kind: enum(u8) { same, added, removed },
    text: []const u8, // the line content (not owned — points into input slices)
};

/// Compute a line-by-line diff between two strings.
/// Returns an array of LineDiff entries. Simple O(n+m) algorithm:
/// splits both into lines, walks forward matching equal lines,
/// marks unmatched lines as removed/added.
pub fn lineDiff(old: []const u8, new: []const u8, alloc: Allocator) ![]LineDiff {
    var result: std.ArrayList(LineDiff) = .empty;

    // Split into lines
    const old_lines = try splitLines(old, alloc);
    defer alloc.free(old_lines);
    const new_lines = try splitLines(new, alloc);
    defer alloc.free(new_lines);

    // Simple LCS-free diff: walk both line arrays with greedy matching
    var oi: usize = 0;
    var ni: usize = 0;

    while (oi < old_lines.len and ni < new_lines.len) {
        if (std.mem.eql(u8, old_lines[oi], new_lines[ni])) {
            // Lines match — context
            try result.append(alloc, .{
                .line_no = @intCast(oi + 1),
                .kind = .same,
                .text = old_lines[oi],
            });
            oi += 1;
            ni += 1;
        } else {
            // Try to find the new line ahead in old (deletion then continue)
            // or old line ahead in new (addition then continue)
            // Simple heuristic: look ahead up to 5 lines in each direction
            var found_in_new: ?usize = null;
            var found_in_old: ?usize = null;

            for (0..@min(5, new_lines.len - ni)) |look| {
                if (std.mem.eql(u8, old_lines[oi], new_lines[ni + look])) {
                    found_in_new = look;
                    break;
                }
            }
            for (0..@min(5, old_lines.len - oi)) |look| {
                if (std.mem.eql(u8, old_lines[oi + look], new_lines[ni])) {
                    found_in_old = look;
                    break;
                }
            }

            if (found_in_new) |skip| {
                // Lines were added in new
                for (0..skip) |j| {
                    try result.append(alloc, .{
                        .line_no = @intCast(ni + j + 1),
                        .kind = .added,
                        .text = new_lines[ni + j],
                    });
                }
                ni += skip;
            } else if (found_in_old) |skip| {
                // Lines were removed from old
                for (0..skip) |j| {
                    try result.append(alloc, .{
                        .line_no = @intCast(oi + j + 1),
                        .kind = .removed,
                        .text = old_lines[oi + j],
                    });
                }
                oi += skip;
            } else {
                // No match found — treat as remove old + add new
                try result.append(alloc, .{
                    .line_no = @intCast(oi + 1),
                    .kind = .removed,
                    .text = old_lines[oi],
                });
                try result.append(alloc, .{
                    .line_no = @intCast(ni + 1),
                    .kind = .added,
                    .text = new_lines[ni],
                });
                oi += 1;
                ni += 1;
            }
        }
    }

    // Remaining old lines = removed
    while (oi < old_lines.len) : (oi += 1) {
        try result.append(alloc, .{
            .line_no = @intCast(oi + 1),
            .kind = .removed,
            .text = old_lines[oi],
        });
    }

    // Remaining new lines = added
    while (ni < new_lines.len) : (ni += 1) {
        try result.append(alloc, .{
            .line_no = @intCast(ni + 1),
            .kind = .added,
            .text = new_lines[ni],
        });
    }

    return result.toOwnedSlice(alloc);
}

/// Split a string into lines (by \n). Returns slice of slices into the original string.
fn splitLines(text: []const u8, alloc: Allocator) ![][]const u8 {
    var lines: std.ArrayList([]const u8) = .empty;
    var start: usize = 0;
    for (text, 0..) |c, i| {
        if (c == '\n') {
            try lines.append(alloc, text[start..i]);
            start = i + 1;
        }
    }
    if (start <= text.len) {
        try lines.append(alloc, text[start..]);
    }
    return lines.toOwnedSlice(alloc);
}

test "lineDiff detects added and removed lines" {
    const alloc = std.testing.allocator;
    const old = "line1\nline2\nline3\n";
    const new = "line1\nline2_modified\nline3\nline4\n";

    const diff = try lineDiff(old, new, alloc);
    defer alloc.free(diff);

    // Should have: same(line1), removed(line2), added(line2_modified), same(line3), added(line4)
    var added: u32 = 0;
    var removed: u32 = 0;
    var same: u32 = 0;
    for (diff) |d| {
        switch (d.kind) {
            .added => added += 1,
            .removed => removed += 1,
            .same => same += 1,
        }
    }
    try std.testing.expect(added >= 1);
    try std.testing.expect(removed >= 1);
    try std.testing.expect(same >= 1);
}

test "lineDiff identical files" {
    const alloc = std.testing.allocator;
    const text = "aaa\nbbb\nccc\n";
    const diff = try lineDiff(text, text, alloc);
    defer alloc.free(diff);

    for (diff) |d| {
        try std.testing.expectEqual(d.kind, .same);
    }
}

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
            .writes = std.StringHashMap(Branch.BranchWrite).init(self.allocator),
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
