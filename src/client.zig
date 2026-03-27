const std = @import("std");
const collection_mod = @import("collection.zig");
const doc_mod = @import("doc.zig");

pub const Database = collection_mod.Database;
pub const Collection = collection_mod.Collection;
pub const Doc = doc_mod.Doc;
pub const DocHeader = doc_mod.DocHeader;

/// Embedded TurboDB client — direct access, zero network overhead.
pub const Db = struct {
    inner: *Database,
    alloc: std.mem.Allocator,

    /// Open a TurboDB database at the given directory.
    pub fn open(alloc: std.mem.Allocator, data_dir: []const u8) !Db {
        // Ensure directory exists
        std.fs.cwd().makeDir(data_dir) catch |e| switch (e) {
            error.PathAlreadyExists => {},
            else => return e,
        };
        const db = try Database.open(alloc, data_dir);
        return Db{ .inner = db, .alloc = alloc };
    }

    pub fn close(self: *Db) void {
        self.inner.close();
    }

    /// Get or create a named collection.
    pub fn collection(self: *Db, name: []const u8) !*Collection {
        return self.inner.collection(name);
    }

    /// Shorthand: insert into a collection.
    pub fn insert(self: *Db, col_name: []const u8, key: []const u8, value: []const u8) !u64 {
        const col = try self.inner.collection(col_name);
        return col.insert(key, value);
    }

    /// Shorthand: get from a collection.
    pub fn get(self: *Db, col_name: []const u8, key: []const u8) !?Doc {
        const col = try self.inner.collection(col_name);
        return col.get(key);
    }

    /// Shorthand: update in a collection.
    pub fn update(self: *Db, col_name: []const u8, key: []const u8, value: []const u8) !bool {
        const col = try self.inner.collection(col_name);
        return col.update(key, value);
    }

    /// Shorthand: delete from a collection.
    pub fn delete(self: *Db, col_name: []const u8, key: []const u8) !bool {
        const col = try self.inner.collection(col_name);
        return col.delete(key);
    }

    /// Shorthand: search (trigram-indexed).
    pub fn search(self: *Db, col_name: []const u8, query: []const u8, limit: u32, alloc: std.mem.Allocator) !Collection.TextSearchResult {
        const col = try self.inner.collection(col_name);
        return col.searchText(query, limit, alloc);
    }
};

// ── Tests ──────────────────────────────────────────────────────────────────

test "embedded client: open, insert, get, search" {
    const alloc = std.testing.allocator;

    // Use a temp dir
    const tmp_dir = "/tmp/turbodb_client_test";
    std.fs.cwd().deleteTree(tmp_dir) catch {};

    var db = try Db.open(alloc, tmp_dir);
    defer db.close();

    // Insert
    const id = try db.insert("users", "u1", "{\"name\":\"alice\",\"role\":\"admin\"}");
    try std.testing.expect(id > 0);

    // Get
    const doc = try db.get("users", "u1");
    try std.testing.expect(doc != null);

    // Update
    const updated = try db.update("users", "u1", "{\"name\":\"alice\",\"role\":\"superadmin\"}");
    try std.testing.expect(updated);

    // Delete
    const deleted = try db.delete("users", "u1");
    try std.testing.expect(deleted);

    // Get after delete should be null
    const gone = try db.get("users", "u1");
    try std.testing.expect(gone == null);

    // Cleanup
    std.fs.cwd().deleteTree(tmp_dir) catch {};
}
