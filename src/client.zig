const std = @import("std");
const compat = @import("compat");
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
        compat.cwd().makeDir(data_dir) catch |e| switch (e) {
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

    pub fn collectionForTenant(self: *Db, tenant_id: []const u8, name: []const u8) !*Collection {
        return self.inner.collectionForTenant(tenant_id, name);
    }

    /// Shorthand: insert into a collection.
    pub fn insert(self: *Db, col_name: []const u8, key: []const u8, value: []const u8) !u64 {
        return self.insertForTenant(collection_mod.DEFAULT_TENANT, col_name, key, value);
    }

    pub fn insertForTenant(self: *Db, tenant_id: []const u8, col_name: []const u8, key: []const u8, value: []const u8) !u64 {
        try self.inner.recordTenantOperation(tenant_id);
        try self.inner.ensureTenantStorageAvailable(tenant_id, value.len);
        const col = try self.inner.collectionForTenant(tenant_id, col_name);
        return col.insert(key, value);
    }

    /// Shorthand: get from a collection.
    pub fn get(self: *Db, col_name: []const u8, key: []const u8) !?Doc {
        return self.getForTenant(collection_mod.DEFAULT_TENANT, col_name, key);
    }

    pub fn getForTenant(self: *Db, tenant_id: []const u8, col_name: []const u8, key: []const u8) !?Doc {
        try self.inner.recordTenantOperation(tenant_id);
        const col = try self.inner.collectionForTenant(tenant_id, col_name);
        return col.get(key);
    }

    pub fn getForTenantAsOf(self: *Db, tenant_id: []const u8, col_name: []const u8, key: []const u8, ts_ms: i64) !?Doc {
        try self.inner.recordTenantOperation(tenant_id);
        const col = try self.inner.collectionForTenant(tenant_id, col_name);
        return col.getAsOfTimestamp(key, ts_ms);
    }

    /// Shorthand: update in a collection.
    pub fn update(self: *Db, col_name: []const u8, key: []const u8, value: []const u8) !bool {
        return self.updateForTenant(collection_mod.DEFAULT_TENANT, col_name, key, value);
    }

    pub fn updateForTenant(self: *Db, tenant_id: []const u8, col_name: []const u8, key: []const u8, value: []const u8) !bool {
        try self.inner.recordTenantOperation(tenant_id);
        try self.inner.ensureTenantStorageAvailable(tenant_id, value.len);
        const col = try self.inner.collectionForTenant(tenant_id, col_name);
        return col.update(key, value);
    }

    /// Shorthand: delete from a collection.
    pub fn delete(self: *Db, col_name: []const u8, key: []const u8) !bool {
        return self.deleteForTenant(collection_mod.DEFAULT_TENANT, col_name, key);
    }

    pub fn deleteForTenant(self: *Db, tenant_id: []const u8, col_name: []const u8, key: []const u8) !bool {
        try self.inner.recordTenantOperation(tenant_id);
        const col = try self.inner.collectionForTenant(tenant_id, col_name);
        return col.delete(key);
    }

    /// Shorthand: search (trigram-indexed).
    pub fn search(self: *Db, col_name: []const u8, query: []const u8, limit: u32, alloc: std.mem.Allocator) !Collection.TextSearchResult {
        return self.searchForTenant(collection_mod.DEFAULT_TENANT, col_name, query, limit, alloc);
    }

    pub fn searchForTenant(self: *Db, tenant_id: []const u8, col_name: []const u8, query: []const u8, limit: u32, alloc: std.mem.Allocator) !Collection.TextSearchResult {
        try self.inner.recordTenantOperation(tenant_id);
        const col = try self.inner.collectionForTenant(tenant_id, col_name);
        return col.searchText(query, limit, alloc);
    }
};

// ── Tests ──────────────────────────────────────────────────────────────────

test "embedded client: open, insert, get, search" {
    const alloc = std.testing.allocator;

    // Use a temp dir
    const tmp_dir = "/tmp/turbodb_client_test";
    compat.cwd().deleteTree(tmp_dir) catch {};

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
    compat.cwd().deleteTree(tmp_dir) catch {};
}

test "embedded client isolates tenants" {
    const alloc = std.testing.allocator;
    const tmp_dir = "/tmp/turbodb_client_tenants";
    compat.cwd().deleteTree(tmp_dir) catch {};

    var db = try Db.open(alloc, tmp_dir);
    defer db.close();
    defer compat.cwd().deleteTree(tmp_dir) catch {};

    _ = try db.insertForTenant("tenant-a", "users", "u1", "{\"name\":\"alice\"}");
    _ = try db.insertForTenant("tenant-b", "users", "u1", "{\"name\":\"bob\"}");

    const a_doc = (try db.getForTenant("tenant-a", "users", "u1")).?;
    const b_doc = (try db.getForTenant("tenant-b", "users", "u1")).?;
    try std.testing.expectEqualStrings("{\"name\":\"alice\"}", a_doc.value);
    try std.testing.expectEqualStrings("{\"name\":\"bob\"}", b_doc.value);
}
