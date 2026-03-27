/// ZagDB — Content-addressed blob store
///
/// Stores package tarballs on disk at:
///   <data_dir>/blobs/<2-hex-prefix>/<64-hex-hash>.tar.zst
///
/// TurboDB's `blobs` collection tracks metadata only (hash, size, disk path).
/// Deduplication is automatic: same content → same hash → same file.
const std = @import("std");
const hash_mod = @import("hash.zig");
const auth_mod = @import("auth.zig");
const Visibility = auth_mod.Visibility;

/// BlobStore manages content-addressed blob storage.
/// Works standalone (filesystem only) — TurboDB integration happens in registry.zig.
pub const BlobStore = struct {
    data_dir: []const u8,
    alloc: std.mem.Allocator,

    pub fn init(alloc: std.mem.Allocator, data_dir: []const u8) !BlobStore {
        // Ensure both public and private blob directories exist
        var path_buf: [512]u8 = undefined;
        const pub_dir = try std.fmt.bufPrint(&path_buf, "{s}/public/blobs", .{data_dir});
        try std.fs.cwd().makePath(pub_dir);
        var path_buf2: [512]u8 = undefined;
        const priv_dir = try std.fmt.bufPrint(&path_buf2, "{s}/private/blobs", .{data_dir});
        try std.fs.cwd().makePath(priv_dir);
        // Also keep legacy flat blobs/ for backward compat
        var path_buf3: [512]u8 = undefined;
        const legacy_dir = try std.fmt.bufPrint(&path_buf3, "{s}/blobs", .{data_dir});
        try std.fs.cwd().makePath(legacy_dir);

        return .{
            .data_dir = data_dir,
            .alloc = alloc,
        };
    }

    /// Store a blob. Returns its BLAKE3 hash.
    /// If the blob already exists (same hash), this is a no-op (content-addressed dedup).
    pub fn put(self: *BlobStore, data: []const u8) ![32]u8 {
        const content_hash = hash_mod.hashBytes(data);
        var hex: [64]u8 = undefined;
        hash_mod.hexEncode(content_hash, &hex);

        // Build path: blobs/<first2hex>/<full64hex>.tar.zst
        var dir_buf: [512]u8 = undefined;
        const prefix_dir = try std.fmt.bufPrint(&dir_buf, "{s}/blobs/{s}", .{ self.data_dir, hex[0..2] });

        var file_buf: [512]u8 = undefined;
        const file_path = try std.fmt.bufPrint(&file_buf, "{s}/blobs/{s}/{s}.tar.zst", .{ self.data_dir, hex[0..2], hex[0..] });

        // Check if already exists (dedup)
        if (self.existsPath(file_path)) return content_hash;

        // Create prefix directory
        std.fs.cwd().makePath(prefix_dir) catch {};

        // Write blob atomically: write to .tmp, then rename
        var tmp_buf: [512]u8 = undefined;
        const tmp_path = try std.fmt.bufPrint(&tmp_buf, "{s}.tmp", .{file_path});

        {
            const file = try std.fs.cwd().createFile(tmp_path, .{});
            defer file.close();
            try file.writeAll(data);
        }

        // Atomic rename
        std.fs.cwd().rename(tmp_path, file_path) catch |err| {
            // If rename fails, try to clean up tmp
            std.fs.cwd().deleteFile(tmp_path) catch {};
            return err;
        };

        return content_hash;
    }

    /// Check if a blob exists by its hex hash.
    pub fn exists(self: *BlobStore, hash_hex: []const u8) bool {
        if (hash_hex.len != 64) return false;
        var path_buf: [512]u8 = undefined;
        const path = std.fmt.bufPrint(&path_buf, "{s}/blobs/{s}/{s}.tar.zst", .{
            self.data_dir,
            hash_hex[0..2],
            hash_hex[0..],
        }) catch return false;
        return self.existsPath(path);
    }

    /// Get the size of a blob in bytes. Returns null if not found.
    pub fn blobSize(self: *BlobStore, hash_hex: []const u8) ?u64 {
        if (hash_hex.len != 64) return null;
        var path_buf: [512]u8 = undefined;
        const path = std.fmt.bufPrint(&path_buf, "{s}/blobs/{s}/{s}.tar.zst", .{
            self.data_dir,
            hash_hex[0..2],
            hash_hex[0..],
        }) catch return null;

        const file = std.fs.cwd().openFile(path, .{}) catch return null;
        defer file.close();
        const stat = file.stat() catch return null;
        return stat.size;
    }

    /// Read a blob's contents. Caller owns returned slice.
    pub fn readBlob(self: *BlobStore, hash_hex: []const u8) ![]u8 {
        if (hash_hex.len != 64) return error.InvalidHash;
        var path_buf: [512]u8 = undefined;
        const path = try std.fmt.bufPrint(&path_buf, "{s}/blobs/{s}/{s}.tar.zst", .{
            self.data_dir,
            hash_hex[0..2],
            hash_hex[0..],
        });
        return std.fs.cwd().readFileAlloc(self.alloc, path, 256 * 1024 * 1024);
    }

    /// Open a blob file for streaming reads. Caller owns the file handle.
    pub fn openBlob(self: *BlobStore, hash_hex: []const u8) !std.fs.File {
        if (hash_hex.len != 64) return error.InvalidHash;
        var path_buf: [512]u8 = undefined;
        const path = try std.fmt.bufPrint(&path_buf, "{s}/blobs/{s}/{s}.tar.zst", .{
            self.data_dir,
            hash_hex[0..2],
            hash_hex[0..],
        });
        return std.fs.cwd().openFile(path, .{});
    }

    /// Get the filesystem path for a blob (for serving over HTTP).
    pub fn blobPath(self: *BlobStore, hash_hex: []const u8, out: []u8) ![]const u8 {
        if (hash_hex.len != 64) return error.InvalidHash;
        return std.fmt.bufPrint(out, "{s}/blobs/{s}/{s}.tar.zst", .{
            self.data_dir,
            hash_hex[0..2],
            hash_hex[0..],
        });
    }

    /// Build metadata JSON for a blob (to store in TurboDB).
    pub fn metadataJson(_: *BlobStore, hash_hex: []const u8, data_len: usize, buf: []u8) ![]const u8 {
        var path_buf: [512]u8 = undefined;
        const disk_path = try std.fmt.bufPrint(&path_buf, "blobs/{s}/{s}.tar.zst", .{
            hash_hex[0..2],
            hash_hex[0..],
        });
        const now = std.time.timestamp();
        return std.fmt.bufPrint(buf,
            \\{{"hash":"{s}","size":{d},"content_type":"application/zstd","disk_path":"{s}","uploaded_at":{d},"ref_count":1}}
        , .{ hash_hex, data_len, disk_path, now });
    }

    // ─── Visibility-aware methods ────────────────────────────────────────────

    /// Store a blob in the public or private directory.
    pub fn putScoped(self: *BlobStore, data: []const u8, visibility: Visibility) ![32]u8 {
        const content_hash = hash_mod.hashBytes(data);
        var hex: [64]u8 = undefined;
        hash_mod.hexEncode(content_hash, &hex);

        const scope = visibility.toStr();
        var dir_buf: [512]u8 = undefined;
        const prefix_dir = try std.fmt.bufPrint(&dir_buf, "{s}/{s}/blobs/{s}", .{ self.data_dir, scope, hex[0..2] });

        var file_buf: [512]u8 = undefined;
        const file_path = try std.fmt.bufPrint(&file_buf, "{s}/{s}/blobs/{s}/{s}.tar.zst", .{ self.data_dir, scope, hex[0..2], hex[0..] });

        if (self.existsPath(file_path)) return content_hash;

        std.fs.cwd().makePath(prefix_dir) catch {};

        var tmp_buf: [512]u8 = undefined;
        const tmp_path = try std.fmt.bufPrint(&tmp_buf, "{s}.tmp", .{file_path});

        {
            const file = try std.fs.cwd().createFile(tmp_path, .{});
            defer file.close();
            try file.writeAll(data);
        }

        std.fs.cwd().rename(tmp_path, file_path) catch |err| {
            std.fs.cwd().deleteFile(tmp_path) catch {};
            return err;
        };

        return content_hash;
    }

    /// Check if a blob exists in a specific scope.
    pub fn existsScoped(self: *BlobStore, hash_hex: []const u8, visibility: Visibility) bool {
        if (hash_hex.len != 64) return false;
        const scope = visibility.toStr();
        var path_buf: [512]u8 = undefined;
        const path = std.fmt.bufPrint(&path_buf, "{s}/{s}/blobs/{s}/{s}.tar.zst", .{
            self.data_dir, scope, hash_hex[0..2], hash_hex[0..],
        }) catch return false;
        return self.existsPath(path);
    }

    /// Read a scoped blob. Caller owns returned slice.
    pub fn readBlobScoped(self: *BlobStore, hash_hex: []const u8, visibility: Visibility) ![]u8 {
        if (hash_hex.len != 64) return error.InvalidHash;
        const scope = visibility.toStr();
        var path_buf: [512]u8 = undefined;
        const path = try std.fmt.bufPrint(&path_buf, "{s}/{s}/blobs/{s}/{s}.tar.zst", .{
            self.data_dir, scope, hash_hex[0..2], hash_hex[0..],
        });
        return std.fs.cwd().readFileAlloc(self.alloc, path, 256 * 1024 * 1024);
    }

    /// Build metadata JSON with visibility field.
    pub fn metadataJsonScoped(_: *BlobStore, hash_hex: []const u8, data_len: usize, visibility: Visibility, buf: []u8) ![]const u8 {
        const scope = visibility.toStr();
        var path_buf: [512]u8 = undefined;
        const disk_path = try std.fmt.bufPrint(&path_buf, "{s}/blobs/{s}/{s}.tar.zst", .{
            scope, hash_hex[0..2], hash_hex[0..],
        });
        const now = std.time.timestamp();
        return std.fmt.bufPrint(buf,
            \\{{"hash":"{s}","size":{d},"content_type":"application/zstd","disk_path":"{s}","visibility":"{s}","uploaded_at":{d},"ref_count":1}}
        , .{ hash_hex, data_len, disk_path, scope, now });
    }

    fn existsPath(_: *BlobStore, path: []const u8) bool {
        std.fs.cwd().access(path, .{}) catch return false;
        return true;
    }
};

// ─── Tests ──────────────────────────────────────────────────────────────────

test "put and read blob" {
    const alloc = std.testing.allocator;
    const tmp_dir = "/tmp/zagdb-store-test";
    std.fs.cwd().deleteTree(tmp_dir) catch {};

    var store = try BlobStore.init(alloc, tmp_dir);

    const data = "hello zag package content";
    const content_hash = try store.put(data);

    // Should exist now
    var hex: [64]u8 = undefined;
    hash_mod.hexEncode(content_hash, &hex);
    try std.testing.expect(store.exists(&hex));

    // Read it back
    const read_back = try store.readBlob(&hex);
    defer alloc.free(read_back);
    try std.testing.expectEqualStrings(data, read_back);

    // Size should match
    try std.testing.expectEqual(@as(u64, data.len), store.blobSize(&hex).?);

    std.fs.cwd().deleteTree(tmp_dir) catch {};
}

test "put deduplicates" {
    const alloc = std.testing.allocator;
    const tmp_dir = "/tmp/zagdb-store-dedup";
    std.fs.cwd().deleteTree(tmp_dir) catch {};

    var store = try BlobStore.init(alloc, tmp_dir);

    const data = "same content twice";
    const h1 = try store.put(data);
    const h2 = try store.put(data);

    // Same hash
    try std.testing.expectEqual(h1, h2);

    std.fs.cwd().deleteTree(tmp_dir) catch {};
}

test "different content different hash" {
    const alloc = std.testing.allocator;
    const tmp_dir = "/tmp/zagdb-store-diff";
    std.fs.cwd().deleteTree(tmp_dir) catch {};

    var store = try BlobStore.init(alloc, tmp_dir);

    const h1 = try store.put("content A");
    const h2 = try store.put("content B");

    try std.testing.expect(!std.mem.eql(u8, &h1, &h2));

    // Both exist
    var hex1: [64]u8 = undefined;
    var hex2: [64]u8 = undefined;
    hash_mod.hexEncode(h1, &hex1);
    hash_mod.hexEncode(h2, &hex2);
    try std.testing.expect(store.exists(&hex1));
    try std.testing.expect(store.exists(&hex2));

    std.fs.cwd().deleteTree(tmp_dir) catch {};
}

test "nonexistent blob" {
    const alloc = std.testing.allocator;
    const tmp_dir = "/tmp/zagdb-store-none";
    std.fs.cwd().deleteTree(tmp_dir) catch {};

    var store = try BlobStore.init(alloc, tmp_dir);

    const fake_hex = "0000000000000000000000000000000000000000000000000000000000000000";
    try std.testing.expect(!store.exists(fake_hex));
    try std.testing.expectEqual(@as(?u64, null), store.blobSize(fake_hex));

    std.fs.cwd().deleteTree(tmp_dir) catch {};
}

test "metadata json" {
    const alloc = std.testing.allocator;
    const tmp_dir = "/tmp/zagdb-store-meta";
    std.fs.cwd().deleteTree(tmp_dir) catch {};

    var store = try BlobStore.init(alloc, tmp_dir);

    const data = "test blob";
    const content_hash = try store.put(data);
    var hex: [64]u8 = undefined;
    hash_mod.hexEncode(content_hash, &hex);

    var buf: [1024]u8 = undefined;
    const json = try store.metadataJson(&hex, data.len, &buf);

    // Should contain the hash and size
    try std.testing.expect(std.mem.indexOf(u8, json, &hex) != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"size\":9") != null);

    std.fs.cwd().deleteTree(tmp_dir) catch {};
}

test "scoped put and read" {
    const alloc = std.testing.allocator;
    const tmp_dir = "/tmp/zagdb-store-scoped";
    std.fs.cwd().deleteTree(tmp_dir) catch {};
    defer std.fs.cwd().deleteTree(tmp_dir) catch {};

    var store = try BlobStore.init(alloc, tmp_dir);

    // Store a public blob
    const pub_data = "public package content";
    const pub_hash = try store.putScoped(pub_data, .public);
    var pub_hex: [64]u8 = undefined;
    hash_mod.hexEncode(pub_hash, &pub_hex);

    // Store a private blob
    const priv_data = "private secret content";
    const priv_hash = try store.putScoped(priv_data, .private);
    var priv_hex: [64]u8 = undefined;
    hash_mod.hexEncode(priv_hash, &priv_hex);

    // Public blob exists in public scope only
    try std.testing.expect(store.existsScoped(&pub_hex, .public));
    try std.testing.expect(!store.existsScoped(&pub_hex, .private));

    // Private blob exists in private scope only
    try std.testing.expect(store.existsScoped(&priv_hex, .private));
    try std.testing.expect(!store.existsScoped(&priv_hex, .public));

    // Read back
    const read_pub = try store.readBlobScoped(&pub_hex, .public);
    defer alloc.free(read_pub);
    try std.testing.expectEqualStrings(pub_data, read_pub);

    const read_priv = try store.readBlobScoped(&priv_hex, .private);
    defer alloc.free(read_priv);
    try std.testing.expectEqualStrings(priv_data, read_priv);
}

test "scoped metadata json" {
    const alloc = std.testing.allocator;
    const tmp_dir = "/tmp/zagdb-store-scoped-meta";
    std.fs.cwd().deleteTree(tmp_dir) catch {};
    defer std.fs.cwd().deleteTree(tmp_dir) catch {};

    var store = try BlobStore.init(alloc, tmp_dir);

    const data = "test content";
    const h = try store.putScoped(data, .private);
    var hex: [64]u8 = undefined;
    hash_mod.hexEncode(h, &hex);

    var buf: [1024]u8 = undefined;
    const json = try store.metadataJsonScoped(&hex, data.len, .private, &buf);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"visibility\":\"private\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "private/blobs") != null);
}
