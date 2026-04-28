/// ZagDB — Content-addressed blob store
///
/// Stores package tarballs on disk at:
///   <data_dir>/blobs/<2-hex-prefix>/<64-hex-hash>.tar.zst
///
/// TurboDB's `blobs` collection tracks metadata only (hash, size, disk path).
/// Deduplication is automatic: same content → same hash → same file.
const std = @import("std");
const compat = @import("compat");
const hash_mod = @import("hash.zig");

/// BlobStore manages content-addressed blob storage.
/// Works standalone (filesystem only) — TurboDB integration happens in registry.zig.
pub const BlobStore = struct {
    data_dir: []const u8,
    alloc: std.mem.Allocator,

    pub fn init(alloc: std.mem.Allocator, data_dir: []const u8) !BlobStore {
        // Ensure blobs directory exists
        var path_buf: [512]u8 = undefined;
        const blobs_dir = try std.fmt.bufPrint(&path_buf, "{s}/blobs", .{data_dir});
        try compat.cwd().makePath(blobs_dir);

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
        compat.cwd().makePath(prefix_dir) catch {};

        // Write blob atomically: write to .tmp, then rename
        var tmp_buf: [512]u8 = undefined;
        const tmp_path = try std.fmt.bufPrint(&tmp_buf, "{s}.tmp", .{file_path});

        {
            const file = try compat.cwd().createFile(tmp_path, .{});
            defer file.close();
            try file.writeAll(data);
        }

        // Atomic rename
        compat.cwd().rename(tmp_path, file_path) catch |err| {
            // If rename fails, try to clean up tmp
            compat.cwd().deleteFile(tmp_path) catch {};
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

        const file = compat.cwd().openFile(path, .{}) catch return null;
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
        return compat.cwd().readFileAlloc(self.alloc, path, 256 * 1024 * 1024);
    }

    /// Open a blob file for streaming reads. Caller owns the file handle.
    pub fn openBlob(self: *BlobStore, hash_hex: []const u8) !compat.File {
        if (hash_hex.len != 64) return error.InvalidHash;
        var path_buf: [512]u8 = undefined;
        const path = try std.fmt.bufPrint(&path_buf, "{s}/blobs/{s}/{s}.tar.zst", .{
            self.data_dir,
            hash_hex[0..2],
            hash_hex[0..],
        });
        return compat.cwd().openFile(path, .{});
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
        const now = compat.timestamp();
        return std.fmt.bufPrint(buf,
            \\{{"hash":"{s}","size":{d},"content_type":"application/zstd","disk_path":"{s}","uploaded_at":{d},"ref_count":1}}
        , .{ hash_hex, data_len, disk_path, now });
    }

    fn existsPath(_: *BlobStore, path: []const u8) bool {
        compat.cwd().access(path, .{}) catch return false;
        return true;
    }
};

// ─── Tests ──────────────────────────────────────────────────────────────────

test "put and read blob" {
    const alloc = std.testing.allocator;
    const tmp_dir = "/tmp/zagdb-store-test";
    compat.cwd().deleteTree(tmp_dir) catch {};

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

    compat.cwd().deleteTree(tmp_dir) catch {};
}

test "put deduplicates" {
    const alloc = std.testing.allocator;
    const tmp_dir = "/tmp/zagdb-store-dedup";
    compat.cwd().deleteTree(tmp_dir) catch {};

    var store = try BlobStore.init(alloc, tmp_dir);

    const data = "same content twice";
    const h1 = try store.put(data);
    const h2 = try store.put(data);

    // Same hash
    try std.testing.expectEqual(h1, h2);

    compat.cwd().deleteTree(tmp_dir) catch {};
}

test "different content different hash" {
    const alloc = std.testing.allocator;
    const tmp_dir = "/tmp/zagdb-store-diff";
    compat.cwd().deleteTree(tmp_dir) catch {};

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

    compat.cwd().deleteTree(tmp_dir) catch {};
}

test "nonexistent blob" {
    const alloc = std.testing.allocator;
    const tmp_dir = "/tmp/zagdb-store-none";
    compat.cwd().deleteTree(tmp_dir) catch {};

    var store = try BlobStore.init(alloc, tmp_dir);

    const fake_hex = "0000000000000000000000000000000000000000000000000000000000000000";
    try std.testing.expect(!store.exists(fake_hex));
    try std.testing.expectEqual(@as(?u64, null), store.blobSize(fake_hex));

    compat.cwd().deleteTree(tmp_dir) catch {};
}

test "metadata json" {
    const alloc = std.testing.allocator;
    const tmp_dir = "/tmp/zagdb-store-meta";
    compat.cwd().deleteTree(tmp_dir) catch {};

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

    compat.cwd().deleteTree(tmp_dir) catch {};
}
