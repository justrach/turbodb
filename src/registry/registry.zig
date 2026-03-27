/// ZagDB — Core registry logic
///
/// Orchestrates TurboDB collections, blob store, and validation to provide:
///   - publish: validate manifest + signature, store blob, insert metadata
///   - search: full-text search via TurboDB trigram index
///   - getPackage / getVersion / listVersions: metadata lookups
///   - yank: non-destructive version yanking (Cargo-style)
///   - download: stream blob by content hash
///
/// Collections used:
///   packages  — key: package name → metadata JSON
///   versions  — key: "name@semver" → version metadata JSON
///   blobs     — key: blake3 hex → blob metadata JSON
///   identities — key: ed25519 pubkey hex → profile JSON
const std = @import("std");
const hash_mod = @import("hash.zig");
const sign_mod = @import("sign.zig");
const manifest_mod = @import("manifest.zig");
const store_mod = @import("store.zig");

pub const PublishResult = struct {
    package_name: []const u8,
    version: []const u8,
    source_hash_hex: [64]u8,
};

pub const PackageInfo = struct {
    name: []const u8,
    description: []const u8,
    version: []const u8,
};

pub const VersionInfo = struct {
    package: []const u8,
    version: []const u8,
    source_hash: []const u8,
    yanked: bool,
};

/// The Registry operates in two modes:
///   1. Standalone (no TurboDB) — just blob store + in-memory maps for testing/CLI
///   2. Full (with TurboDB) — blob store + TurboDB collections
///
/// This module implements standalone mode. TurboDB integration is wired in api.zig/main.zig
/// where the server has access to the Database instance.
pub const Registry = struct {
    store: store_mod.BlobStore,
    data_dir: []const u8,
    alloc: std.mem.Allocator,

    // In-memory indexes (for standalone mode / testing)
    // In production, these are backed by TurboDB collections
    packages: std.StringHashMap([]const u8), // name → metadata JSON
    versions: std.StringHashMap([]const u8), // "name@ver" → version JSON
    identities: std.StringHashMap([]const u8), // pubkey hex → identity JSON

    pub fn init(alloc: std.mem.Allocator, data_dir: []const u8) !Registry {
        // Ensure data directory exists
        std.fs.cwd().makePath(data_dir) catch {};

        return .{
            .store = try store_mod.BlobStore.init(alloc, data_dir),
            .data_dir = data_dir,
            .alloc = alloc,
            .packages = std.StringHashMap([]const u8).init(alloc),
            .versions = std.StringHashMap([]const u8).init(alloc),
            .identities = std.StringHashMap([]const u8).init(alloc),
        };
    }

    pub fn deinit(self: *Registry) void {
        // Free all owned strings
        var pkg_it = self.packages.iterator();
        while (pkg_it.next()) |entry| {
            self.alloc.free(entry.key_ptr.*);
            self.alloc.free(entry.value_ptr.*);
        }
        self.packages.deinit();

        var ver_it = self.versions.iterator();
        while (ver_it.next()) |entry| {
            self.alloc.free(entry.key_ptr.*);
            self.alloc.free(entry.value_ptr.*);
        }
        self.versions.deinit();

        var id_it = self.identities.iterator();
        while (id_it.next()) |entry| {
            self.alloc.free(entry.key_ptr.*);
            self.alloc.free(entry.value_ptr.*);
        }
        self.identities.deinit();
    }

    // ─── Publish ────────────────────────────────────────────────────────────

    /// Publish a new package version.
    /// 1. Hash the tarball content
    /// 2. Parse manifest from the tarball (assumes tarball IS the manifest JSON for MVP)
    /// 3. Verify Ed25519 signature over the content hash
    /// 4. Store blob (content-addressed, deduped)
    /// 5. Insert/update package and version metadata
    /// Returns error if version already exists (immutable).
    pub fn publish(
        self: *Registry,
        tarball: []const u8,
        signature: [64]u8,
        pubkey: [32]u8,
    ) !PublishResult {
        // 1. Hash content
        const content_hash = try self.store.put(tarball);
        var hash_hex: [64]u8 = undefined;
        hash_mod.hexEncode(content_hash, &hash_hex);

        // 2. Verify signature (signs the content hash)
        if (!sign_mod.verify(&hash_hex, signature, pubkey)) {
            return error.InvalidSignature;
        }

        // 3. Parse manifest from tarball content
        const m = manifest_mod.parse(self.alloc, tarball) catch return error.InvalidManifest;

        // 4. Check version doesn't already exist (immutability)
        var ver_key_buf: [256]u8 = undefined;
        const ver_key = try std.fmt.bufPrint(&ver_key_buf, "{s}@{s}", .{ m.name, m.version });
        if (self.versions.contains(ver_key)) {
            return error.VersionAlreadyExists;
        }

        // 5. Build package metadata JSON
        var pubkey_hex = sign_mod.pubkeyHex(pubkey);
        var sig_hex = sign_mod.signatureHex(signature);
        const now = std.time.timestamp();

        // Package metadata (upsert — latest version wins)
        var pkg_buf: [2048]u8 = undefined;
        const pkg_json = try std.fmt.bufPrint(&pkg_buf,
            \\{{"name":"{s}","description":"{s}","author_pubkey":"{s}","latest_version":"{s}","updated_at":{d}}}
        , .{ m.name, m.description, &pubkey_hex, m.version, now });

        // Version metadata
        var ver_buf: [2048]u8 = undefined;
        const ver_json = try std.fmt.bufPrint(&ver_buf,
            \\{{"package":"{s}","version":"{s}","source_hash":"{s}","signature":"{s}","yanked":false,"published_at":{d}}}
        , .{ m.name, m.version, &hash_hex, &sig_hex, now });

        // 6. Store in maps (owned copies)
        const owned_name = try self.alloc.dupe(u8, m.name);
        const owned_pkg_json = try self.alloc.dupe(u8, pkg_json);

        // Remove old package entry if exists
        if (self.packages.fetchRemove(m.name)) |old| {
            self.alloc.free(old.key);
            self.alloc.free(old.value);
        }
        try self.packages.put(owned_name, owned_pkg_json);

        const owned_ver_key = try self.alloc.dupe(u8, ver_key);
        const owned_ver_json = try self.alloc.dupe(u8, ver_json);
        try self.versions.put(owned_ver_key, owned_ver_json);

        // 7. Store blob metadata
        var meta_buf: [1024]u8 = undefined;
        const blob_meta = try self.store.metadataJson(&hash_hex, tarball.len, &meta_buf);
        _ = blob_meta; // In TurboDB mode, this would be inserted into blobs collection

        return .{
            .package_name = owned_name,
            .version = m.version,
            .source_hash_hex = hash_hex,
        };
    }

    // ─── Search ─────────────────────────────────────────────────────────────

    /// Search packages by name substring (simple in-memory search for standalone mode).
    /// In TurboDB mode, this delegates to Collection.searchText() for trigram search.
    pub fn search(self: *Registry, query: []const u8, limit: u32, results_buf: []PackageInfo) !u32 {
        var count: u32 = 0;
        var it = self.packages.iterator();
        while (it.next()) |entry| {
            if (count >= limit) break;
            if (count >= results_buf.len) break;

            // Simple substring match on key (package name)
            if (std.mem.indexOf(u8, entry.key_ptr.*, query) != null) {
                results_buf[count] = .{
                    .name = entry.key_ptr.*,
                    .description = jsonGetField(entry.value_ptr.*, "description") orelse "",
                    .version = jsonGetField(entry.value_ptr.*, "latest_version") orelse "0.0.0",
                };
                count += 1;
                continue;
            }

            // Also check description
            const desc = jsonGetField(entry.value_ptr.*, "description") orelse "";
            if (desc.len > 0 and std.mem.indexOf(u8, desc, query) != null) {
                results_buf[count] = .{
                    .name = entry.key_ptr.*,
                    .description = desc,
                    .version = jsonGetField(entry.value_ptr.*, "latest_version") orelse "0.0.0",
                };
                count += 1;
            }
        }
        return count;
    }

    // ─── Lookups ────────────────────────────────────────────────────────────

    /// Get package metadata JSON by name.
    pub fn getPackage(self: *Registry, name: []const u8) ?[]const u8 {
        return self.packages.get(name);
    }

    /// Get version metadata JSON.
    pub fn getVersion(self: *Registry, name: []const u8, version: []const u8) ?[]const u8 {
        var key_buf: [256]u8 = undefined;
        const key = std.fmt.bufPrint(&key_buf, "{s}@{s}", .{ name, version }) catch return null;
        return self.versions.get(key);
    }

    // ─── Yank ───────────────────────────────────────────────────────────────

    /// Yank a version (Cargo-style: non-destructive, existing locks keep working).
    /// Requires valid signature from the original publisher.
    pub fn yank(
        self: *Registry,
        name: []const u8,
        version: []const u8,
        signature: [64]u8,
        pubkey: [32]u8,
    ) !bool {
        var key_buf: [256]u8 = undefined;
        const key = try std.fmt.bufPrint(&key_buf, "{s}@{s}", .{ name, version });

        const ver_json = self.versions.get(key) orelse return false;

        // Verify the yanker owns this package
        const pkg_json = self.packages.get(name) orelse return false;
        const pkg_pubkey = jsonGetField(pkg_json, "author_pubkey") orelse return false;
        var pubkey_hex = sign_mod.pubkeyHex(pubkey);
        if (!std.mem.eql(u8, pkg_pubkey, &pubkey_hex)) return error.Unauthorized;

        // Verify signature over "yank:name@version"
        var msg_buf: [512]u8 = undefined;
        const msg = try std.fmt.bufPrint(&msg_buf, "yank:{s}@{s}", .{ name, version });
        if (!sign_mod.verify(msg, signature, pubkey)) return error.InvalidSignature;

        // Replace yanked:false with yanked:true
        // (In TurboDB mode, this is a Collection.update() call)
        if (std.mem.indexOf(u8, ver_json, "\"yanked\":false")) |pos| {
            const new_json = try self.alloc.dupe(u8, ver_json);
            @memcpy(new_json[pos + 9 ..][0..5], "true ");
            // Update the map
            if (self.versions.getEntry(key)) |entry| {
                self.alloc.free(entry.value_ptr.*);
                entry.value_ptr.* = new_json;
            }
            return true;
        }
        return false; // already yanked
    }

    // ─── Download ───────────────────────────────────────────────────────────

    /// Read blob content by hash. Caller owns returned slice.
    pub fn download(self: *Registry, source_hash_hex: []const u8) ![]u8 {
        return self.store.readBlob(source_hash_hex);
    }

    /// Check if a blob exists by hash.
    pub fn blobExists(self: *Registry, source_hash_hex: []const u8) bool {
        return self.store.exists(source_hash_hex);
    }

    // ─── Identity ───────────────────────────────────────────────────────────

    /// Register an author identity.
    pub fn registerIdentity(self: *Registry, pubkey: [32]u8, display_name: []const u8, email: []const u8) !void {
        var pubkey_hex = sign_mod.pubkeyHex(pubkey);
        const now = std.time.timestamp();

        var buf: [1024]u8 = undefined;
        const json = try std.fmt.bufPrint(&buf,
            \\{{"pubkey":"{s}","display_name":"{s}","email":"{s}","registered_at":{d}}}
        , .{ &pubkey_hex, display_name, email, now });

        const owned_key = try self.alloc.dupe(u8, &pubkey_hex);
        const owned_json = try self.alloc.dupe(u8, json);
        try self.identities.put(owned_key, owned_json);
    }

    /// Get identity by pubkey hex.
    pub fn getIdentity(self: *Registry, pubkey_hex: []const u8) ?[]const u8 {
        return self.identities.get(pubkey_hex);
    }
};

// ─── Helpers ────────────────────────────────────────────────────────────────

/// Simple JSON field extractor (same pattern as doc.zig's jsonGetField)
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
    // number / bool / null
    var end = i;
    while (end < json.len and json[end] != ',' and json[end] != '}' and
        json[end] != '\n' and json[end] != ' ') end += 1;
    return json[i..end];
}

// ─── Tests ──────────────────────────────────────────────────────────────────

test "publish and search" {
    const alloc = std.testing.allocator;
    const tmp_dir = "/tmp/zagdb-registry-test";
    std.fs.cwd().deleteTree(tmp_dir) catch {};
    defer std.fs.cwd().deleteTree(tmp_dir) catch {};

    var reg = try Registry.init(alloc, tmp_dir);
    defer reg.deinit();

    // Create a keypair and a minimal manifest as "tarball"
    const kp = sign_mod.KeyPair.generate();

    const tarball =
        \\{"name":"test-pkg","version":"1.0.0","description":"A test package"}
    ;

    // Sign the content hash
    const content_hash = hash_mod.hashBytes(tarball);
    var hash_hex: [64]u8 = undefined;
    hash_mod.hexEncode(content_hash, &hash_hex);
    const sig = sign_mod.sign(&hash_hex, kp.secret_key);

    // Publish
    const result = try reg.publish(tarball, sig, kp.public_key);
    try std.testing.expectEqualStrings("test-pkg", result.package_name);

    // Search by name
    var search_results: [10]PackageInfo = undefined;
    const count = try reg.search("test", 10, &search_results);
    try std.testing.expectEqual(@as(u32, 1), count);
    try std.testing.expectEqualStrings("test-pkg", search_results[0].name);

    // Lookup
    try std.testing.expect(reg.getPackage("test-pkg") != null);
    try std.testing.expect(reg.getVersion("test-pkg", "1.0.0") != null);
    try std.testing.expect(reg.getVersion("test-pkg", "2.0.0") == null);
}

test "publish duplicate version fails" {
    const alloc = std.testing.allocator;
    const tmp_dir = "/tmp/zagdb-registry-dup";
    std.fs.cwd().deleteTree(tmp_dir) catch {};
    defer std.fs.cwd().deleteTree(tmp_dir) catch {};

    var reg = try Registry.init(alloc, tmp_dir);
    defer reg.deinit();

    const kp = sign_mod.KeyPair.generate();
    const tarball =
        \\{"name":"dup-pkg","version":"1.0.0","description":"test"}
    ;
    const content_hash = hash_mod.hashBytes(tarball);
    var hash_hex: [64]u8 = undefined;
    hash_mod.hexEncode(content_hash, &hash_hex);
    const sig = sign_mod.sign(&hash_hex, kp.secret_key);

    _ = try reg.publish(tarball, sig, kp.public_key);

    // Second publish of same version should fail
    try std.testing.expectError(error.VersionAlreadyExists, reg.publish(tarball, sig, kp.public_key));
}

test "invalid signature rejected" {
    const alloc = std.testing.allocator;
    const tmp_dir = "/tmp/zagdb-registry-badsig";
    std.fs.cwd().deleteTree(tmp_dir) catch {};
    defer std.fs.cwd().deleteTree(tmp_dir) catch {};

    var reg = try Registry.init(alloc, tmp_dir);
    defer reg.deinit();

    const kp = sign_mod.KeyPair.generate();
    const other_kp = sign_mod.KeyPair.generate();
    const tarball =
        \\{"name":"bad-sig","version":"1.0.0","description":"test"}
    ;
    const content_hash = hash_mod.hashBytes(tarball);
    var hash_hex: [64]u8 = undefined;
    hash_mod.hexEncode(content_hash, &hash_hex);

    // Sign with wrong key
    const bad_sig = sign_mod.sign(&hash_hex, other_kp.secret_key);
    try std.testing.expectError(error.InvalidSignature, reg.publish(tarball, bad_sig, kp.public_key));
}

test "yank version" {
    const alloc = std.testing.allocator;
    const tmp_dir = "/tmp/zagdb-registry-yank";
    std.fs.cwd().deleteTree(tmp_dir) catch {};
    defer std.fs.cwd().deleteTree(tmp_dir) catch {};

    var reg = try Registry.init(alloc, tmp_dir);
    defer reg.deinit();

    const kp = sign_mod.KeyPair.generate();
    const tarball =
        \\{"name":"yank-pkg","version":"1.0.0","description":"will be yanked"}
    ;
    const content_hash = hash_mod.hashBytes(tarball);
    var hash_hex: [64]u8 = undefined;
    hash_mod.hexEncode(content_hash, &hash_hex);
    const sig = sign_mod.sign(&hash_hex, kp.secret_key);

    _ = try reg.publish(tarball, sig, kp.public_key);

    // Yank it
    const yank_sig = sign_mod.sign("yank:yank-pkg@1.0.0", kp.secret_key);
    const yanked = try reg.yank("yank-pkg", "1.0.0", yank_sig, kp.public_key);
    try std.testing.expect(yanked);

    // Version metadata should show yanked
    const ver = reg.getVersion("yank-pkg", "1.0.0").?;
    try std.testing.expect(std.mem.indexOf(u8, ver, "\"yanked\":true") != null);
}

test "download after publish" {
    const alloc = std.testing.allocator;
    const tmp_dir = "/tmp/zagdb-registry-dl";
    std.fs.cwd().deleteTree(tmp_dir) catch {};
    defer std.fs.cwd().deleteTree(tmp_dir) catch {};

    var reg = try Registry.init(alloc, tmp_dir);
    defer reg.deinit();

    const kp = sign_mod.KeyPair.generate();
    const tarball =
        \\{"name":"dl-pkg","version":"1.0.0","description":"downloadable"}
    ;
    const content_hash = hash_mod.hashBytes(tarball);
    var hash_hex: [64]u8 = undefined;
    hash_mod.hexEncode(content_hash, &hash_hex);
    const sig = sign_mod.sign(&hash_hex, kp.secret_key);

    const result = try reg.publish(tarball, sig, kp.public_key);

    // Download by hash
    const data = try reg.download(&result.source_hash_hex);
    defer alloc.free(data);
    try std.testing.expectEqualStrings(tarball, data);
}

test "register and lookup identity" {
    const alloc = std.testing.allocator;
    const tmp_dir = "/tmp/zagdb-registry-id";
    std.fs.cwd().deleteTree(tmp_dir) catch {};
    defer std.fs.cwd().deleteTree(tmp_dir) catch {};

    var reg = try Registry.init(alloc, tmp_dir);
    defer reg.deinit();

    const kp = sign_mod.KeyPair.generate();
    try reg.registerIdentity(kp.public_key, "Alice", "alice@example.com");

    const pubkey_hex = sign_mod.pubkeyHex(kp.public_key);
    const identity = reg.getIdentity(&pubkey_hex);
    try std.testing.expect(identity != null);
    try std.testing.expect(std.mem.indexOf(u8, identity.?, "Alice") != null);
}
