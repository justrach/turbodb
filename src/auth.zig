/// TurboDB — Authentication & Authorization
///
/// API key authentication with HMAC-SHA256 verification.
/// Keys are stored as BLAKE3 hashes — plaintext never persisted.
///
/// Wire protocol: First frame after connect must be OP_AUTH with the API key.
/// HTTP: X-Api-Key header on every request.
///
/// No auth configured → open access (dev mode).
const std = @import("std");
const crypto = @import("crypto.zig");
const Allocator = std.mem.Allocator;

pub const MAX_KEYS = 64;

/// Permission level for an API key.
pub const Permission = enum(u8) {
    read_only = 0,
    read_write = 1,
    admin = 2,
};

/// A registered API key (stored as hash, never plaintext).
pub const KeyEntry = struct {
    hash: [32]u8, // BLAKE3 of the raw key
    name: [64]u8,
    name_len: u8,
    tenant_id: [64]u8,
    tenant_id_len: u8,
    perm: Permission,
};

pub const AuthContext = struct {
    perm: Permission,
    tenant_id: [64]u8,
    tenant_id_len: u8,

    pub fn tenant(self: *const AuthContext) []const u8 {
        return self.tenant_id[0..self.tenant_id_len];
    }
};

/// Auth store. Thread-safe via RwLock.
pub const AuthStore = struct {
    keys: [MAX_KEYS]KeyEntry = undefined,
    count: u32 = 0,
    enabled: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),
    lock: std.Thread.RwLock = .{},

    /// Add an API key. Returns the BLAKE3 hash if added, null if at MAX_KEYS.
    pub fn addKey(self: *AuthStore, raw_key: []const u8, name: []const u8, perm: Permission) ?[32]u8 {
        return self.addKeyForTenant(raw_key, name, "default", perm);
    }

    /// Add an API key for a specific tenant. Returns the BLAKE3 hash if added,
    /// null if the key store is full (MAX_KEYS reached).
    pub fn addKeyForTenant(self: *AuthStore, raw_key: []const u8, name: []const u8, tenant_id: []const u8, perm: Permission) ?[32]u8 {
        self.lock.lock();
        defer self.lock.unlock();

        if (self.count >= MAX_KEYS) return null;

        const hash = crypto.blake3(raw_key);
        var entry = KeyEntry{
            .hash = hash,
            .name = undefined,
            .name_len = @intCast(@min(name.len, 64)),
            .tenant_id = undefined,
            .tenant_id_len = @intCast(@min(tenant_id.len, 64)),
            .perm = perm,
        };
        @memcpy(entry.name[0..entry.name_len], name[0..entry.name_len]);
        @memcpy(entry.tenant_id[0..entry.tenant_id_len], tenant_id[0..entry.tenant_id_len]);
        self.keys[self.count] = entry;
        self.count += 1;
        self.enabled.store(true, .release);
        return hash;
    }

    /// Verify an API key. Returns the Permission if valid, null if rejected.
    pub fn verify(self: *AuthStore, raw_key: []const u8) ?Permission {
        const ctx = self.resolve(raw_key) orelse return null;
        return ctx.perm;
    }

    pub fn resolve(self: *AuthStore, raw_key: []const u8) ?AuthContext {
        // Fast-path: if no keys have been added, skip hashing and locking.
        // Uses acquire to see the latest store from addKeyForTenant.
        if (!self.enabled.load(.acquire)) {
            return AuthContext{
                .perm = .admin,
                .tenant_id = [_]u8{0} ** 64,
                .tenant_id_len = 0,
            };
        }
        const hash = crypto.blake3(raw_key);

        self.lock.lockShared();
        defer self.lock.unlockShared();

        for (self.keys[0..self.count]) |*entry| {
            if (std.mem.eql(u8, &entry.hash, &hash)) {
                return AuthContext{
                    .perm = entry.perm,
                    .tenant_id = entry.tenant_id,
                    .tenant_id_len = entry.tenant_id_len,
                };
            }
        }
        return null;
    }

    /// Check if auth is enabled.
    pub fn isEnabled(self: *AuthStore) bool {
        return self.enabled.load(.acquire);
    }

    /// Extract API key from HTTP headers.
    pub fn extractHttpKey(request: []const u8) ?[]const u8 {
        return extractHeader(request, "X-Api-Key: ");
    }

    pub fn extractHttpTenant(request: []const u8) ?[]const u8 {
        return extractHeader(request, "X-Tenant-Id: ");
    }

    fn extractHeader(request: []const u8, needle: []const u8) ?[]const u8 {
        const pos = std.mem.indexOf(u8, request, needle) orelse return null;
        const start = pos + needle.len;
        const end = std.mem.indexOfScalarPos(u8, request, start, '\r') orelse
            std.mem.indexOfScalarPos(u8, request, start, '\n') orelse request.len;
        const key = request[start..end];
        return if (key.len > 0) key else null;
    }
};

// ── Wire protocol auth ──────────────────────────────────────────────────────

pub const OP_AUTH: u8 = 0x10;
pub const STATUS_UNAUTHORIZED: u8 = 0x03;

// ── Tests ────────────────────────────────────────────────────────────────────

test "auth disabled returns admin" {
    var store = AuthStore{};
    try std.testing.expectEqual(Permission.admin, store.verify("anything").?);
}

test "add and verify key" {
    var store = AuthStore{};
    _ = store.addKey("my-secret-key", "test-key", .read_write);
    try std.testing.expectEqual(Permission.read_write, store.verify("my-secret-key").?);
    try std.testing.expectEqual(@as(?Permission, null), store.verify("wrong-key"));
}

test "read-only key cannot write" {
    var store = AuthStore{};
    _ = store.addKey("reader", "reader", .read_only);
    const perm = store.verify("reader").?;
    try std.testing.expect(perm == .read_only);
}

test "tenant key resolves tenant context" {
    var store = AuthStore{};
    _ = store.addKeyForTenant("tenant-reader", "reader", "customer-a", .read_only);
    const ctx = store.resolve("tenant-reader").?;
    try std.testing.expectEqual(Permission.read_only, ctx.perm);
    try std.testing.expectEqualStrings("customer-a", ctx.tenant());
}

test "extract HTTP key" {
    const req = "GET /db/users HTTP/1.1\r\nX-Api-Key: abc123\r\nHost: localhost\r\n\r\n";
    const key = AuthStore.extractHttpKey(req).?;
    try std.testing.expectEqualStrings("abc123", key);
}

test "extract HTTP tenant" {
    const req = "GET /db/users HTTP/1.1\r\nX-Tenant-Id: customer-a\r\nHost: localhost\r\n\r\n";
    const tenant = AuthStore.extractHttpTenant(req).?;
    try std.testing.expectEqualStrings("customer-a", tenant);
}

test "extract HTTP key missing" {
    const req = "GET /db/users HTTP/1.1\r\nHost: localhost\r\n\r\n";
    try std.testing.expectEqual(@as(?[]const u8, null), AuthStore.extractHttpKey(req));
}
