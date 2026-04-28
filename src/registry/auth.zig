/// ZagDB — Authentication & access control
///
/// Provides:
///   - Visibility enum (public/private)
///   - AuthContext from HTTP headers (X-Zag-Pubkey + X-Zag-Signature)
///   - Access grants (per-package, per-pubkey permissions)
///   - Request signing/verification (Ed25519 over method+path+timestamp)
const std = @import("std");
const compat = @import("compat");
const sign_mod = @import("sign.zig");

// ─── Visibility ────────────────────────────────────────────────────────────────

pub const Visibility = enum {
    public,
    private,

    pub fn fromStr(s: []const u8) Visibility {
        if (std.mem.eql(u8, s, "private")) return .private;
        return .public;
    }

    pub fn toStr(self: Visibility) []const u8 {
        return switch (self) {
            .public => "public",
            .private => "private",
        };
    }
};

// ─── Permissions ───────────────────────────────────────────────────────────────

pub const Permissions = struct {
    read: bool = false,
    publish: bool = false,
    yank: bool = false,
    admin: bool = false, // can grant/revoke access

    pub const owner: Permissions = .{ .read = true, .publish = true, .yank = true, .admin = true };
    pub const reader: Permissions = .{ .read = true };
    pub const publisher: Permissions = .{ .read = true, .publish = true };

    pub fn hasRead(self: Permissions) bool {
        return self.read;
    }
    pub fn hasPublish(self: Permissions) bool {
        return self.publish;
    }
    pub fn hasYank(self: Permissions) bool {
        return self.yank;
    }
    pub fn hasAdmin(self: Permissions) bool {
        return self.admin;
    }

    /// Serialize to JSON-friendly string: "read,publish,yank,admin"
    pub fn toStr(self: Permissions, buf: []u8) ![]const u8 {
        var fbs = compat.fixedBufferStream(buf);
        const w = fbs.writer();
        var first = true;
        if (self.read) { if (!first) try w.writeAll(","); try w.writeAll("read"); first = false; }
        if (self.publish) { if (!first) try w.writeAll(","); try w.writeAll("publish"); first = false; }
        if (self.yank) { if (!first) try w.writeAll(","); try w.writeAll("yank"); first = false; }
        if (self.admin) { if (!first) try w.writeAll(","); try w.writeAll("admin"); first = false; }
        return fbs.getWritten();
    }

    /// Parse from comma-separated string
    pub fn fromStr(s: []const u8) Permissions {
        var p = Permissions{};
        var iter = std.mem.splitScalar(u8, s, ',');
        while (iter.next()) |part| {
            if (std.mem.eql(u8, part, "read")) p.read = true;
            if (std.mem.eql(u8, part, "publish")) p.publish = true;
            if (std.mem.eql(u8, part, "yank")) p.yank = true;
            if (std.mem.eql(u8, part, "admin")) p.admin = true;
        }
        return p;
    }
};

// ─── Auth Context ──────────────────────────────────────────────────────────────

pub const AuthContext = struct {
    pubkey: ?[32]u8 = null,
    pubkey_hex: ?[64]u8 = null,
    authenticated: bool = false,

    pub const anonymous: AuthContext = .{};

    pub fn isAuthenticated(self: AuthContext) bool {
        return self.authenticated and self.pubkey != null;
    }
};

/// Parse auth from raw HTTP headers.
/// Looks for:
///   X-Zag-Pubkey: <64 hex chars>
///   X-Zag-Signature: <128 hex chars>
///   X-Zag-Timestamp: <unix timestamp>
pub fn parseAuth(raw_headers: []const u8, method: []const u8, path: []const u8) AuthContext {
    const pubkey_hex = getHeader(raw_headers, "X-Zag-Pubkey") orelse return AuthContext.anonymous;
    const sig_hex = getHeader(raw_headers, "X-Zag-Signature") orelse return AuthContext.anonymous;
    const ts_str = getHeader(raw_headers, "X-Zag-Timestamp") orelse return AuthContext.anonymous;

    if (pubkey_hex.len != 64 or sig_hex.len != 128) return AuthContext.anonymous;

    // Decode pubkey
    const pubkey = hexDecode32(pubkey_hex) catch return AuthContext.anonymous;

    // Decode signature
    const sig = hexDecode64(sig_hex) catch return AuthContext.anonymous;

    // Parse timestamp
    const timestamp = std.fmt.parseInt(i64, ts_str, 10) catch return AuthContext.anonymous;

    // Verify: signature must sign "METHOD PATH TIMESTAMP"
    if (!verifyRequest(method, path, timestamp, sig, pubkey)) return AuthContext.anonymous;

    var pk_hex: [64]u8 = undefined;
    sign_mod.hexEncode32(pubkey, &pk_hex);

    return .{
        .pubkey = pubkey,
        .pubkey_hex = pk_hex,
        .authenticated = true,
    };
}

// ─── Access Control ───────────────────────────────────────────────────────────

/// Check if a package is visible to the given auth context.
/// Public packages are visible to everyone.
/// Private packages require the viewer to be the owner or in the access list.
pub fn canView(pkg_json: []const u8, auth: AuthContext) bool {
    const vis_str = jsonGetField(pkg_json, "visibility") orelse return true; // default public
    if (std.mem.eql(u8, vis_str, "public")) return true;

    // Private package — must be authenticated
    if (!auth.isAuthenticated()) return false;
    const auth_hex = auth.pubkey_hex orelse return false;

    // Check if viewer is owner
    const owner = jsonGetField(pkg_json, "owner_pubkey") orelse
        jsonGetField(pkg_json, "author_pubkey") orelse return false;
    if (std.mem.eql(u8, owner, &auth_hex)) return true;

    // Check access grants
    return hasAccessGrant(pkg_json, &auth_hex, "read");
}

/// Check if a pubkey has a specific permission on a package.
pub fn checkAccess(pkg_json: []const u8, pubkey_hex: []const u8, required: []const u8) bool {
    // Owner has all permissions
    const owner = jsonGetField(pkg_json, "owner_pubkey") orelse
        jsonGetField(pkg_json, "author_pubkey") orelse return false;
    if (std.mem.eql(u8, owner, pubkey_hex)) return true;

    return hasAccessGrant(pkg_json, pubkey_hex, required);
}

/// Check if pubkey appears in access_grants with the required permission.
fn hasAccessGrant(pkg_json: []const u8, pubkey_hex: []const u8, required: []const u8) bool {
    // Search for the pubkey in the access_grants array
    // Format: "access_grants":[{"pubkey":"...","permissions":"read,publish",...}]
    const grants_start = std.mem.indexOf(u8, pkg_json, "\"access_grants\":") orelse return false;
    const arr_start = std.mem.indexOfScalarPos(u8, pkg_json, grants_start, '[') orelse return false;
    const arr_end = std.mem.indexOfScalarPos(u8, pkg_json, arr_start, ']') orelse return false;
    const grants_section = pkg_json[arr_start..arr_end + 1];

    // Look for the pubkey within the grants section
    var search_pos: usize = 0;
    while (std.mem.indexOfPos(u8, grants_section, search_pos, pubkey_hex)) |pos| {
        // Found the pubkey, now check if it has the required permission
        // Look for the permissions field in the same grant object
        const obj_start = std.mem.lastIndexOfScalar(u8, grants_section[0..pos], '{') orelse {
            search_pos = pos + 1;
            continue;
        };
        const obj_end = std.mem.indexOfScalarPos(u8, grants_section, pos, '}') orelse {
            search_pos = pos + 1;
            continue;
        };
        const grant_obj = grants_section[obj_start .. obj_end + 1];

        // Check permissions field
        if (jsonGetField(grant_obj, "permissions")) |perms| {
            if (std.mem.indexOf(u8, perms, required) != null) return true;
        }
        search_pos = pos + 1;
    }
    return false;
}

// ─── Request Signing ─────────────────────────────────────────────────────────

/// Sign an HTTP request for authentication.
/// Signs: "METHOD PATH TIMESTAMP"
pub fn signRequest(method: []const u8, path: []const u8, timestamp: i64, secret_key: [64]u8) [64]u8 {
    var msg_buf: [1024]u8 = undefined;
    const msg = std.fmt.bufPrint(&msg_buf, "{s} {s} {d}", .{ method, path, timestamp }) catch return std.mem.zeroes([64]u8);
    return sign_mod.sign(msg, secret_key);
}

/// Verify an HTTP request signature.
pub fn verifyRequest(method: []const u8, path: []const u8, timestamp: i64, signature: [64]u8, pubkey: [32]u8) bool {
    // Check timestamp is within 5 minutes
    const now = compat.timestamp();
    const diff = if (now > timestamp) now - timestamp else timestamp - now;
    if (diff > 300) return false; // 5 minute window

    var msg_buf: [1024]u8 = undefined;
    const msg = std.fmt.bufPrint(&msg_buf, "{s} {s} {d}", .{ method, path, timestamp }) catch return false;
    return sign_mod.verify(msg, signature, pubkey);
}

/// Format auth headers for CLI HTTP requests.
pub fn formatAuthHeaders(pubkey: [32]u8, signature: [64]u8, timestamp: i64, buf: []u8) ![]const u8 {
    const pk_hex = sign_mod.pubkeyHex(pubkey);
    const sig_hex = sign_mod.signatureHex(signature);
    return std.fmt.bufPrint(buf, "X-Zag-Pubkey: {s}\r\nX-Zag-Signature: {s}\r\nX-Zag-Timestamp: {d}", .{ &pk_hex, &sig_hex, timestamp });
}

// ─── Org Namespaces ──────────────────────────────────────────────────────────

/// Parse a scoped package name like "@org/package" into (org, name).
/// Unscoped names return (null, name).
pub fn parsePackageName(full_name: []const u8) struct { org: ?[]const u8, name: []const u8 } {
    if (full_name.len > 1 and full_name[0] == '@') {
        if (std.mem.indexOfScalar(u8, full_name, '/')) |sep| {
            return .{
                .org = full_name[1..sep],
                .name = full_name[sep + 1 ..],
            };
        }
    }
    return .{ .org = null, .name = full_name };
}

/// Format a package key: "@org/name" or just "name"
pub fn formatPackageKey(org: ?[]const u8, name: []const u8, buf: []u8) ![]const u8 {
    if (org) |o| {
        return std.fmt.bufPrint(buf, "@{s}/{s}", .{ o, name });
    }
    return std.fmt.bufPrint(buf, "{s}", .{name});
}

// ─── Helpers ─────────────────────────────────────────────────────────────────

fn getHeader(raw: []const u8, name: []const u8) ?[]const u8 {
    var search_buf: [128]u8 = undefined;
    const needle = std.fmt.bufPrint(&search_buf, "{s}: ", .{name}) catch return null;
    const pos = std.mem.indexOf(u8, raw, needle) orelse return null;
    const start = pos + needle.len;
    // Find end of header value (\r\n or \n)
    var end = start;
    while (end < raw.len and raw[end] != '\r' and raw[end] != '\n') end += 1;
    if (end == start) return null;
    return raw[start..end];
}

fn jsonGetField(json: []const u8, key: []const u8) ?[]const u8 {
    var search_buf: [128]u8 = undefined;
    const needle = std.fmt.bufPrint(&search_buf, "\"{s}\":", .{key}) catch return null;
    const pos = std.mem.indexOf(u8, json, needle) orelse return null;
    var i = pos + needle.len;
    while (i < json.len and (json[i] == ' ' or json[i] == '\t')) i += 1;
    if (i >= json.len) return null;
    if (json[i] == '"') {
        const s = i + 1;
        var j = s;
        while (j < json.len) : (j += 1) {
            if (json[j] == '\\') { j += 1; continue; }
            if (json[j] == '"') return json[s..j];
        }
        return null;
    }
    var end = i;
    while (end < json.len and json[end] != ',' and json[end] != '}' and
        json[end] != '\n' and json[end] != ' ') end += 1;
    return json[i..end];
}

fn hexDecode32(hex: []const u8) ![32]u8 {
    if (hex.len < 64) return error.InvalidHexLength;
    var out: [32]u8 = undefined;
    for (0..32) |i| {
        const hi: u8 = @intCast(try hexVal(hex[i * 2]));
        const lo: u8 = @intCast(try hexVal(hex[i * 2 + 1]));
        out[i] = (hi << 4) | lo;
    }
    return out;
}

fn hexDecode64(hex: []const u8) ![64]u8 {
    if (hex.len < 128) return error.InvalidHexLength;
    var out: [64]u8 = undefined;
    for (0..64) |i| {
        const hi: u8 = @intCast(try hexVal(hex[i * 2]));
        const lo: u8 = @intCast(try hexVal(hex[i * 2 + 1]));
        out[i] = (hi << 4) | lo;
    }
    return out;
}

fn hexVal(c: u8) !u4 {
    if (c >= '0' and c <= '9') return @intCast(c - '0');
    if (c >= 'a' and c <= 'f') return @intCast(c - 'a' + 10);
    if (c >= 'A' and c <= 'F') return @intCast(c - 'A' + 10);
    return error.InvalidHexChar;
}

// ─── Tests ───────────────────────────────────────────────────────────────────

test "visibility enum" {
    try std.testing.expectEqual(Visibility.public, Visibility.fromStr("public"));
    try std.testing.expectEqual(Visibility.private, Visibility.fromStr("private"));
    try std.testing.expectEqual(Visibility.public, Visibility.fromStr("unknown"));
    try std.testing.expectEqualStrings("public", Visibility.public.toStr());
    try std.testing.expectEqualStrings("private", Visibility.private.toStr());
}

test "permissions" {
    const owner = Permissions.owner;
    try std.testing.expect(owner.hasRead());
    try std.testing.expect(owner.hasPublish());
    try std.testing.expect(owner.hasYank());
    try std.testing.expect(owner.hasAdmin());

    const reader = Permissions.reader;
    try std.testing.expect(reader.hasRead());
    try std.testing.expect(!reader.hasPublish());

    var buf: [64]u8 = undefined;
    const s = try Permissions.owner.toStr(&buf);
    try std.testing.expect(std.mem.indexOf(u8, s, "read") != null);
    try std.testing.expect(std.mem.indexOf(u8, s, "admin") != null);

    const parsed = Permissions.fromStr("read,yank");
    try std.testing.expect(parsed.hasRead());
    try std.testing.expect(parsed.hasYank());
    try std.testing.expect(!parsed.hasPublish());
}

test "parse package name" {
    const scoped = parsePackageName("@myorg/router");
    try std.testing.expectEqualStrings("myorg", scoped.org.?);
    try std.testing.expectEqualStrings("router", scoped.name);

    const unscoped = parsePackageName("json-parser");
    try std.testing.expectEqual(@as(?[]const u8, null), unscoped.org);
    try std.testing.expectEqualStrings("json-parser", unscoped.name);
}

test "format package key" {
    var buf: [128]u8 = undefined;
    const scoped = try formatPackageKey("myorg", "router", &buf);
    try std.testing.expectEqualStrings("@myorg/router", scoped);

    const unscoped = try formatPackageKey(null, "json", &buf);
    try std.testing.expectEqualStrings("json", unscoped);
}

test "request signing and verification" {
    const kp = sign_mod.KeyPair.generate();
    const now = compat.timestamp();

    const sig = signRequest("GET", "/api/v1/packages/test", now, kp.secret_key);
    try std.testing.expect(verifyRequest("GET", "/api/v1/packages/test", now, sig, kp.public_key));

    // Wrong method fails
    try std.testing.expect(!verifyRequest("POST", "/api/v1/packages/test", now, sig, kp.public_key));

    // Wrong key fails
    const kp2 = sign_mod.KeyPair.generate();
    try std.testing.expect(!verifyRequest("GET", "/api/v1/packages/test", now, sig, kp2.public_key));
}

test "canView public package" {
    const public_pkg = "{\"name\":\"test\",\"visibility\":\"public\"}";
    try std.testing.expect(canView(public_pkg, AuthContext.anonymous));
}

test "canView private package" {
    const kp = sign_mod.KeyPair.generate();
    const pk_hex = sign_mod.pubkeyHex(kp.public_key);

    var pkg_buf: [512]u8 = undefined;
    const private_pkg = std.fmt.bufPrint(&pkg_buf,
        \\{{"name":"secret","visibility":"private","owner_pubkey":"{s}"}}
    , .{&pk_hex}) catch unreachable;

    // Anonymous cannot view
    try std.testing.expect(!canView(private_pkg, AuthContext.anonymous));

    // Owner can view
    const owner_ctx = AuthContext{ .pubkey = kp.public_key, .pubkey_hex = pk_hex, .authenticated = true };
    try std.testing.expect(canView(private_pkg, owner_ctx));

    // Other user cannot view
    const kp2 = sign_mod.KeyPair.generate();
    const other_ctx = AuthContext{ .pubkey = kp2.public_key, .pubkey_hex = sign_mod.pubkeyHex(kp2.public_key), .authenticated = true };
    try std.testing.expect(!canView(private_pkg, other_ctx));
}

test "checkAccess owner has all" {
    const kp = sign_mod.KeyPair.generate();
    const pk_hex = sign_mod.pubkeyHex(kp.public_key);

    var pkg_buf: [512]u8 = undefined;
    const pkg = std.fmt.bufPrint(&pkg_buf,
        \\{{"name":"test","owner_pubkey":"{s}"}}
    , .{&pk_hex}) catch unreachable;

    try std.testing.expect(checkAccess(pkg, &pk_hex, "read"));
    try std.testing.expect(checkAccess(pkg, &pk_hex, "publish"));
    try std.testing.expect(checkAccess(pkg, &pk_hex, "yank"));
    try std.testing.expect(checkAccess(pkg, &pk_hex, "admin"));
}

test "checkAccess with grants" {
    const kp_owner = sign_mod.KeyPair.generate();
    const kp_reader = sign_mod.KeyPair.generate();
    const owner_hex = sign_mod.pubkeyHex(kp_owner.public_key);
    const reader_hex = sign_mod.pubkeyHex(kp_reader.public_key);

    var pkg_buf: [1024]u8 = undefined;
    const pkg = std.fmt.bufPrint(&pkg_buf,
        \\{{"name":"test","owner_pubkey":"{s}","access_grants":[{{"pubkey":"{s}","permissions":"read"}}]}}
    , .{ &owner_hex, &reader_hex }) catch unreachable;

    // Reader has read access
    try std.testing.expect(checkAccess(pkg, &reader_hex, "read"));
    // Reader does NOT have publish access
    try std.testing.expect(!checkAccess(pkg, &reader_hex, "publish"));
}

test "format auth headers" {
    const kp = sign_mod.KeyPair.generate();
    const sig = signRequest("GET", "/test", 1234567890, kp.secret_key);
    var buf: [512]u8 = undefined;
    const headers = try formatAuthHeaders(kp.public_key, sig, 1234567890, &buf);
    try std.testing.expect(std.mem.indexOf(u8, headers, "X-Zag-Pubkey:") != null);
    try std.testing.expect(std.mem.indexOf(u8, headers, "X-Zag-Signature:") != null);
    try std.testing.expect(std.mem.indexOf(u8, headers, "X-Zag-Timestamp: 1234567890") != null);
}
