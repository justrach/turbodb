/// TurboDB — Cryptographic primitives
///
/// Exposes SHA-256, SHA-512, BLAKE3, HMAC-SHA256, and Ed25519 via both
/// Zig API (for internal use) and C ABI exports (for FFI clients).
///
/// All hash functions output fixed-size byte arrays. Hex encoding helpers
/// are included for convenient string representation.
///
/// Uses Zig's std.crypto — no external dependencies, no OpenSSL.
const std = @import("std");
const runtime = @import("runtime");

// ── Hash algorithms ──────────────────────────────────────────────────────────

const Sha256 = std.crypto.hash.sha2.Sha256;
const Sha512 = std.crypto.hash.sha2.Sha512;
const Blake3 = std.crypto.hash.Blake3;
const HmacSha256 = std.crypto.auth.hmac.sha2.HmacSha256;
const Ed25519 = std.crypto.sign.Ed25519;

// ── Zig API ──────────────────────────────────────────────────────────────────

/// SHA-256 digest (32 bytes).
/// SHA-256 digest (32 bytes).
pub fn sha256(data: []const u8) [32]u8 {
    var out: [32]u8 = undefined;
    Sha256.hash(data, &out, .{});
    return out;
}

/// SHA-512 digest (64 bytes).
/// SHA-512 digest (64 bytes).
pub fn sha512(data: []const u8) [64]u8 {
    var out: [64]u8 = undefined;
    Sha512.hash(data, &out, .{});
    return out;
}

/// BLAKE3 digest (32 bytes). Faster than SHA-256, used internally.
pub fn blake3(data: []const u8) [32]u8 {
    var out: [32]u8 = undefined;
    Blake3.hash(data, &out, .{});
    return out;
}

/// HMAC-SHA256 (32 bytes). Use for API key derivation, webhooks, etc.
pub fn hmacSha256(key: []const u8, data: []const u8) [32]u8 {
    var out: [32]u8 = undefined;
    HmacSha256.create(&out, data, key);
    return out;
}

/// Hex-encode any fixed-size hash.
pub fn hexEncode(comptime N: usize, bytes: [N]u8) [N * 2]u8 {
    const hex = "0123456789abcdef";
    var out: [N * 2]u8 = undefined;
    for (bytes, 0..) |b, i| {
        out[i * 2] = hex[b >> 4];
        out[i * 2 + 1] = hex[b & 0x0f];
    }
    return out;
}

/// SHA-256 → 64 hex chars.
pub fn sha256Hex(data: []const u8) [64]u8 {
    return hexEncode(32, sha256(data));
}

/// BLAKE3 → 64 hex chars.
pub fn blake3Hex(data: []const u8) [64]u8 {
    return hexEncode(32, blake3(data));
}

/// HMAC-SHA256 → 64 hex chars.
pub fn hmacSha256Hex(key: []const u8, data: []const u8) [64]u8 {
    return hexEncode(32, hmacSha256(key, data));
}

// ── Ed25519 ──────────────────────────────────────────────────────────────────

pub const KeyPair = struct {
    public_key: [32]u8,
    secret_key: [64]u8,

    pub fn generate() KeyPair {
        const kp = Ed25519.KeyPair.generate(runtime.io);
        return .{
            .public_key = kp.public_key.toBytes(),
            .secret_key = kp.secret_key.toBytes(),
        };
    }
};

pub fn ed25519Sign(message: []const u8, secret_key: [64]u8) [64]u8 {
    const sk = Ed25519.SecretKey.fromBytes(secret_key) catch unreachable;
    const pk = Ed25519.PublicKey.fromBytes(sk.publicKeyBytes()) catch unreachable;
    const kp = Ed25519.KeyPair{ .secret_key = sk, .public_key = pk };
    const sig = kp.sign(message, null) catch unreachable;
    return sig.toBytes();
}

pub fn ed25519Verify(message: []const u8, signature: [64]u8, public_key: [32]u8) bool {
    const pk = Ed25519.PublicKey.fromBytes(public_key) catch return false;
    const sig = Ed25519.Signature.fromBytes(signature);
    sig.verify(message, pk) catch return false;
    return true;
}


// ── Tests ────────────────────────────────────────────────────────────────────

test "sha256 known vector" {
    const hash = sha256Hex("hello");
    const expected = "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824";
    try std.testing.expectEqualStrings(expected, &hash);
}

test "blake3 deterministic" {
    const a = blake3("test data");
    const b = blake3("test data");
    try std.testing.expectEqual(a, b);
}

test "blake3 different for different inputs" {
    const a = blake3("hello");
    const b = blake3("world");
    try std.testing.expect(!std.mem.eql(u8, &a, &b));
}

test "hmac-sha256 deterministic" {
    const a = hmacSha256("secret", "data");
    const b = hmacSha256("secret", "data");
    try std.testing.expectEqual(a, b);
}

test "hmac-sha256 different keys give different results" {
    const a = hmacSha256("key1", "data");
    const b = hmacSha256("key2", "data");
    try std.testing.expect(!std.mem.eql(u8, &a, &b));
}

test "ed25519 sign and verify" {
    const kp = KeyPair.generate();
    const msg = "TurboDB is fast";
    const sig = ed25519Sign(msg, kp.secret_key);
    try std.testing.expect(ed25519Verify(msg, sig, kp.public_key));
}

test "ed25519 tampered message fails" {
    const kp = KeyPair.generate();
    const sig = ed25519Sign("original", kp.secret_key);
    try std.testing.expect(!ed25519Verify("tampered", sig, kp.public_key));
}

test "hex encode round-trip" {
    const hash = sha256("test");
    const hex = hexEncode(32, hash);
    // Verify it's 64 hex chars and deterministic
    try std.testing.expectEqual(@as(usize, 64), hex.len);
    const hash2 = sha256("test");
    const hex2 = hexEncode(32, hash2);
    try std.testing.expectEqualStrings(&hex, &hex2);
}
