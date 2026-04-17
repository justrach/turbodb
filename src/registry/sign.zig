/// ZagDB — Ed25519 package signing and verification
///
/// Every published package is signed by the author's Ed25519 key.
/// The signing target is the BLAKE3 hash of the source tarball.
/// Keypairs are stored at ~/.zag/keys/
const std = @import("std");
const compat = @import("compat");
const runtime = @import("runtime");
const Ed25519 = std.crypto.sign.Ed25519;

pub const KeyPair = struct {
    public_key: [32]u8,
    secret_key: [64]u8,

    /// Generate a new random Ed25519 keypair.
    pub fn generate() KeyPair {
        const kp = Ed25519.KeyPair.generate(runtime.io);
        return .{
            .public_key = kp.public_key.toBytes(),
            .secret_key = kp.secret_key.toBytes(),
        };
    }

    /// Reconstruct keypair from a secret key.
    pub fn fromSecretKey(sk: [64]u8) !KeyPair {
        const secret = try Ed25519.SecretKey.fromBytes(sk);
        const kp = try Ed25519.KeyPair.fromSecretKey(secret);
        return .{
            .public_key = kp.public_key.toBytes(),
            .secret_key = kp.secret_key.toBytes(),
        };
    }
};

/// Sign a message with a secret key. Returns 64-byte signature.
pub fn sign(message: []const u8, secret_key_bytes: [64]u8) [64]u8 {
    const sk = Ed25519.SecretKey.fromBytes(secret_key_bytes) catch unreachable;
    const pk = Ed25519.PublicKey.fromBytes(sk.publicKeyBytes()) catch unreachable;
    const kp = Ed25519.KeyPair{ .secret_key = sk, .public_key = pk };
    const sig = kp.sign(message, null) catch unreachable;
    return sig.toBytes();
}

/// Verify a signature against a public key.
pub fn verify(message: []const u8, signature: [64]u8, public_key: [32]u8) bool {
    const pk = Ed25519.PublicKey.fromBytes(public_key) catch return false;
    const sig = Ed25519.Signature.fromBytes(signature);
    sig.verify(message, pk) catch return false;
    return true;
}

// ─── Key file I/O ───────────────────────────────────────────────────────────

const hex_chars = "0123456789abcdef";

pub fn hexEncode32(bytes: [32]u8, out: *[64]u8) void {
    for (bytes, 0..) |b, i| {
        out[i * 2] = hex_chars[b >> 4];
        out[i * 2 + 1] = hex_chars[b & 0x0f];
    }
}

fn hexEncode64(bytes: [64]u8, out: *[128]u8) void {
    for (bytes, 0..) |b, i| {
        out[i * 2] = hex_chars[b >> 4];
        out[i * 2 + 1] = hex_chars[b & 0x0f];
    }
}

fn hexVal(c: u8) !u4 {
    if (c >= '0' and c <= '9') return @intCast(c - '0');
    if (c >= 'a' and c <= 'f') return @intCast(c - 'a' + 10);
    if (c >= 'A' and c <= 'F') return @intCast(c - 'A' + 10);
    return error.InvalidHexChar;
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

/// Save keypair to directory as hex-encoded files.
/// Creates: <dir>/default.pub (64 hex chars) and <dir>/default.sec (128 hex chars)
pub fn saveKeyPair(kp: KeyPair, dir_path: []const u8) !void {
    // Ensure directory exists
    compat.fs.cwdMakePath(dir_path) catch {};

    var dir = try compat.fs.cwdOpenDir(dir_path, .{});
    defer dir.close(runtime.io);

    // Write public key
    {
        var hex: [64]u8 = undefined;
        hexEncode32(kp.public_key, &hex);
        const file = try dir.createFile(runtime.io, "default.pub", .{});
        defer file.close(runtime.io);
        try file.writeStreamingAll(runtime.io, &hex);
        try file.writeStreamingAll(runtime.io, "\n");
    }

    // Write secret key
    {
        var hex: [128]u8 = undefined;
        hexEncode64(kp.secret_key, &hex);
        const file = try dir.createFile(runtime.io, "default.sec", .{});
        defer file.close(runtime.io);
        try file.writeStreamingAll(runtime.io, &hex);
        try file.writeStreamingAll(runtime.io, "\n");
    }
}

/// Load keypair from directory.
pub fn loadKeyPair(dir_path: []const u8) !KeyPair {
    var dir = try compat.fs.cwdOpenDir(dir_path, .{});
    defer dir.close(runtime.io);

    // Read public key
    var pub_buf: [128]u8 = undefined;
    const pub_len = blk: {
        const file = try dir.openFile(runtime.io, "default.pub", .{});
        defer file.close(runtime.io);
        const n = try compat.fs.fileReadAll(file, &pub_buf);
        break :blk n;
    };
    // Trim trailing newline
    var pub_hex = pub_buf[0..pub_len];
    while (pub_hex.len > 0 and (pub_hex[pub_hex.len - 1] == '\n' or pub_hex[pub_hex.len - 1] == '\r'))
        pub_hex = pub_hex[0 .. pub_hex.len - 1];

    // Read secret key
    var sec_buf: [256]u8 = undefined;
    const sec_len = blk: {
        const file = try dir.openFile(runtime.io, "default.sec", .{});
        defer file.close(runtime.io);
        const n = try compat.fs.fileReadAll(file, &sec_buf);
        break :blk n;
    };
    var sec_hex = sec_buf[0..sec_len];
    while (sec_hex.len > 0 and (sec_hex[sec_hex.len - 1] == '\n' or sec_hex[sec_hex.len - 1] == '\r'))
        sec_hex = sec_hex[0 .. sec_hex.len - 1];

    return .{
        .public_key = try hexDecode32(pub_hex),
        .secret_key = try hexDecode64(sec_hex),
    };
}

/// Format public key as 64-char hex string.
pub fn pubkeyHex(pk: [32]u8) [64]u8 {
    var out: [64]u8 = undefined;
    hexEncode32(pk, &out);
    return out;
}

/// Format signature as 128-char hex string.
pub fn signatureHex(sig: [64]u8) [128]u8 {
    var out: [128]u8 = undefined;
    hexEncode64(sig, &out);
    return out;
}

// ─── Tests ──────────────────────────────────────────────────────────────────

test "generate and verify" {
    const kp = KeyPair.generate();
    const message = "hello zag packages";
    const sig = sign(message, kp.secret_key);
    try std.testing.expect(verify(message, sig, kp.public_key));
}

test "tampered message fails" {
    const kp = KeyPair.generate();
    const sig = sign("original", kp.secret_key);
    try std.testing.expect(!verify("tampered", sig, kp.public_key));
}

test "wrong key fails" {
    const kp1 = KeyPair.generate();
    const kp2 = KeyPair.generate();
    const sig = sign("message", kp1.secret_key);
    try std.testing.expect(!verify("message", sig, kp2.public_key));
}

test "reconstruct from secret key" {
    const kp1 = KeyPair.generate();
    const kp2 = try KeyPair.fromSecretKey(kp1.secret_key);
    try std.testing.expectEqual(kp1.public_key, kp2.public_key);
}

test "save and load keypair" {
    const kp = KeyPair.generate();
    const tmp_dir = "/tmp/zag-test-keys";

    // Clean up from previous runs
    compat.fs.cwdDeleteTree(tmp_dir) catch {};

    try saveKeyPair(kp, tmp_dir);
    const loaded = try loadKeyPair(tmp_dir);

    try std.testing.expectEqual(kp.public_key, loaded.public_key);
    try std.testing.expectEqual(kp.secret_key, loaded.secret_key);

    // Verify the loaded key can sign/verify
    const sig = sign("test", loaded.secret_key);
    try std.testing.expect(verify("test", sig, loaded.public_key));

    // Clean up
    compat.fs.cwdDeleteTree(tmp_dir) catch {};
}

test "pubkey and signature hex" {
    const kp = KeyPair.generate();
    const pk_hex = pubkeyHex(kp.public_key);
    const sig = sign("msg", kp.secret_key);
    const sig_hex = signatureHex(sig);

    // Should be valid hex
    for (pk_hex) |c| {
        try std.testing.expect((c >= '0' and c <= '9') or (c >= 'a' and c <= 'f'));
    }
    for (sig_hex) |c| {
        try std.testing.expect((c >= '0' and c <= '9') or (c >= 'a' and c <= 'f'));
    }
}
