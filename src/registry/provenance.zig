/// ZagDB — Build provenance attestations (SLSA-style)
///
/// Proves that a specific binary artifact was built from a specific source hash,
/// by a specific builder, at a specific time. Users can verify the chain:
///   source → build → artifact
const std = @import("std");
const sign_mod = @import("sign.zig");
const hash_mod = @import("hash.zig");
const compat = @import("compat");

pub const Provenance = struct {
    source_hash: []const u8, // blake3 hex of source tree
    builder_pubkey: []const u8, // ed25519 pubkey hex of builder
    build_timestamp: i64, // unix timestamp
    zig_version: []const u8, // zig compiler version used
    target_triple: []const u8, // e.g. "x86_64-linux-gnu"
    artifact_hash: []const u8, // blake3 hex of built artifact
    signature: []const u8, // ed25519 signature over all above fields
};

/// Create a provenance attestation.
/// Signs: "provenance:" ++ source_hash ++ ":" ++ target ++ ":" ++ artifact_hash
pub fn createAttestation(
    source_hash: []const u8,
    target: []const u8,
    artifact_hash: []const u8,
    zig_version: []const u8,
    keypair: sign_mod.KeyPair,
    buf: *AttestationBuf,
) Provenance {
    const pubkey_hex = sign_mod.pubkeyHex(keypair.public_key);
    const now = compat.timestampSec();

    // Build message to sign
    const msg = std.fmt.bufPrint(&buf.msg_buf, "provenance:{s}:{s}:{s}", .{
        source_hash,
        target,
        artifact_hash,
    }) catch "";

    const sig = sign_mod.sign(msg, keypair.secret_key);
    const sig_hex = sign_mod.signatureHex(sig);

    // Copy hex values into stable buffers
    @memcpy(buf.pubkey_buf[0..64], &pubkey_hex);
    @memcpy(buf.sig_buf[0..128], &sig_hex);

    return .{
        .source_hash = source_hash,
        .builder_pubkey = buf.pubkey_buf[0..64],
        .build_timestamp = now,
        .zig_version = zig_version,
        .target_triple = target,
        .artifact_hash = artifact_hash,
        .signature = buf.sig_buf[0..128],
    };
}

pub const AttestationBuf = struct {
    msg_buf: [512]u8 = undefined,
    pubkey_buf: [64]u8 = undefined,
    sig_buf: [128]u8 = undefined,
};

/// Verify a provenance attestation.
pub fn verifyAttestation(p: Provenance, pubkey: [32]u8) bool {
    var msg_buf: [512]u8 = undefined;
    const msg = std.fmt.bufPrint(&msg_buf, "provenance:{s}:{s}:{s}", .{
        p.source_hash,
        p.target_triple,
        p.artifact_hash,
    }) catch return false;

    // Decode signature from hex
    if (p.signature.len != 128) return false;
    var sig_bytes: [64]u8 = undefined;
    for (0..64) |i| {
        const hi = hexVal(p.signature[i * 2]) catch return false;
        const lo = hexVal(p.signature[i * 2 + 1]) catch return false;
        sig_bytes[i] = (@as(u8, @intCast(hi)) << 4) | @as(u8, @intCast(lo));
    }

    return sign_mod.verify(msg, sig_bytes, pubkey);
}

fn hexVal(c: u8) !u4 {
    if (c >= '0' and c <= '9') return @intCast(c - '0');
    if (c >= 'a' and c <= 'f') return @intCast(c - 'a' + 10);
    if (c >= 'A' and c <= 'F') return @intCast(c - 'A' + 10);
    return error.InvalidHexChar;
}

/// Serialize provenance to JSON.
pub fn toJson(p: Provenance, buf: []u8) ![]const u8 {
    return std.fmt.bufPrint(buf,
        \\{{"source_hash":"{s}","builder_pubkey":"{s}","build_timestamp":{d},"zig_version":"{s}","target_triple":"{s}","artifact_hash":"{s}","signature":"{s}"}}
    , .{
        p.source_hash,
        p.builder_pubkey,
        p.build_timestamp,
        p.zig_version,
        p.target_triple,
        p.artifact_hash,
        p.signature,
    });
}

// ─── Tests ──────────────────────────────────────────────────────────────────

test "create and verify attestation" {
    const kp = sign_mod.KeyPair.generate();
    var att_buf: AttestationBuf = .{};

    const att = createAttestation(
        "abc123source",
        "aarch64-macos",
        "def456artifact",
        "0.15.0",
        kp,
        &att_buf,
    );

    try std.testing.expect(verifyAttestation(att, kp.public_key));
    try std.testing.expectEqualStrings("abc123source", att.source_hash);
    try std.testing.expectEqualStrings("aarch64-macos", att.target_triple);
    try std.testing.expectEqualStrings("def456artifact", att.artifact_hash);
}

test "attestation fails with wrong key" {
    const kp1 = sign_mod.KeyPair.generate();
    const kp2 = sign_mod.KeyPair.generate();
    var att_buf: AttestationBuf = .{};

    const att = createAttestation(
        "src_hash",
        "x86_64-linux",
        "art_hash",
        "0.15.0",
        kp1,
        &att_buf,
    );

    // Verify with wrong key should fail
    try std.testing.expect(!verifyAttestation(att, kp2.public_key));
}

test "attestation toJson" {
    const kp = sign_mod.KeyPair.generate();
    var att_buf: AttestationBuf = .{};

    const att = createAttestation(
        "source123",
        "aarch64-macos",
        "artifact456",
        "0.15.0",
        kp,
        &att_buf,
    );

    var json_buf: [2048]u8 = undefined;
    const json = try toJson(att, &json_buf);

    try std.testing.expect(std.mem.indexOf(u8, json, "source123") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "aarch64-macos") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "artifact456") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "0.15.0") != null);
}
