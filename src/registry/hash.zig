/// ZagDB — BLAKE3 content-addressed hashing
///
/// Deterministic source tree hashing:
///   1. Walk directory, collect all file paths
///   2. Sort paths alphabetically
///   3. For each file: BLAKE3(relative_path ++ "\x00" ++ file_contents)
///   4. Sort per-file hashes
///   5. Final BLAKE3 pass over sorted hashes → Merkle root
///
/// Same source tree ALWAYS produces same hash regardless of filesystem ordering.
const std = @import("std");
const compat = @import("compat");
const runtime = @import("runtime");
const Blake3 = std.crypto.hash.Blake3;

/// Hash raw bytes with BLAKE3. Returns 32-byte digest.
pub fn hashBytes(data: []const u8) [32]u8 {
    var out: [32]u8 = undefined;
    Blake3.hash(data, &out, .{});
    return out;
}

/// Hash an entire source tree deterministically.
/// Walks directory recursively, sorts entries, computes Merkle root.
pub fn hashSourceTree(alloc: std.mem.Allocator, dir_path: []const u8) !struct { hash: [32]u8 } {
    var paths: std.ArrayList([]const u8) = .empty;
    defer {
        for (paths.items) |p| alloc.free(p);
        paths.deinit(alloc);
    }

    // Collect all file paths recursively
    try collectFiles(alloc, dir_path, "", &paths);

    // Sort paths alphabetically for determinism
    std.mem.sort([]const u8, paths.items, {}, struct {
        fn lessThan(_: void, a: []const u8, b: []const u8) bool {
            return std.mem.order(u8, a, b) == .lt;
        }
    }.lessThan);

    // Hash each file: BLAKE3(relative_path ++ \0 ++ content)
    var file_hashes: std.ArrayList([32]u8) = .empty;
    defer file_hashes.deinit(alloc);

    for (paths.items) |rel_path| {
        var full_path_buf: [4096]u8 = undefined;
        const full_path = std.fmt.bufPrint(&full_path_buf, "{s}/{s}", .{ dir_path, rel_path }) catch continue;

        const content = compat.fs.cwdReadFileAlloc(alloc, full_path, 64 * 1024 * 1024) catch continue;
        defer alloc.free(content);

        // BLAKE3(path ++ \0 ++ content)
        var hasher = Blake3.init(.{});
        hasher.update(rel_path);
        hasher.update(&[_]u8{0});
        hasher.update(content);
        var file_hash: [32]u8 = undefined;
        hasher.final(&file_hash);
        try file_hashes.append(alloc, file_hash);
    }

    // Sort file hashes for determinism
    std.mem.sort([32]u8, file_hashes.items, {}, struct {
        fn lessThan(_: void, a: [32]u8, b: [32]u8) bool {
            return std.mem.order(u8, &a, &b) == .lt;
        }
    }.lessThan);

    // Final Merkle root: BLAKE3(all sorted hashes concatenated)
    var root_hasher = Blake3.init(.{});
    for (file_hashes.items) |fh| {
        root_hasher.update(&fh);
    }
    var root: [32]u8 = undefined;
    root_hasher.final(&root);

    return .{ .hash = root };
}

fn collectFiles(
    alloc: std.mem.Allocator,
    base_dir: []const u8,
    prefix: []const u8,
    paths: *std.ArrayList([]const u8),
) !void {
    var path_buf: [4096]u8 = undefined;
    const dir_to_open = if (prefix.len > 0)
        std.fmt.bufPrint(&path_buf, "{s}/{s}", .{ base_dir, prefix }) catch return
    else
        base_dir;

    var dir = compat.fs.cwdOpenDir(dir_to_open, .{ .iterate = true }) catch return;
    defer compat.fs.dirClose(dir);

    var iter = dir.iterate();
    while (try iter.next(runtime.io)) |entry| {
        // Skip hidden files and common non-source dirs
        if (entry.name.len > 0 and entry.name[0] == '.') continue;
        if (std.mem.eql(u8, entry.name, "zig-out")) continue;
        if (std.mem.eql(u8, entry.name, "zig-cache")) continue;
        if (std.mem.eql(u8, entry.name, ".zig-cache")) continue;
        if (std.mem.eql(u8, entry.name, "node_modules")) continue;

        var rel_buf: [4096]u8 = undefined;
        const rel_path = if (prefix.len > 0)
            std.fmt.bufPrint(&rel_buf, "{s}/{s}", .{ prefix, entry.name }) catch continue
        else
            std.fmt.bufPrint(&rel_buf, "{s}", .{entry.name}) catch continue;

        switch (entry.kind) {
            .file => {
                const owned = try alloc.dupe(u8, rel_path);
                try paths.append(alloc, owned);
            },
            .directory => {
                try collectFiles(alloc, base_dir, rel_path, paths);
            },
            else => {},
        }
    }
}

// ─── Hex encoding/decoding ──────────────────────────────────────────────────

const hex_chars = "0123456789abcdef";

/// Format a 32-byte hash as 64-char lowercase hex.
pub fn hexEncode(hash: [32]u8, out: *[64]u8) void {
    for (hash, 0..) |byte, i| {
        out[i * 2] = hex_chars[byte >> 4];
        out[i * 2 + 1] = hex_chars[byte & 0x0f];
    }
}

/// Parse 64-char hex string back to 32 bytes.
pub fn hexDecode(hex: []const u8) ![32]u8 {
    if (hex.len != 64) return error.InvalidHexLength;
    var out: [32]u8 = undefined;
    for (0..32) |i| {
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

/// Convenience: hash bytes and return hex string.
pub fn hashBytesHex(data: []const u8) [64]u8 {
    const h = hashBytes(data);
    var out: [64]u8 = undefined;
    hexEncode(h, &out);
    return out;
}

// ─── Tests ──────────────────────────────────────────────────────────────────

test "hashBytes deterministic" {
    const h1 = hashBytes("hello world");
    const h2 = hashBytes("hello world");
    try std.testing.expectEqual(h1, h2);
}

test "hashBytes different inputs" {
    const h1 = hashBytes("hello");
    const h2 = hashBytes("world");
    try std.testing.expect(!std.mem.eql(u8, &h1, &h2));
}

test "hex round-trip" {
    const original = hashBytes("test data");
    var hex: [64]u8 = undefined;
    hexEncode(original, &hex);
    const decoded = try hexDecode(&hex);
    try std.testing.expectEqual(original, decoded);
}

test "hexDecode invalid length" {
    const short = "abcd";
    try std.testing.expectError(error.InvalidHexLength, hexDecode(short));
}

test "hexDecode invalid char" {
    var bad: [64]u8 = undefined;
    @memset(&bad, 'g'); // 'g' is not valid hex
    try std.testing.expectError(error.InvalidHexChar, hexDecode(&bad));
}

test "hashBytesHex" {
    const hex = hashBytesHex("hello");
    // Should be valid hex
    for (hex) |c| {
        try std.testing.expect((c >= '0' and c <= '9') or (c >= 'a' and c <= 'f'));
    }
}
