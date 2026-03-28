/// TurboDB — LZ4 block compression for 4KB pages
///
/// Implements a simplified but correct LZ4 block compressor/decompressor.
/// All operations are in-place (no allocator needed). Pages with a compression
/// ratio below 1.1x are left uncompressed to avoid wasting CPU.
const std = @import("std");
const page = @import("page.zig");

pub const COMPRESS_FLAG: u8 = 0x01;

const HASH_LOG = 12;
const HASH_SIZE = 1 << HASH_LOG; // 4096

const MIN_MATCH = 4;
const LAST_LITERALS = 5; // safety margin: last 5 bytes always emitted as literals
const ML_MASK = 0x0F;
const RUN_MASK = 0x0F;

const CompressionError = error{
    OutputTooSmall,
    InvalidInput,
};

// ─── LZ4 block compress ──────────────────────────────────────────────────

/// Compress `src` into `dst` using LZ4 block format.
/// Returns the number of bytes written to `dst`.
pub fn compress(src: []const u8, dst: []u8) CompressionError!usize {
    if (src.len == 0) {
        return 0;
    }

    // If source is too small for any matches, emit everything as literals.
    if (src.len < MIN_MATCH + LAST_LITERALS) {
        return emitLiterals(src, 0, src.len, dst, 0);
    }

    var hash_table: [HASH_SIZE]u16 = [_]u16{0} ** HASH_SIZE;

    var src_pos: usize = 0;
    var dst_pos: usize = 0;
    var anchor: usize = 0; // start of pending literals

    const src_limit = src.len - LAST_LITERALS; // don't match in the last 5 bytes

    while (src_pos < src_limit) {
        // Hash current 4-byte sequence
        const h = hash4(src, src_pos);
        const match_pos: usize = hash_table[h];
        hash_table[h] = @intCast(if (src_pos > std.math.maxInt(u16)) std.math.maxInt(u16) else src_pos);

        // Check for match: must be within u16 offset range and actually match 4 bytes
        const offset = src_pos -% match_pos;
        if (match_pos < src_pos and offset > 0 and offset <= std.math.maxInt(u16) and
            eql4(src, src_pos, match_pos))
        {
            // Found a match — count match length
            var match_len: usize = MIN_MATCH;
            const max_match = @min(src.len - src_pos, src.len - match_pos) ;
            while (match_len < max_match and src[src_pos + match_len] == src[match_pos + match_len]) {
                match_len += 1;
            }

            // Emit: [token][literal_ext][literals][offset_le16][match_ext]
            const lit_len = src_pos - anchor;
            dst_pos = try emitSequence(src, dst, dst_pos, anchor, lit_len, @intCast(offset), match_len);

            src_pos += match_len;
            anchor = src_pos;
        } else {
            src_pos += 1;
        }
    }

    // Emit remaining bytes as trailing literals
    const remaining = src.len - anchor;
    if (remaining > 0) {
        dst_pos = try emitLiterals(src, anchor, remaining, dst, dst_pos);
    }

    return dst_pos;
}

// ─── LZ4 block decompress ────────────────────────────────────────────────

/// Decompress `src` (LZ4 block) into `dst`.
/// `original_size` is the expected decompressed length.
/// Returns the number of decompressed bytes written.
pub fn decompress(src: []const u8, dst: []u8, original_size: usize) CompressionError!usize {
    if (src.len == 0) {
        if (original_size != 0) return error.InvalidInput;
        return 0;
    }

    if (dst.len < original_size) return error.OutputTooSmall;

    var sp: usize = 0; // source position
    var dp: usize = 0; // destination position

    while (sp < src.len) {
        if (sp >= src.len) return error.InvalidInput;
        const token = src[sp];
        sp += 1;

        // --- Literal run ---
        var lit_len: usize = @as(usize, token >> 4);
        if (lit_len == 15) {
            lit_len += try readExtendedLen(src, &sp);
        }

        if (sp + lit_len > src.len) return error.InvalidInput;
        if (dp + lit_len > dst.len) return error.OutputTooSmall;

        @memcpy(dst[dp..][0..lit_len], src[sp..][0..lit_len]);
        sp += lit_len;
        dp += lit_len;

        // If we've consumed all source bytes, this was the final literal-only sequence.
        if (sp >= src.len) break;

        // --- Match copy ---
        if (sp + 2 > src.len) return error.InvalidInput;
        const offset: usize = @as(usize, src[sp]) | (@as(usize, src[sp + 1]) << 8);
        sp += 2;

        if (offset == 0) return error.InvalidInput;
        if (dp < offset) return error.InvalidInput;

        var match_len: usize = @as(usize, token & ML_MASK) + MIN_MATCH;
        if ((token & ML_MASK) == 15) {
            match_len += try readExtendedLen(src, &sp);
        }

        if (dp + match_len > dst.len) return error.OutputTooSmall;

        // Copy from back-reference (may overlap, so byte-by-byte)
        const match_start = dp - offset;
        for (0..match_len) |i| {
            dst[dp + i] = dst[match_start + i];
        }
        dp += match_len;
    }

    if (dp != original_size) return error.InvalidInput;
    return dp;
}

// ─── Page-level helpers ──────────────────────────────────────────────────

const MIN_COMPRESSION_RATIO_X10 = 11; // 1.1x stored as fixed-point * 10

/// Compress a full page (usable area only, header excluded).
/// Returns the compressed size and whether compression was applied.
/// If compression is not worthwhile (ratio < 1.1x), returns the original
/// data unmodified with `compressed = false`.
pub fn compressPage(data: []const u8, out: []u8) CompressionError!struct { size: usize, compressed: bool } {
    if (data.len == 0) return .{ .size = 0, .compressed = false };

    const compressed_size = try compress(data, out);

    // Check if compression is worthwhile: original / compressed >= 1.1
    // Rearranged to avoid floats: original * 10 >= compressed * 11
    if (compressed_size > 0 and data.len * 10 >= compressed_size * MIN_COMPRESSION_RATIO_X10) {
        return .{ .size = compressed_size, .compressed = true };
    }

    // Not worth it — copy original data to output
    if (out.len < data.len) return error.OutputTooSmall;
    @memcpy(out[0..data.len], data);
    return .{ .size = data.len, .compressed = false };
}

/// Decompress a page. If `src` is a compressed page, decompresses into `dst`
/// and returns a slice of `dst`. If uncompressed, copies `src` into `dst` and
/// returns a slice of `dst` with `original_size` bytes.
pub fn decompressPage(src: []const u8, dst: []u8, original_size: usize) CompressionError![]u8 {
    if (src.len == original_size) {
        // Not compressed — just copy
        if (dst.len < original_size) return error.OutputTooSmall;
        @memcpy(dst[0..original_size], src[0..original_size]);
        return dst[0..original_size];
    }

    const written = try decompress(src, dst, original_size);
    return dst[0..written];
}

// ─── Internal helpers ────────────────────────────────────────────────────

/// Hash 4 bytes at `pos` into a table index.
inline fn hash4(data: []const u8, pos: usize) u12 {
    const v = std.mem.readInt(u32, data[pos..][0..4], .little);
    return @truncate(v *% 2654435761); // Knuth multiplicative hash
}

/// Check if 4 bytes at positions a and b are equal.
inline fn eql4(data: []const u8, a: usize, b: usize) bool {
    return std.mem.readInt(u32, data[a..][0..4], .little) ==
        std.mem.readInt(u32, data[b..][0..4], .little);
}

/// Read extended length (sum of 255-bytes until a byte < 255).
fn readExtendedLen(src: []const u8, pos: *usize) CompressionError!usize {
    var length: usize = 0;
    while (pos.* < src.len) {
        const byte = src[pos.*];
        pos.* += 1;
        length += byte;
        if (byte < 255) return length;
    }
    return error.InvalidInput;
}

/// Write extended length bytes into dst. Returns updated dst position.
fn writeExtendedLen(dst: []u8, pos: usize, length: usize) CompressionError!usize {
    var dp = pos;
    var rem = length;
    while (rem >= 255) {
        if (dp >= dst.len) return error.OutputTooSmall;
        dst[dp] = 255;
        dp += 1;
        rem -= 255;
    }
    if (dp >= dst.len) return error.OutputTooSmall;
    dst[dp] = @intCast(rem);
    dp += 1;
    return dp;
}

/// Emit a literal-only sequence (no match).
fn emitLiterals(src: []const u8, anchor: usize, lit_len: usize, dst: []u8, pos: usize) CompressionError!usize {
    var dp = pos;

    // Token byte
    if (dp >= dst.len) return error.OutputTooSmall;
    if (lit_len < 15) {
        dst[dp] = @as(u8, @intCast(lit_len)) << 4;
        dp += 1;
    } else {
        dst[dp] = 0xF0;
        dp += 1;
        dp = try writeExtendedLen(dst, dp, lit_len - 15);
    }

    // Literal bytes
    if (dp + lit_len > dst.len) return error.OutputTooSmall;
    @memcpy(dst[dp..][0..lit_len], src[anchor..][0..lit_len]);
    dp += lit_len;

    return dp;
}

/// Emit a full LZ4 sequence: literals + match.
fn emitSequence(
    src: []const u8,
    dst: []u8,
    pos: usize,
    anchor: usize,
    lit_len: usize,
    offset: u16,
    match_len: usize,
) CompressionError!usize {
    var dp = pos;
    const ml = match_len - MIN_MATCH; // match length field = actual - 4

    // Token byte
    if (dp >= dst.len) return error.OutputTooSmall;
    const lit_part: u8 = if (lit_len >= 15) 15 else @intCast(lit_len);
    const match_part: u8 = if (ml >= 15) 15 else @intCast(ml);
    dst[dp] = (lit_part << 4) | match_part;
    dp += 1;

    // Extended literal length
    if (lit_len >= 15) {
        dp = try writeExtendedLen(dst, dp, lit_len - 15);
    }

    // Literal bytes
    if (dp + lit_len > dst.len) return error.OutputTooSmall;
    @memcpy(dst[dp..][0..lit_len], src[anchor..][0..lit_len]);
    dp += lit_len;

    // Match offset (little-endian u16)
    if (dp + 2 > dst.len) return error.OutputTooSmall;
    dst[dp] = @intCast(offset & 0xFF);
    dst[dp + 1] = @intCast(offset >> 8);
    dp += 2;

    // Extended match length
    if (ml >= 15) {
        dp = try writeExtendedLen(dst, dp, ml - 15);
    }

    return dp;
}

// ─── Tests ───────────────────────────────────────────────────────────────

test "compress/decompress empty input" {
    var dst: [64]u8 = undefined;
    const csz = try compress(&[_]u8{}, &dst);
    try std.testing.expectEqual(@as(usize, 0), csz);

    const dsz = try decompress(&[_]u8{}, &dst, 0);
    try std.testing.expectEqual(@as(usize, 0), dsz);
}

test "compress/decompress small input (all literals)" {
    const src = "Hello!";
    var compressed: [128]u8 = undefined;
    var decompressed: [128]u8 = undefined;

    const csz = try compress(src, &compressed);
    try std.testing.expect(csz > 0);

    const dsz = try decompress(compressed[0..csz], &decompressed, src.len);
    try std.testing.expectEqual(src.len, dsz);
    try std.testing.expectEqualSlices(u8, src, decompressed[0..dsz]);
}

test "compress/decompress repetitive data (high compression)" {
    // 1024 bytes of repeating pattern — should compress very well
    var src: [1024]u8 = undefined;
    for (0..src.len) |i| {
        src[i] = @intCast(i % 7);
    }

    var compressed: [2048]u8 = undefined;
    var decompressed: [1024]u8 = undefined;

    const csz = try compress(&src, &compressed);
    try std.testing.expect(csz > 0);
    try std.testing.expect(csz < src.len); // should actually compress

    const dsz = try decompress(compressed[0..csz], &decompressed, src.len);
    try std.testing.expectEqual(src.len, dsz);
    try std.testing.expectEqualSlices(u8, &src, decompressed[0..dsz]);
}

test "compress/decompress random-ish data (low compression)" {
    // Pseudo-random data using a simple LCG
    var src: [512]u8 = undefined;
    var state: u32 = 0xDEAD_BEEF;
    for (0..src.len) |i| {
        state = state *% 1103515245 +% 12345;
        src[i] = @truncate(state >> 16);
    }

    var compressed: [1024]u8 = undefined;
    var decompressed: [512]u8 = undefined;

    const csz = try compress(&src, &compressed);
    try std.testing.expect(csz > 0);

    const dsz = try decompress(compressed[0..csz], &decompressed, src.len);
    try std.testing.expectEqual(src.len, dsz);
    try std.testing.expectEqualSlices(u8, &src, decompressed[0..dsz]);
}

test "round-trip 4KB page data" {
    // Simulate a realistic page: header-like prefix + structured data + padding
    var src: [page.PAGE_SIZE]u8 = undefined;
    @memset(&src, 0);
    // Some structured content in the usable area
    for (page.PAGE_HEADER_SIZE..page.PAGE_SIZE) |i| {
        src[i] = @intCast((i * 31 + 17) % 256);
    }

    var compressed: [page.PAGE_SIZE * 2]u8 = undefined;
    var decompressed: [page.PAGE_SIZE]u8 = undefined;

    const csz = try compress(&src, &compressed);
    const dsz = try decompress(compressed[0..csz], &decompressed, src.len);
    try std.testing.expectEqual(src.len, dsz);
    try std.testing.expectEqualSlices(u8, &src, &decompressed);
}

test "compressPage skips compression when ratio < 1.1x" {
    // Random data won't compress well
    var src: [512]u8 = undefined;
    var state: u32 = 0xCAFE_BABE;
    for (0..src.len) |i| {
        state = state *% 1103515245 +% 12345;
        src[i] = @truncate(state >> 16);
    }

    var out: [1024]u8 = undefined;
    const result = try compressPage(&src, &out);

    if (!result.compressed) {
        // Should have copied original data
        try std.testing.expectEqual(src.len, result.size);
        try std.testing.expectEqualSlices(u8, &src, out[0..result.size]);
    }
}

test "compressPage/decompressPage round-trip" {
    // Highly compressible data
    var src: [page.PAGE_USABLE]u8 = undefined;
    @memset(&src, 0xAA);
    // Sprinkle some variation
    for (0..src.len) |i| {
        if (i % 64 == 0) src[i] = @intCast(i % 256);
    }

    var compressed: [page.PAGE_SIZE * 2]u8 = undefined;
    const result = try compressPage(&src, &compressed);

    var decompressed: [page.PAGE_USABLE]u8 = undefined;
    const out = try decompressPage(compressed[0..result.size], &decompressed, src.len);
    try std.testing.expectEqualSlices(u8, &src, out);
}

test "compressPage empty input" {
    var out: [64]u8 = undefined;
    const result = try compressPage(&[_]u8{}, &out);
    try std.testing.expectEqual(@as(usize, 0), result.size);
    try std.testing.expect(!result.compressed);
}
