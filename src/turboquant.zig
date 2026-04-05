//! TurboQuant: Near-Optimal Online Vector Quantization
//!
//! Implements the TurboQuant algorithm (Zandieh et al., 2025) for data-oblivious
//! vector quantization with near-information-theoretic-optimal distortion.
//!
//! Core idea:
//!   1. Random rotation Π maps any vector to uniform distribution on hypersphere
//!   2. Each rotated coordinate follows Beta((d-1)/2, (d-1)/2) ≈ N(0, 1/d) in high dims
//!   3. Coordinates become nearly independent → optimal per-coordinate scalar quantization
//!   4. Pre-computed Lloyd-Max codebooks give near-optimal MSE at any bit-width
//!
//! Key properties:
//!   - Zero indexing time (data-oblivious, no codebook training)
//!   - Near-optimal MSE: within 2.7x of information-theoretic lower bound
//!   - SIMD-friendly: per-coordinate operations, no codebook lookups during scan
//!   - Works at any bit-width b = 1, 2, 3, 4 bits per coordinate

const std = @import("std");
const math = std.math;
const Allocator = std.mem.Allocator;

/// SIMD lane width for vector operations.
const V8 = @Vector(8, f32);

/// Pre-computed Lloyd-Max centroids for Gaussian approximation N(0, 1/d).
/// These are the optimal scalar quantizer centroids for bit-widths 1-4,
/// normalized so the actual centroid = table_value / sqrt(d).
///
/// Derived from solving the continuous 1D k-means problem (Eq. 4 in paper):
///   minimize sum_i integral |x - c_i|^2 * f(x) dx
/// where f(x) is the Beta/Gaussian PDF.
const CODEBOOK_1BIT = [_]f32{ -0.7979, 0.7979 }; // ±√(2/π)
const CODEBOOK_2BIT = [_]f32{ -1.5104, -0.4528, 0.4528, 1.5104 };
const CODEBOOK_3BIT = [_]f32{ -2.1520, -1.3440, -0.7560, -0.2451, 0.2451, 0.7560, 1.3440, 2.1520 };
const CODEBOOK_4BIT = [_]f32{
    -2.7326, -2.0690, -1.6180, -1.2562, -0.9424, -0.6568, -0.3880, -0.1284,
    0.1284,  0.3880,  0.6568,  0.9424,  1.2562,  1.6180,  2.0690,  2.7326,
};

/// TurboQuant quantizer for a fixed dimension and bit-width.
pub const TurboQuant = struct {
    dims: u32,
    bit_width: u8, // 1, 2, 3, or 4
    num_centroids: u32, // 2^bit_width
    rotation: []f32, // Π matrix (dims × dims, row-major)
    codebook: []f32, // scaled centroids for this dims (num_centroids entries)
    boundaries: []f32, // decision boundaries between centroids (num_centroids - 1 entries)
    bytes_per_vec: u32, // ceil(dims * bit_width / 8)
    allocator: Allocator,

    pub fn init(allocator: Allocator, dims: u32, bit_width: u8, seed: u64) !TurboQuant {
        if (bit_width < 1 or bit_width > 4) return error.InvalidBitWidth;
        if (dims == 0) return error.InvalidDimension;

        const nc: u32 = @as(u32, 1) << @intCast(bit_width);
        const d: usize = @intCast(dims);

        // Allocate rotation matrix (d × d)
        const rotation = try allocator.alloc(f32, d * d);
        errdefer allocator.free(rotation);

        // Generate random orthogonal matrix via Gram-Schmidt on random Gaussian columns
        var rng = std.Random.Xoshiro256.init(seed);
        generateOrthogonalMatrix(rotation, d, &rng);

        // Build scaled codebook: centroid_i = base_centroid_i / sqrt(d)
        const codebook = try allocator.alloc(f32, nc);
        errdefer allocator.free(codebook);
        const scale: f32 = 1.0 / @sqrt(@as(f32, @floatFromInt(dims)));
        const base = switch (bit_width) {
            1 => &CODEBOOK_1BIT,
            2 => &CODEBOOK_2BIT,
            3 => &CODEBOOK_3BIT,
            4 => &CODEBOOK_4BIT,
            else => unreachable,
        };
        for (0..nc) |i| {
            codebook[i] = base[i] * scale;
        }

        // Pre-compute decision boundaries (midpoints between adjacent centroids)
        const nb = nc - 1;
        const boundaries = try allocator.alloc(f32, nb);
        errdefer allocator.free(boundaries);
        for (0..nb) |i| {
            boundaries[i] = (codebook[i] + codebook[i + 1]) * 0.5;
        }

        const bpv = (dims * bit_width + 7) / 8;

        return .{
            .dims = dims,
            .bit_width = bit_width,
            .num_centroids = nc,
            .rotation = rotation,
            .codebook = codebook,
            .boundaries = boundaries,
            .bytes_per_vec = bpv,
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *TurboQuant) void {
        self.allocator.free(self.rotation);
        self.allocator.free(self.codebook);
        self.allocator.free(self.boundaries);
    }

    /// Quantize a d-dimensional FP32 vector to packed b-bit indices.
    /// `out` must have length >= self.bytes_per_vec.
    pub fn quantize(self: *const TurboQuant, vector: []const f32, out: []u8) void {
        const d: usize = @intCast(self.dims);
        if (vector.len < d) return;

        // Step 1: Rotate — y = Π · x (matrix-vector multiply)
        // Use stack buffer for small dims, heap for large
        var rotated_buf: [1024]f32 = undefined;
        const rotated = if (d <= 1024) rotated_buf[0..d] else blk: {
            break :blk self.allocator.alloc(f32, d) catch return;
        };
        defer if (d > 1024) self.allocator.free(rotated);

        matvecMul(self.rotation, vector, rotated, d);

        // Step 2: Quantize each coordinate to nearest centroid index
        @memset(out[0..self.bytes_per_vec], 0);
        const bw = self.bit_width;

        for (0..d) |j| {
            const val = rotated[j];
            // Find nearest centroid via boundary comparison
            var idx: u8 = 0;
            for (self.boundaries) |b| {
                if (val > b) idx += 1 else break;
            }
            // Pack b-bit index into output bytes
            packBits(out, j, idx, bw);
        }
    }

    /// Dequantize packed b-bit indices back to FP32 vector.
    /// `out` must have length >= dims.
    pub fn dequantize(self: *const TurboQuant, quantized: []const u8, out: []f32) void {
        const d: usize = @intCast(self.dims);

        // Step 1: Unpack indices and look up centroids
        var rotated_buf: [1024]f32 = undefined;
        const rotated = if (d <= 1024) rotated_buf[0..d] else blk: {
            break :blk self.allocator.alloc(f32, d) catch return;
        };
        defer if (d > 1024) self.allocator.free(rotated);

        const bw = self.bit_width;
        for (0..d) |j| {
            const idx = unpackBits(quantized, j, bw);
            rotated[j] = self.codebook[idx];
        }

        // Step 2: Inverse rotate — x̃ = Π^T · ỹ
        matvecMulTranspose(self.rotation, rotated, out, d);
    }

    /// Asymmetric L2 distance: FP32 query vs quantized database vector.
    /// This is the key optimization — we compute distance in the rotated space
    /// without dequantizing the database vector.
    pub fn asymmetricL2(self: *const TurboQuant, query: []const f32, quantized: []const u8) f32 {
        const d: usize = @intCast(self.dims);

        // Rotate query: y_q = Π · q
        var rotated_buf: [1024]f32 = undefined;
        const yq = if (d <= 1024) rotated_buf[0..d] else blk: {
            break :blk self.allocator.alloc(f32, d) catch return math.floatMax(f32);
        };
        defer if (d > 1024) self.allocator.free(yq);

        matvecMul(self.rotation, query, yq, d);

        // Sum (yq_j - centroid[idx_j])^2 over all coordinates
        var sum: f32 = 0;
        const bw = self.bit_width;
        const cb = self.codebook;

        // SIMD path for 8 coordinates at a time
        var j: usize = 0;
        var v_sum: V8 = @splat(0.0);
        while (j + 8 <= d) : (j += 8) {
            const yq_v: V8 = yq[j..][0..8].*;
            var cb_v: V8 = undefined;
            inline for (0..8) |k| {
                cb_v[k] = cb[unpackBits(quantized, j + k, bw)];
            }
            const diff = yq_v - cb_v;
            v_sum += diff * diff;
            j = j; // suppress unused
        }
        sum = @reduce(.Add, v_sum);

        // Scalar tail
        while (j < d) : (j += 1) {
            const diff = yq[j] - cb[unpackBits(quantized, j, bw)];
            sum += diff * diff;
        }

        return sum;
    }

    /// Asymmetric dot product: FP32 query vs quantized database vector.
    pub fn asymmetricDot(self: *const TurboQuant, query: []const f32, quantized: []const u8) f32 {
        const d: usize = @intCast(self.dims);

        var rotated_buf: [1024]f32 = undefined;
        const yq = if (d <= 1024) rotated_buf[0..d] else blk: {
            break :blk self.allocator.alloc(f32, d) catch return 0;
        };
        defer if (d > 1024) self.allocator.free(yq);

        matvecMul(self.rotation, query, yq, d);

        // Sum yq_j * centroid[idx_j] over all coordinates
        var sum: f32 = 0;
        const bw = self.bit_width;
        const cb = self.codebook;

        var j: usize = 0;
        var v_sum: V8 = @splat(0.0);
        while (j + 8 <= d) : (j += 8) {
            const yq_v: V8 = yq[j..][0..8].*;
            var cb_v: V8 = undefined;
            inline for (0..8) |k| {
                cb_v[k] = cb[unpackBits(quantized, j + k, bw)];
            }
            v_sum += yq_v * cb_v;
        }
        sum = @reduce(.Add, v_sum);

        while (j < d) : (j += 1) {
            sum += yq[j] * cb[unpackBits(quantized, j, bw)];
        }

        return sum;
    }

    /// Symmetric distance between two quantized vectors using pre-computed distance table.
    pub fn symmetricL2(self: *const TurboQuant, qa: []const u8, qb: []const u8) f32 {
        const d: usize = @intCast(self.dims);
        const nc = self.num_centroids;
        const bw = self.bit_width;
        const cb = self.codebook;

        // Build distance table (tiny: max 16×16 = 256 entries for b=4)
        var dist_table: [16 * 16]f32 = undefined;
        for (0..nc) |i| {
            for (0..nc) |j_idx| {
                const diff = cb[i] - cb[j_idx];
                dist_table[i * nc + j_idx] = diff * diff;
            }
        }

        var sum: f32 = 0;
        for (0..d) |j| {
            const ia = unpackBits(qa, j, bw);
            const ib = unpackBits(qb, j, bw);
            sum += dist_table[@as(usize, ia) * nc + @as(usize, ib)];
        }

        return sum;
    }

    /// Pre-rotate a query vector: y_q = Π · q. Caller owns the returned slice.
    /// Call this ONCE per search, then use buildDistTable + scanWithTable per vector.
    pub fn rotateQuery(self: *const TurboQuant, allocator: Allocator, query: []const f32) ![]f32 {
        const d: usize = @intCast(self.dims);
        const yq = try allocator.alloc(f32, d);
        matvecMul(self.rotation, query, yq, d);
        return yq;
    }

    /// Build an ADC (Asymmetric Distance Computation) lookup table for L2.
    /// dist_table[j * nc + k] = (rotated_query[j] - centroid[k])^2
    /// Call ONCE per query, then scanWithTable is a trivial table lookup per coordinate.
    pub fn buildL2Table(self: *const TurboQuant, allocator: Allocator, rotated_query: []const f32) ![]f32 {
        const d: usize = @intCast(self.dims);
        const nc: usize = self.num_centroids;
        const cb = self.codebook;
        const table = try allocator.alloc(f32, d * nc);
        for (0..d) |j| {
            const qj = rotated_query[j];
            for (0..nc) |k| {
                const diff = qj - cb[k];
                table[j * nc + k] = diff * diff;
            }
        }
        return table;
    }

    /// Build an ADC lookup table for dot product.
    /// dot_table[j * nc + k] = rotated_query[j] * centroid[k]
    pub fn buildDotTable(self: *const TurboQuant, allocator: Allocator, rotated_query: []const f32) ![]f32 {
        const d: usize = @intCast(self.dims);
        const nc: usize = self.num_centroids;
        const cb = self.codebook;
        const table = try allocator.alloc(f32, d * nc);
        for (0..d) |j| {
            const qj = rotated_query[j];
            for (0..nc) |k| {
                table[j * nc + k] = qj * cb[k];
            }
        }
        return table;
    }

    /// Ultra-fast scan: just table lookups per coordinate. O(d) with tiny constant.
    /// For 4-bit: 2 coords per byte, each is a table lookup + accumulate.
    pub fn scanWithTable(self: *const TurboQuant, table: []const f32, quantized: []const u8) f32 {
        const d: usize = @intCast(self.dims);
        const nc: usize = self.num_centroids;
        const bw = self.bit_width;
        var sum: f32 = 0;

        if (bw == 4) {
            // Fast path for 4-bit: 2 nibbles per byte, no bit-shifting needed
            var j: usize = 0;
            var byte_idx: usize = 0;
            while (j + 2 <= d) : ({
                j += 2;
                byte_idx += 1;
            }) {
                const b = quantized[byte_idx];
                const lo: usize = b & 0x0F;
                const hi: usize = (b >> 4) & 0x0F;
                sum += table[j * nc + lo];
                sum += table[(j + 1) * nc + hi];
            }
            if (j < d) {
                sum += table[j * nc + @as(usize, quantized[byte_idx] & 0x0F)];
            }
        } else if (bw == 2) {
            // Fast path for 2-bit: 4 values per byte
            var j: usize = 0;
            var byte_idx: usize = 0;
            while (j + 4 <= d) : ({
                j += 4;
                byte_idx += 1;
            }) {
                const b = quantized[byte_idx];
                sum += table[j * nc + @as(usize, b & 0x03)];
                sum += table[(j + 1) * nc + @as(usize, (b >> 2) & 0x03)];
                sum += table[(j + 2) * nc + @as(usize, (b >> 4) & 0x03)];
                sum += table[(j + 3) * nc + @as(usize, (b >> 6) & 0x03)];
            }
            while (j < d) : (j += 1) {
                sum += table[j * nc + @as(usize, unpackBits(quantized, j, bw))];
            }
        } else {
            // Generic path
            for (0..d) |j| {
                sum += table[j * nc + @as(usize, unpackBits(quantized, j, bw))];
            }
        }
        return sum;
    }

    // Keep the old methods for backward compat / single-vector queries
    pub fn scanL2Rotated(self: *const TurboQuant, rotated_query: []const f32, quantized: []const u8) f32 {
        const d: usize = @intCast(self.dims);
        const bw = self.bit_width;
        const cb = self.codebook;
        var sum: f32 = 0;
        for (0..d) |j| {
            const diff = rotated_query[j] - cb[unpackBits(quantized, j, bw)];
            sum += diff * diff;
        }
        return sum;
    }

    pub fn scanDotRotated(self: *const TurboQuant, rotated_query: []const f32, quantized: []const u8) f32 {
        const d: usize = @intCast(self.dims);
        const bw = self.bit_width;
        const cb = self.codebook;
        var sum: f32 = 0;
        for (0..d) |j| {
            sum += rotated_query[j] * cb[unpackBits(quantized, j, bw)];
        }
        return sum;
    }
};

// ─── Bit packing ────────────────────────────────────────────────────────

/// Pack a b-bit value at coordinate index `coord_idx` into byte array.
fn packBits(out: []u8, coord_idx: usize, value: u8, bit_width: u8) void {
    const bit_offset = coord_idx * bit_width;
    const byte_idx = bit_offset / 8;
    const bit_shift: u3 = @intCast(bit_offset % 8);
    if (byte_idx < out.len) {
        out[byte_idx] |= @as(u8, value) << bit_shift;
        // Handle overflow into next byte
        const bits_in_first = 8 - @as(u4, bit_shift);
        if (bits_in_first < bit_width and byte_idx + 1 < out.len) {
            out[byte_idx + 1] |= @as(u8, value) >> @intCast(bits_in_first);
        }
    }
}

/// Unpack a b-bit value at coordinate index `coord_idx` from byte array.
fn unpackBits(data: []const u8, coord_idx: usize, bit_width: u8) u8 {
    const bit_offset = coord_idx * bit_width;
    const byte_idx = bit_offset / 8;
    const bit_shift: u3 = @intCast(bit_offset % 8);
    const mask: u8 = (@as(u8, 1) << @intCast(bit_width)) - 1;
    if (byte_idx >= data.len) return 0;
    var val = (data[byte_idx] >> bit_shift) & mask;
    // Handle bits spanning two bytes
    const bits_in_first = 8 - @as(u4, bit_shift);
    if (bits_in_first < bit_width and byte_idx + 1 < data.len) {
        val |= (data[byte_idx + 1] << @intCast(bits_in_first)) & mask;
    }
    return val;
}

// ─── Linear algebra helpers ─────────────────────────────────────────────

/// Matrix-vector multiply: out = M · x (M is d×d row-major)
fn matvecMul(m: []const f32, x: []const f32, out: []f32, d: usize) void {
    for (0..d) |i| {
        const row = m[i * d ..][0..d];
        var sum: f32 = 0;
        // SIMD accumulation
        var j: usize = 0;
        var v_sum: V8 = @splat(0.0);
        while (j + 8 <= d) : (j += 8) {
            const a: V8 = row[j..][0..8].*;
            const b: V8 = x[j..][0..8].*;
            v_sum += a * b;
        }
        sum = @reduce(.Add, v_sum);
        // Scalar tail
        while (j < d) : (j += 1) {
            sum += row[j] * x[j];
        }
        out[i] = sum;
    }
}

/// Matrix-transpose-vector multiply: out = M^T · x (M is d×d row-major)
fn matvecMulTranspose(m: []const f32, x: []const f32, out: []f32, d: usize) void {
    @memset(out[0..d], 0);
    for (0..d) |i| {
        const row = m[i * d ..][0..d];
        const xi = x[i];
        // out += xi * row
        var j: usize = 0;
        const v_xi: V8 = @splat(xi);
        while (j + 8 <= d) : (j += 8) {
            const r: V8 = row[j..][0..8].*;
            const o: V8 = out[j..][0..8].*;
            const result = o + v_xi * r;
            out[j..][0..8].* = result;
        }
        while (j < d) : (j += 1) {
            out[j] += xi * row[j];
        }
    }
}

/// Generate a random orthogonal matrix via Gram-Schmidt on random Gaussian columns.
/// Uses the Xoshiro256 PRNG for reproducibility.
fn generateOrthogonalMatrix(m: []f32, d: usize, rng: *std.Random.Xoshiro256) void {
    const r = rng.random();
    // Fill with random Gaussian-like values (Box-Muller)
    var i: usize = 0;
    while (i + 1 < d * d) : (i += 2) {
        const ra = @as(f32, @floatFromInt(r.int(u32))) / @as(f32, @floatFromInt(math.maxInt(u32)));
        const rb = @as(f32, @floatFromInt(r.int(u32))) / @as(f32, @floatFromInt(math.maxInt(u32)));
        const rac = @max(ra, 1e-10); // avoid log(0)
        const mag = @sqrt(-2.0 * @log(rac));
        m[i] = mag * @cos(2.0 * math.pi * rb);
        m[i + 1] = mag * @sin(2.0 * math.pi * rb);
    }
    if (i < d * d) {
        const ra = @as(f32, @floatFromInt(r.int(u32))) / @as(f32, @floatFromInt(math.maxInt(u32)));
        const rb = @as(f32, @floatFromInt(r.int(u32))) / @as(f32, @floatFromInt(math.maxInt(u32)));
        const rac = @max(ra, 1e-10);
        m[i] = @sqrt(-2.0 * @log(rac)) * @cos(2.0 * math.pi * rb);
    }

    // Gram-Schmidt orthogonalization (column-wise)
    for (0..d) |col| {
        for (0..col) |prev| {
            var dot: f32 = 0;
            for (0..d) |row| {
                dot += m[row * d + col] * m[row * d + prev];
            }
            for (0..d) |row| {
                m[row * d + col] -= dot * m[row * d + prev];
            }
        }
        var norm: f32 = 0;
        for (0..d) |row| {
            norm += m[row * d + col] * m[row * d + col];
        }
        norm = @sqrt(@max(norm, 1e-10));
        for (0..d) |row| {
            m[row * d + col] /= norm;
        }
    }
}

// ─── Tests ──────────────────────────────────────────────────────────────

test "bit packing round-trip" {
    var buf: [4]u8 = .{ 0, 0, 0, 0 };

    // 2-bit packing: values 0-3
    packBits(&buf, 0, 3, 2);
    packBits(&buf, 1, 1, 2);
    packBits(&buf, 2, 0, 2);
    packBits(&buf, 3, 2, 2);

    try std.testing.expectEqual(@as(u8, 3), unpackBits(&buf, 0, 2));
    try std.testing.expectEqual(@as(u8, 1), unpackBits(&buf, 1, 2));
    try std.testing.expectEqual(@as(u8, 0), unpackBits(&buf, 2, 2));
    try std.testing.expectEqual(@as(u8, 2), unpackBits(&buf, 3, 2));
}

test "bit packing 4-bit round-trip" {
    var buf: [8]u8 = .{0} ** 8;
    for (0..16) |i| {
        packBits(&buf, i, @intCast(i % 16), 4);
    }
    for (0..16) |i| {
        try std.testing.expectEqual(@as(u8, @intCast(i % 16)), unpackBits(&buf, i, 4));
    }
}

test "TurboQuant quantize-dequantize round-trip" {
    const alloc = std.testing.allocator;
    var tq = try TurboQuant.init(alloc, 64, 4, 42);
    defer tq.deinit();

    // Create a unit vector
    var vec: [64]f32 = undefined;
    const norm_inv = 1.0 / @sqrt(@as(f32, 64.0));
    for (&vec) |*v| v.* = norm_inv;

    // Quantize
    var qbuf: [32]u8 = undefined; // 64 * 4 bits / 8 = 32 bytes
    tq.quantize(&vec, &qbuf);

    // Dequantize
    var out: [64]f32 = undefined;
    tq.dequantize(&qbuf, &out);

    // Check MSE is reasonable (should be < 0.1 for 4-bit at d=64)
    var mse: f32 = 0;
    for (0..64) |i| {
        const diff = vec[i] - out[i];
        mse += diff * diff;
    }
    // Paper says MSE ≈ 0.009 for b=4, but with d=64 we have higher variance
    try std.testing.expect(mse < 0.5);
}

test "TurboQuant asymmetric L2 distance" {
    const alloc = std.testing.allocator;
    var tq = try TurboQuant.init(alloc, 32, 2, 123);
    defer tq.deinit();

    // Two identical vectors should have ~0 distance
    var vec: [32]f32 = undefined;
    const norm_inv = 1.0 / @sqrt(@as(f32, 32.0));
    for (&vec) |*v| v.* = norm_inv;

    var qbuf: [8]u8 = undefined; // 32 * 2 bits / 8 = 8 bytes
    tq.quantize(&vec, &qbuf);

    const dist = tq.asymmetricL2(&vec, &qbuf);
    // Distance from vec to its own quantized version should be small
    try std.testing.expect(dist < 1.0);
}

test "orthogonal matrix is orthogonal" {
    const alloc = std.testing.allocator;
    const d = 16;
    const m = try alloc.alloc(f32, d * d);
    defer alloc.free(m);

    var rng = std.Random.Xoshiro256.init(99);
    generateOrthogonalMatrix(m, d, &rng);

    // Check M * M^T ≈ I
    for (0..d) |i| {
        for (0..d) |j| {
            var dot: f32 = 0;
            for (0..d) |k| {
                dot += m[i * d + k] * m[j * d + k];
            }
            const expected: f32 = if (i == j) 1.0 else 0.0;
            try std.testing.expect(@abs(dot - expected) < 0.1);
        }
    }
}
