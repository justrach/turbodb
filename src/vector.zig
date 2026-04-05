/// TurboDB — SIMD Vector Column Engine
///
/// Dense f32 vector storage with hardware-accelerated similarity search.
/// Supports cosine similarity, dot product, and L2 (Euclidean) distance.
///
/// Design:
///   - Vectors stored as flat f32 arrays (cache-friendly, SIMD-aligned)
///   - All vectors in a column must have the same dimensionality
///   - Similarity functions process 8 floats at a time via @Vector(8, f32)
///   - Top-K search returns indices sorted by similarity (descending)
///
/// Use cases: embeddings search, RAG, recommendation, image similarity.
const std = @import("std");
const Allocator = std.mem.Allocator;
const turboquant = @import("turboquant.zig");
/// SIMD lane width — process 8 f32s at a time (AVX2-width).
const LANE: usize = 8;
const VL = @Vector(LANE, f32);
const V8 = @Vector(LANE, f32);

/// Distance metric for similarity search.
pub const Metric = enum {
    cosine,
    dot_product,
    l2,
};

/// Result of a similarity search: vector index + score.
pub const SearchResult = struct {
    index: u32,
    score: f32,
};

/// A dense vector column. All vectors share the same dimensionality.
pub const VectorColumn = struct {
    dims: u32,
    data: std.ArrayListUnmanaged(f32) = .empty,
    count: u32 = 0,
    /// Optional: pre-computed L2 norms for cosine similarity (avoids re-computing).
    norms: std.ArrayListUnmanaged(f32) = .empty,
    /// Optional TurboQuant quantizer for compressed search.
    quant: ?*turboquant.TurboQuant = null,
    /// Packed quantized vector data (all vectors concatenated).
    qdata: std.ArrayListUnmanaged(u8) = .empty,
    /// Bytes per quantized vector (0 if quantization not enabled).
    bytes_per_vec: u32 = 0,

    pub fn init(dims: u32) VectorColumn {
        return .{ .dims = dims };
    }

    pub fn deinit(self: *VectorColumn, alloc: Allocator) void {
        self.data.deinit(alloc);
        self.norms.deinit(alloc);
        self.qdata.deinit(alloc);
        if (self.quant) |q| {
            q.deinit();
            alloc.destroy(q);
            self.quant = null;
        }
    }

    /// Append a vector. Must be exactly `dims` floats.
    pub fn append(self: *VectorColumn, alloc: Allocator, vec: []const f32) !void {
        if (vec.len != self.dims) return error.DimensionMismatch;
        try self.data.appendSlice(alloc, vec);
        // Pre-compute and cache the L2 norm for fast cosine later.
        try self.norms.append(alloc, l2Norm(vec));
        self.count += 1;

        // If quantization is enabled, also quantize the new vector.
        if (self.quant) |q| {
            const bpv = q.bytes_per_vec;
            const old_len = self.qdata.items.len;
            try self.qdata.resize(alloc, old_len + bpv);
            q.quantize(vec, self.qdata.items[old_len..][0..bpv]);
        }
    }

    /// Get vector at index. Returns slice into internal storage (zero-copy).
    pub fn get(self: *const VectorColumn, index: u32) ?[]const f32 {
        if (index >= self.count) return null;
        const start = @as(usize, index) * self.dims;
        return self.data.items[start..][0..self.dims];
    }

    /// Brute-force top-K similarity search.
    /// Returns up to `k` results sorted by score (highest first for cosine/dot, lowest for L2).
    pub fn search(
        self: *const VectorColumn,
        alloc: Allocator,
        query: []const f32,
        k: u32,
        metric: Metric,
    ) ![]SearchResult {
        if (query.len != self.dims) return error.DimensionMismatch;
        if (self.count == 0) return &.{};

        const actual_k = @min(k, self.count);
        const query_norm = if (metric == .cosine) l2Norm(query) else @as(f32, 0);

        // Allocate results heap (min-heap for top-K).
        var results = try alloc.alloc(SearchResult, actual_k);
        var heap_size: u32 = 0;

        var i: u32 = 0;
        while (i < self.count) : (i += 1) {
            const vec = self.get(i) orelse continue;
            const score = switch (metric) {
                .cosine => cosineSim(query, vec, query_norm, self.norms.items[i]),
                .dot_product => dotProduct(query, vec),
                .l2 => -l2Distance(query, vec), // negate so higher = closer
            };

            if (heap_size < actual_k) {
                // Fill heap
                results[heap_size] = .{ .index = i, .score = score };
                heap_size += 1;
                if (heap_size == actual_k) {
                    // Build min-heap
                    var j: u32 = heap_size / 2;
                    while (j > 0) {
                        j -= 1;
                        heapifyDown(results[0..heap_size], j);
                    }
                }
            } else if (score > results[0].score) {
                // Replace min element
                results[0] = .{ .index = i, .score = score };
                heapifyDown(results[0..heap_size], 0);
            }
        }

        // Sort results by score descending.
        std.mem.sort(SearchResult, results[0..heap_size], {}, struct {
            fn cmp(_: void, a: SearchResult, b: SearchResult) bool {
                return a.score > b.score;
            }
        }.cmp);

        // Un-negate L2 scores for the caller.
        if (metric == .l2) {
            for (results[0..heap_size]) |*r| r.score = -r.score;
        }

        return results[0..heap_size];
    }

    /// Enable quantization on this column. Quantizes all existing vectors.
    pub fn enableQuantization(self: *VectorColumn, alloc: Allocator, bit_width: u8, seed: u64) !void {
        // Clean up previous quantizer if any.
        if (self.quant) |old| {
            old.deinit();
            alloc.destroy(old);
        }

        const q = try alloc.create(turboquant.TurboQuant);
        errdefer alloc.destroy(q);
        q.* = try turboquant.TurboQuant.init(alloc, self.dims, bit_width, seed);
        self.quant = q;
        self.bytes_per_vec = q.bytes_per_vec;

        // Quantize all existing vectors.
        const bpv = q.bytes_per_vec;
        self.qdata.deinit(alloc);
        self.qdata = .empty;
        try self.qdata.resize(alloc, @as(usize, self.count) * bpv);
        @memset(self.qdata.items, 0);

        for (0..self.count) |i| {
            const vec = self.get(@intCast(i)) orelse continue;
            q.quantize(vec, self.qdata.items[i * bpv ..][0..bpv]);
        }
    }

    /// Fast quantized search: asymmetric distance for candidate selection,
    /// then re-rank top 2K with exact FP32 distance.
    pub fn searchQuantized(
        self: *const VectorColumn,
        alloc: Allocator,
        query: []const f32,
        k: u32,
        metric: Metric,
    ) ![]SearchResult {
        const q = self.quant orelse return self.search(alloc, query, k, metric);
        if (query.len != self.dims) return error.DimensionMismatch;
        if (self.count == 0) return &.{};

        // Phase 1: Pre-rotate query ONCE (O(d^2)), then scan all vectors (O(n*d)).
        const candidate_k = @min(self.count, k * 2); // 2x overselection
        var candidates = try alloc.alloc(SearchResult, candidate_k);
        defer alloc.free(candidates);
        var heap_size: u32 = 0;
        const bpv = q.bytes_per_vec;

        // For cosine: normalize query before rotation.
        var norm_query_buf: [1024]f32 = undefined;
        var norm_query_alloc: ?[]f32 = null;
        defer if (norm_query_alloc) |nq| alloc.free(nq);

        const effective_query = if (metric == .cosine) blk: {
            const qnorm = l2Norm(query);
            if (qnorm == 0.0) break :blk query;
            const nq = if (query.len <= 1024) norm_query_buf[0..query.len] else blk2: {
                norm_query_alloc = try alloc.alloc(f32, query.len);
                break :blk2 norm_query_alloc.?;
            };
            for (query, 0..) |v, idx| {
                nq[idx] = v / qnorm;
            }
            break :blk @as([]const f32, nq);
        } else query;

        // Rotate query ONCE — this is the O(d^2) step, done only once per search.
        const rotated_q = try q.rotateQuery(alloc, effective_query);
        defer alloc.free(rotated_q);

        // Build ADC lookup table ONCE — O(d * 2^b), then scan is just table lookups.
        const dist_table = switch (metric) {
            .l2 => try q.buildL2Table(alloc, rotated_q),
            .dot_product, .cosine => try q.buildDotTable(alloc, rotated_q),
        };
        defer alloc.free(dist_table);

        // Scan all quantized vectors — just table lookups per byte, extremely fast.
        var i: u32 = 0;
        while (i < self.count) : (i += 1) {
            const qvec = self.qdata.items[@as(usize, i) * bpv ..][0..bpv];
            const raw = q.scanWithTable(dist_table, qvec);
            const score = if (metric == .l2) -raw else raw;

            if (heap_size < candidate_k) {
                candidates[heap_size] = .{ .index = i, .score = score };
                heap_size += 1;
                if (heap_size == candidate_k) {
                    var j: u32 = heap_size / 2;
                    while (j > 0) {
                        j -= 1;
                        heapifyDown(candidates[0..heap_size], j);
                    }
                }
            } else if (score > candidates[0].score) {
                candidates[0] = .{ .index = i, .score = score };
                heapifyDown(candidates[0..heap_size], 0);
            }
        }

        // Phase 2: Re-rank candidates using exact FP32 distance.
        const actual_k = @min(k, heap_size);
        var results = try alloc.alloc(SearchResult, actual_k);
        var result_heap: u32 = 0;
        const query_norm = if (metric == .cosine) l2Norm(query) else @as(f32, 0);

        for (candidates[0..heap_size]) |cand| {
            const vec = self.get(cand.index) orelse continue;
            const exact_score = switch (metric) {
                .cosine => cosineSim(query, vec, query_norm, self.norms.items[cand.index]),
                .dot_product => dotProduct(query, vec),
                .l2 => -l2Distance(query, vec),
            };

            if (result_heap < actual_k) {
                results[result_heap] = .{ .index = cand.index, .score = exact_score };
                result_heap += 1;
                if (result_heap == actual_k) {
                    var j: u32 = result_heap / 2;
                    while (j > 0) {
                        j -= 1;
                        heapifyDown(results[0..result_heap], j);
                    }
                }
            } else if (exact_score > results[0].score) {
                results[0] = .{ .index = cand.index, .score = exact_score };
                heapifyDown(results[0..result_heap], 0);
            }
        }

        // Sort results by score descending.
        std.mem.sort(SearchResult, results[0..result_heap], {}, struct {
            fn cmp(_: void, a: SearchResult, b: SearchResult) bool {
                return a.score > b.score;
            }
        }.cmp);

        // Un-negate L2 scores.
        if (metric == .l2) {
            for (results[0..result_heap]) |*r| r.score = -r.score;
        }

        return results[0..result_heap];
    }

    /// Total memory used by this vector column in bytes.
    pub fn memoryBytes(self: *const VectorColumn) usize {
        return self.data.items.len * @sizeOf(f32) +
            self.norms.items.len * @sizeOf(f32) +
            self.qdata.items.len;
    }
};

// ── SIMD-accelerated math ────────────────────────────────────────────────────

/// Dot product using @Vector(8, f32) SIMD.
pub fn dotProduct(a: []const f32, b: []const f32) f32 {
    std.debug.assert(a.len == b.len);
    const n = a.len;
    const lanes = n / LANE;
    var sum: V8 = @splat(0.0);

    // SIMD main loop
    for (0..lanes) |i| {
        const va: V8 = a[i * LANE ..][0..LANE].*;
        const vb: V8 = b[i * LANE ..][0..LANE].*;
        sum += va * vb;
    }

    // Horizontal reduce
    var result = @reduce(.Add, sum);

    // Scalar tail
    for (lanes * LANE..n) |i| {
        result += a[i] * b[i];
    }
    return result;
}

/// Cosine similarity = dot(a,b) / (||a|| * ||b||).
pub fn cosineSim(a: []const f32, b: []const f32, norm_a: f32, norm_b: f32) f32 {
    const denom = norm_a * norm_b;
    if (denom == 0.0) return 0.0;
    return dotProduct(a, b) / denom;
}

/// L2 (Euclidean) distance using SIMD.
pub fn l2Distance(a: []const f32, b: []const f32) f32 {
    std.debug.assert(a.len == b.len);
    const n = a.len;
    const lanes = n / LANE;
    var sum: V8 = @splat(0.0);

    for (0..lanes) |i| {
        const va: V8 = a[i * LANE ..][0..LANE].*;
        const vb: V8 = b[i * LANE ..][0..LANE].*;
        const diff = va - vb;
        sum += diff * diff;
    }

    var result = @reduce(.Add, sum);
    for (lanes * LANE..n) |i| {
        const d = a[i] - b[i];
        result += d * d;
    }
    return @sqrt(result);
}

/// L2 norm (magnitude) of a vector.
pub fn l2Norm(v: []const f32) f32 {
    return @sqrt(dotProduct(v, v));
}

// ── Min-heap helpers for top-K ───────────────────────────────────────────────

fn heapifyDown(heap: []SearchResult, pos: u32) void {
    var i = pos;
    const n: u32 = @intCast(heap.len);
    while (true) {
        var smallest = i;
        const left = 2 * i + 1;
        const right = 2 * i + 2;
        if (left < n and heap[left].score < heap[smallest].score) smallest = left;
        if (right < n and heap[right].score < heap[smallest].score) smallest = right;
        if (smallest == i) break;
        std.mem.swap(SearchResult, &heap[i], &heap[smallest]);
        i = smallest;
    }
}

// ── Tests ────────────────────────────────────────────────────────────────────

test "dot product basic" {
    const a = [_]f32{ 1, 2, 3, 4 };
    const b = [_]f32{ 5, 6, 7, 8 };
    const result = dotProduct(&a, &b);
    // 1*5 + 2*6 + 3*7 + 4*8 = 5+12+21+32 = 70
    try std.testing.expectApproxEqAbs(@as(f32, 70.0), result, 0.001);
}

test "dot product odd length" {
    const a = [_]f32{ 1, 2, 3, 4, 5 };
    const b = [_]f32{ 2, 3, 4, 5, 6 };
    const result = dotProduct(&a, &b);
    // 2+6+12+20+30 = 70
    try std.testing.expectApproxEqAbs(@as(f32, 70.0), result, 0.001);
}

test "cosine similarity identical vectors" {
    const a = [_]f32{ 1, 0, 0, 0 };
    const na = l2Norm(&a);
    const result = cosineSim(&a, &a, na, na);
    try std.testing.expectApproxEqAbs(@as(f32, 1.0), result, 0.001);
}

test "cosine similarity orthogonal vectors" {
    const a = [_]f32{ 1, 0, 0, 0 };
    const b = [_]f32{ 0, 1, 0, 0 };
    const result = cosineSim(&a, &b, l2Norm(&a), l2Norm(&b));
    try std.testing.expectApproxEqAbs(@as(f32, 0.0), result, 0.001);
}

test "l2 distance" {
    const a = [_]f32{ 0, 0, 0, 0 };
    const b = [_]f32{ 3, 4, 0, 0 };
    const result = l2Distance(&a, &b);
    try std.testing.expectApproxEqAbs(@as(f32, 5.0), result, 0.001);
}

test "vector column append and search" {
    const alloc = std.testing.allocator;
    var col = VectorColumn.init(4);
    defer col.deinit(alloc);

    // Insert 5 vectors
    try col.append(alloc, &.{ 1, 0, 0, 0 });
    try col.append(alloc, &.{ 0, 1, 0, 0 });
    try col.append(alloc, &.{ 0.9, 0.1, 0, 0 });
    try col.append(alloc, &.{ 0, 0, 1, 0 });
    try col.append(alloc, &.{ 0.8, 0.2, 0.1, 0 });

    try std.testing.expectEqual(@as(u32, 5), col.count);

    // Search for vector closest to [1,0,0,0] (cosine)
    const results = try col.search(alloc, &.{ 1, 0, 0, 0 }, 3, .cosine);
    defer alloc.free(results);

    try std.testing.expect(results.len == 3);
    // First result should be index 0 (exact match, score ≈ 1.0)
    try std.testing.expectEqual(@as(u32, 0), results[0].index);
    try std.testing.expectApproxEqAbs(@as(f32, 1.0), results[0].score, 0.001);
}

test "vector column search dot product" {
    const alloc = std.testing.allocator;
    var col = VectorColumn.init(4);
    defer col.deinit(alloc);

    try col.append(alloc, &.{ 1, 2, 3, 4 });
    try col.append(alloc, &.{ 10, 20, 30, 40 });
    try col.append(alloc, &.{ 0, 0, 0, 1 });

    const results = try col.search(alloc, &.{ 1, 1, 1, 1 }, 2, .dot_product);
    defer alloc.free(results);

    // [10,20,30,40] · [1,1,1,1] = 100, highest dot product
    try std.testing.expectEqual(@as(u32, 1), results[0].index);
}

test "vector column search L2" {
    const alloc = std.testing.allocator;
    var col = VectorColumn.init(4);
    defer col.deinit(alloc);

    try col.append(alloc, &.{ 0, 0, 0, 0 });
    try col.append(alloc, &.{ 1, 1, 1, 1 });
    try col.append(alloc, &.{ 10, 10, 10, 10 });

    const results = try col.search(alloc, &.{ 1, 1, 1, 1 }, 2, .l2);
    defer alloc.free(results);

    // Closest to [1,1,1,1] is index 1 (distance 0)
    try std.testing.expectEqual(@as(u32, 1), results[0].index);
    try std.testing.expectApproxEqAbs(@as(f32, 0.0), results[0].score, 0.001);
}

test "dimension mismatch" {
    const alloc = std.testing.allocator;
    var col = VectorColumn.init(4);
    defer col.deinit(alloc);

    try std.testing.expectError(error.DimensionMismatch, col.append(alloc, &.{ 1, 2, 3 }));
}

test "large vector SIMD" {
    const alloc = std.testing.allocator;
    // 128-dim vectors (like a small embedding)
    var col = VectorColumn.init(128);
    defer col.deinit(alloc);

    var v1: [128]f32 = undefined;
    var v2: [128]f32 = undefined;
    for (0..128) |i| {
        v1[i] = @floatFromInt(i);
        v2[i] = @as(f32, @floatFromInt(i)) * 0.5;
    }
    try col.append(alloc, &v1);
    try col.append(alloc, &v2);

    const results = try col.search(alloc, &v1, 2, .cosine);
    defer alloc.free(results);

    // v1 is most similar to itself
    try std.testing.expectEqual(@as(u32, 0), results[0].index);
    try std.testing.expectApproxEqAbs(@as(f32, 1.0), results[0].score, 0.001);
}

test "quantized search returns correct results" {
    const alloc = std.testing.allocator;
    var col = VectorColumn.init(32);
    defer col.deinit(alloc);

    // Insert several vectors
    var vecs: [10][32]f32 = undefined;
    for (0..10) |vi| {
        for (0..32) |d| {
            vecs[vi][d] = if (d == vi) 1.0 else 0.0;
        }
        try col.append(alloc, &vecs[vi]);
    }

    // Enable 4-bit quantization
    try col.enableQuantization(alloc, 4, 42);

    // Search for vector closest to vecs[0]
    const results = try col.searchQuantized(alloc, &vecs[0], 3, .cosine);
    defer alloc.free(results);

    try std.testing.expect(results.len == 3);
    // First result should be index 0
    try std.testing.expectEqual(@as(u32, 0), results[0].index);
}

test "append after quantization also quantizes" {
    const alloc = std.testing.allocator;
    var col = VectorColumn.init(16);
    defer col.deinit(alloc);

    try col.append(alloc, &[_]f32{1} ** 16);
    try col.enableQuantization(alloc, 2, 99);

    // qdata should have bytes for 1 vector
    try std.testing.expectEqual(@as(usize, col.bytes_per_vec), col.qdata.items.len);

    // Append another vector — qdata should grow
    try col.append(alloc, &[_]f32{0.5} ** 16);
    try std.testing.expectEqual(@as(usize, col.bytes_per_vec * 2), col.qdata.items.len);
    try std.testing.expectEqual(@as(u32, 2), col.count);
}
