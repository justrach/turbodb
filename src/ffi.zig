/// TurboDB — C ABI FFI layer
///
/// Exposes TurboDB's core operations through C-compatible exported functions.
/// All pointers use explicit lengths (no null-termination assumed).
/// Error handling uses integer return codes: 0 = success, -1 = error.
///
/// Memory: Uses std.heap.c_allocator (libc malloc/free) so that foreign
/// callers can reason about memory ownership without Zig-specific abstractions.
const std = @import("std");
const collection = @import("collection.zig");
const doc_mod = @import("doc.zig");
const crypto = @import("crypto.zig");
const vector = @import("vector.zig");
const Database = collection.Database;
const Collection = collection.Collection;
const Doc = doc_mod.Doc;

const alloc = std.heap.c_allocator;

// ── Exported C structs ──────────────────────────────────────────────────────

pub const TurboDocResult = extern struct {
    key_ptr: ?[*]const u8,
    key_len: usize,
    val_ptr: ?[*]const u8,
    val_len: usize,
    doc_id: u64,
    version: u8,
    _pad: [7]u8 = .{ 0, 0, 0, 0, 0, 0, 0 },
};

pub const TurboScanHandle = extern struct {
    docs_ptr: ?[*]Doc,
    count: u32,
    _pad: [4]u8 = .{ 0, 0, 0, 0 },
    // Owned by the scan — caller must call turbodb_scan_free.
};

// ── Database lifecycle ──────────────────────────────────────────────────────

/// Open a database at the given directory.
/// Returns an opaque handle, or null on failure.
export fn turbodb_open(dir: [*]const u8, dir_len: usize) ?*anyopaque {
    // Ensure data directory exists.
    const dir_slice = dir[0..dir_len];
    std.fs.cwd().makeDir(dir_slice) catch |e| switch (e) {
        error.PathAlreadyExists => {},
        else => return null,
    };

    const db = Database.open(alloc, dir_slice) catch return null;
    return @ptrCast(db);
}

/// Close a database and free all resources.
export fn turbodb_close(handle: *anyopaque) void {
    const db: *Database = @ptrCast(@alignCast(handle));
    db.close();
}

// ── Collection access ───────────────────────────────────────────────────────

/// Get or create a named collection.
/// Returns an opaque handle, or null on failure.
export fn turbodb_collection(
    db_handle: *anyopaque,
    name: [*]const u8,
    name_len: usize,
) ?*anyopaque {
    const db: *Database = @ptrCast(@alignCast(db_handle));
    const col = db.collection(name[0..name_len]) catch return null;
    return @ptrCast(col);
}

export fn turbodb_collection_tenant(
    db_handle: *anyopaque,
    tenant: [*]const u8,
    tenant_len: usize,
    name: [*]const u8,
    name_len: usize,
) ?*anyopaque {
    const db: *Database = @ptrCast(@alignCast(db_handle));
    const col = db.collectionForTenant(tenant[0..tenant_len], name[0..name_len]) catch return null;
    return @ptrCast(col);
}

/// Drop a named collection.
export fn turbodb_drop_collection(
    db_handle: *anyopaque,
    name: [*]const u8,
    name_len: usize,
) void {
    const db: *Database = @ptrCast(@alignCast(db_handle));
    db.dropCollection(name[0..name_len]);
}

export fn turbodb_drop_collection_tenant(
    db_handle: *anyopaque,
    tenant: [*]const u8,
    tenant_len: usize,
    name: [*]const u8,
    name_len: usize,
) void {
    const db: *Database = @ptrCast(@alignCast(db_handle));
    db.dropCollectionForTenant(tenant[0..tenant_len], name[0..name_len]);
}

// ── Document operations ─────────────────────────────────────────────────────

/// Insert a document. On success, writes the assigned doc_id to `out_id`.
/// Returns 0 on success, -1 on error.
export fn turbodb_insert(
    col_handle: *anyopaque,
    key: [*]const u8,
    key_len: usize,
    val: [*]const u8,
    val_len: usize,
    out_id: *u64,
) c_int {
    const col: *Collection = @ptrCast(@alignCast(col_handle));
    const doc_id = col.insert(key[0..key_len], val[0..val_len]) catch return -1;
    out_id.* = doc_id;
    return 0;
}

/// Get a document by key. On success, fills `out` with pointers into
/// mmap'd memory (valid until next write to this collection).
/// Returns 0 if found, -1 if not found.
export fn turbodb_get(
    col_handle: *anyopaque,
    key: [*]const u8,
    key_len: usize,
    out: *TurboDocResult,
) c_int {
    const col: *Collection = @ptrCast(@alignCast(col_handle));
    const d = col.get(key[0..key_len]) orelse return -1;
    out.* = docToResult(d);
    return 0;
}

/// Update a document by key. Returns 0 on success, -1 if not found.
export fn turbodb_update(
    col_handle: *anyopaque,
    key: [*]const u8,
    key_len: usize,
    val: [*]const u8,
    val_len: usize,
) c_int {
    const col: *Collection = @ptrCast(@alignCast(col_handle));
    const updated = col.update(key[0..key_len], val[0..val_len]) catch return -1;
    return if (updated) 0 else -1;
}

/// Delete a document by key. Returns 0 on success, -1 if not found.
export fn turbodb_delete(
    col_handle: *anyopaque,
    key: [*]const u8,
    key_len: usize,
) c_int {
    const col: *Collection = @ptrCast(@alignCast(col_handle));
    const deleted = col.delete(key[0..key_len]) catch return -1;
    return if (deleted) 0 else -1;
}

// ── Scan ────────────────────────────────────────────────────────────────────

/// Scan a collection. Returns 0 on success, -1 on error.
/// Caller MUST call turbodb_scan_free when done.
export fn turbodb_scan(
    col_handle: *anyopaque,
    limit: u32,
    offset: u32,
    out: *TurboScanHandle,
) c_int {
    const col: *Collection = @ptrCast(@alignCast(col_handle));
    const result = col.scan(limit, offset, alloc) catch return -1;
    out.docs_ptr = result.docs.ptr;
    out.count = @intCast(result.docs.len);
    return 0;
}

/// Number of documents in scan result.
export fn turbodb_scan_count(scan: *const TurboScanHandle) u32 {
    return scan.count;
}

/// Get a document from scan result by index.
/// Returns 0 on success, -1 if index out of bounds.
export fn turbodb_scan_doc(
    scan: *const TurboScanHandle,
    idx: u32,
    out: *TurboDocResult,
) c_int {
    if (idx >= scan.count) return -1;
    const docs_ptr = scan.docs_ptr orelse return -1;
    out.* = docToResult(docs_ptr[idx]);
    return 0;
}

/// Free scan result memory.
export fn turbodb_scan_free(scan: *TurboScanHandle) void {
    if (scan.docs_ptr) |ptr| {
        alloc.free(ptr[0..scan.count]);
    }
    scan.docs_ptr = null;
    scan.count = 0;
}

// ── Helpers ─────────────────────────────────────────────────────────────────

fn docToResult(d: Doc) TurboDocResult {
    return .{
        .key_ptr = d.key.ptr,
        .key_len = d.key.len,
        .val_ptr = d.value.ptr,
        .val_len = d.value.len,
        .doc_id = d.header.doc_id,
        .version = d.header.version,
    };
}

// ── Search (trigram-indexed) ─────────────────────────────────────────────────

/// Search documents by substring using trigram index.
/// Returns scan handle with matching docs. Free with turbodb_scan_free.
export fn turbodb_search(
    col_handle: *anyopaque,
    query_ptr: [*]const u8,
    query_len: usize,
    limit: u32,
    out: *TurboScanHandle,
) c_int {
    const col: *Collection = @ptrCast(@alignCast(col_handle));
    const query = query_ptr[0..query_len];
    const result = col.searchText(query, limit, alloc) catch return -1;
    out.docs_ptr = result.docs.ptr;
    out.count = @intCast(result.docs.len);
    return 0;
}

export fn turbodb_flush_index(col_handle: *anyopaque) void {
    const col: *Collection = @ptrCast(@alignCast(col_handle));
    col.flushIndex();
}

// ── Version info ────────────────────────────────────────────────────────────

export fn turbodb_version() [*:0]const u8 {
    return "0.1.0";
}

// ── Crypto ──────────────────────────────────────────────────────────────────
// SHA-256, SHA-512, BLAKE3, HMAC-SHA256, Ed25519.

export fn turbodb_sha256(data: [*]const u8, len: usize, out: *[32]u8) void {
    out.* = crypto.sha256(data[0..len]);
}
export fn turbodb_sha256_hex(data: [*]const u8, len: usize, out: *[64]u8) void {
    out.* = crypto.sha256Hex(data[0..len]);
}
export fn turbodb_sha512(data: [*]const u8, len: usize, out: *[64]u8) void {
    out.* = crypto.sha512(data[0..len]);
}
export fn turbodb_blake3(data: [*]const u8, len: usize, out: *[32]u8) void {
    out.* = crypto.blake3(data[0..len]);
}
export fn turbodb_blake3_hex(data: [*]const u8, len: usize, out: *[64]u8) void {
    out.* = crypto.blake3Hex(data[0..len]);
}
export fn turbodb_hmac_sha256(key: [*]const u8, klen: usize, data: [*]const u8, dlen: usize, out: *[32]u8) void {
    out.* = crypto.hmacSha256(key[0..klen], data[0..dlen]);
}
export fn turbodb_hmac_sha256_hex(key: [*]const u8, klen: usize, data: [*]const u8, dlen: usize, out: *[64]u8) void {
    out.* = crypto.hmacSha256Hex(key[0..klen], data[0..dlen]);
}
export fn turbodb_ed25519_keygen(pub_out: *[32]u8, sec_out: *[64]u8) void {
    const kp = crypto.KeyPair.generate();
    pub_out.* = kp.public_key;
    sec_out.* = kp.secret_key;
}
export fn turbodb_ed25519_sign(msg: [*]const u8, msg_len: usize, sk: *const [64]u8, sig: *[64]u8) void {
    sig.* = crypto.ed25519Sign(msg[0..msg_len], sk.*);
}
export fn turbodb_ed25519_verify(msg: [*]const u8, msg_len: usize, sig: *const [64]u8, pk: *const [32]u8) c_int {
    return if (crypto.ed25519Verify(msg[0..msg_len], sig.*, pk.*)) 0 else -1;
}

// ── Vector column ───────────────────────────────────────────────────────────
// SIMD-accelerated vector similarity search (cosine, dot product, L2).

/// Create a vector column with given dimensionality.
export fn turbodb_vector_create(dims: u32) ?*anyopaque {
    const col = alloc.create(vector.VectorColumn) catch return null;
    col.* = vector.VectorColumn.init(dims);
    return @ptrCast(col);
}

/// Free a vector column.
export fn turbodb_vector_free(handle: *anyopaque) void {
    const col: *vector.VectorColumn = @ptrCast(@alignCast(handle));
    col.deinit(alloc);
    alloc.destroy(col);
}

/// Append a vector (dims floats).
export fn turbodb_vector_append(handle: *anyopaque, data: [*]const f32, dims: u32) c_int {
    const col: *vector.VectorColumn = @ptrCast(@alignCast(handle));
    col.append(alloc, data[0..dims]) catch return -1;
    return 0;
}

/// Batch append: insert n_vecs vectors at once (n_vecs * dims floats contiguous).
export fn turbodb_vector_append_batch(handle: *anyopaque, data: [*]const f32, dims: u32, n_vecs: u32) c_int {
    const col: *vector.VectorColumn = @ptrCast(@alignCast(handle));
    col.appendBatch(alloc, data[0 .. @as(usize, n_vecs) * dims], n_vecs) catch return -1;
    return 0;
}

/// Search for top-K similar vectors. Results written to out_indices/out_scores.
/// metric: 0=cosine, 1=dot_product, 2=l2. Returns actual result count.
export fn turbodb_vector_search(
    handle: *anyopaque,
    query: [*]const f32,
    dims: u32,
    k: u32,
    metric: u8,
    out_indices: [*]u32,
    out_scores: [*]f32,
) c_int {
    const col: *const vector.VectorColumn = @ptrCast(@alignCast(handle));
    const m: vector.Metric = switch (metric) {
        0 => .cosine,
        1 => .dot_product,
        2 => .l2,
        else => return -1,
    };
    const results = col.search(alloc, query[0..dims], k, m) catch return -1;
    defer alloc.free(results);
    for (results, 0..) |r, i| {
        out_indices[i] = r.index;
        out_scores[i] = r.score;
    }
    return @intCast(results.len);
}

/// Get vector count.
export fn turbodb_vector_count(handle: *anyopaque) u32 {
    const col: *const vector.VectorColumn = @ptrCast(@alignCast(handle));
    return col.count;
}

/// Get vector column memory usage in bytes.
export fn turbodb_vector_memory(handle: *anyopaque) usize {
    const col: *const vector.VectorColumn = @ptrCast(@alignCast(handle));
    return col.memoryBytes();
}

/// Enable quantization on a vector column.
export fn turbodb_vector_enable_quantization(handle: *anyopaque, bit_width: u8, seed: u64) c_int {
    const col: *vector.VectorColumn = @ptrCast(@alignCast(handle));
    col.enableQuantization(alloc, bit_width, seed) catch return -1;
    return 0;
}

/// Enable quantized-only mode: FWHT rotation, no FP32 storage.
export fn turbodb_vector_enable_quantized_only(handle: *anyopaque, bit_width: u8, seed: u64) c_int {
    const col: *vector.VectorColumn = @ptrCast(@alignCast(handle));
    col.enableQuantizedOnly(alloc, bit_width, seed) catch return -1;
    return 0;
}

/// Quantized search: fast asymmetric scan + FP32 re-rank.
/// metric: 0=cosine, 1=dot_product, 2=l2. Returns actual result count.
export fn turbodb_vector_search_quantized(
    handle: *anyopaque,
    query: [*]const f32,
    dims: u32,
    k: u32,
    metric: u8,
    out_indices: [*]u32,
    out_scores: [*]f32,
) c_int {
    const col: *const vector.VectorColumn = @ptrCast(@alignCast(handle));
    const m: vector.Metric = switch (metric) {
        0 => .cosine,
        1 => .dot_product,
        2 => .l2,
        else => return -1,
    };
    const results = col.searchQuantized(alloc, query[0..dims], k, m) catch return -1;
    defer alloc.free(results);
    for (results, 0..) |r, i| {
        out_indices[i] = r.index;
        out_scores[i] = r.score;
    }
    return @intCast(results.len);
}

/// Enable INT8 direct distance computation on a vector column.
export fn turbodb_vector_enable_int8(handle: *anyopaque, seed: u64) c_int {
    const col: *vector.VectorColumn = @ptrCast(@alignCast(handle));
    col.enableInt8(alloc, seed) catch return -1;
    return 0;
}

/// INT8 SIMD search: quantize query to i8, scan with 16-lane SIMD dot product.
/// metric: 0=cosine, 1=dot_product, 2=l2. Returns actual result count.
export fn turbodb_vector_search_int8(
    handle: *anyopaque,
    query: [*]const f32,
    dims: u32,
    k: u32,
    metric: u8,
    out_indices: [*]u32,
    out_scores: [*]f32,
) c_int {
    const col: *const vector.VectorColumn = @ptrCast(@alignCast(handle));
    const m: vector.Metric = switch (metric) {
        0 => .cosine,
        1 => .dot_product,
        2 => .l2,
        else => return -1,
    };
    const results = col.searchInt8(alloc, query[0..dims], k, m) catch return -1;
    defer alloc.free(results);
    for (results, 0..) |r, i| {
        out_indices[i] = r.index;
        out_scores[i] = r.score;
    }
    return @intCast(results.len);
}

/// Parallel INT8 SIMD search: splits scan across multiple threads.
/// metric: 0=cosine, 1=dot_product, 2=l2. Returns actual result count.
export fn turbodb_vector_search_int8_parallel(
    handle: *anyopaque,
    query: [*]const f32,
    dims: u32,
    k: u32,
    metric: u8,
    out_indices: [*]u32,
    out_scores: [*]f32,
) c_int {
    const col: *const vector.VectorColumn = @ptrCast(@alignCast(handle));
    const m: vector.Metric = switch (metric) {
        0 => .cosine,
        1 => .dot_product,
        2 => .l2,
        else => return -1,
    };
    const results = col.searchInt8Parallel(alloc, query[0..dims], k, m) catch return -1;
    defer alloc.free(results);
    for (results, 0..) |r, i| {
        out_indices[i] = r.index;
        out_scores[i] = r.score;
    }
    return @intCast(results.len);
}

/// Build IVF (Inverted File Index) for approximate nearest neighbor search.
export fn turbodb_vector_build_ivf(handle: *anyopaque, n_clusters: u32, seed: u64) c_int {
    const col: *vector.VectorColumn = @ptrCast(@alignCast(handle));
    col.buildIVF(alloc, n_clusters, seed) catch return -1;
    return 0;
}

/// IVF search: probe only n_probes nearest clusters.
/// metric: 0=cosine, 1=dot_product, 2=l2. Returns actual result count.
export fn turbodb_vector_search_ivf(
    handle: *anyopaque,
    query: [*]const f32,
    dims: u32,
    k: u32,
    metric: u8,
    n_probes: u32,
    out_indices: [*]u32,
    out_scores: [*]f32,
) c_int {
    const col: *const vector.VectorColumn = @ptrCast(@alignCast(handle));
    const m: vector.Metric = switch (metric) {
        0 => .cosine,
        1 => .dot_product,
        2 => .l2,
        else => return -1,
    };
    const results = col.searchIVF(alloc, query[0..dims], k, m, n_probes) catch return -1;
    defer alloc.free(results);
    for (results, 0..) |r, i| {
        out_indices[i] = r.index;
        out_scores[i] = r.score;
    }
    return @intCast(results.len);
}
