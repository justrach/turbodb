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
const compat = @import("compat");
const runtime = @import("runtime");
const Database = collection.Database;
const Collection = collection.Collection;
const Doc = doc_mod.Doc;

const alloc = std.heap.c_allocator;

// ── Handle wrappers with magic sentinel for validation ──────────────────────

const DB_MAGIC: u64 = 0xDB_7042_B0DB_0001;
const COL_MAGIC: u64 = 0xC0_11EC_7100_0001;

const DbHandle = struct {
    magic: u64 = DB_MAGIC,
    ptr: *Database,
};

const ColHandle = struct {
    magic: u64 = COL_MAGIC,
    ptr: *Collection,
};

fn validateDbHandle(handle: ?*anyopaque) ?*Database {
    const h: *DbHandle = @ptrCast(@alignCast(handle orelse return null));
    if (h.magic != DB_MAGIC) return null;
    return h.ptr;
}

fn validateColHandle(handle: ?*anyopaque) ?*Collection {
    const h: *ColHandle = @ptrCast(@alignCast(handle orelse return null));
    if (h.magic != COL_MAGIC) return null;
    return h.ptr;
}

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
    // FFI consumers don't go through std.process.Init; bootstrap runtime.io
    // before any compat/Io call. Idempotent, so cheap to call per-open.
    runtime.init(alloc);

    // Ensure data directory exists.
    const dir_slice = dir[0..dir_len];
    compat.fs.cwdMakeDir(dir_slice) catch |e| switch (e) {
        error.PathAlreadyExists => {},
        else => return null,
    };

    const db = Database.open(alloc, dir_slice) catch return null;
    const handle = alloc.create(DbHandle) catch return null;
    handle.* = .{ .ptr = db };
    return @ptrCast(handle);
}

/// Close a database and free all resources.
export fn turbodb_close(handle: *anyopaque) void {
    const db = validateDbHandle(handle) orelse return;
    db.close();
    const h: *DbHandle = @ptrCast(@alignCast(handle));
    h.magic = 0; // poison the handle
    alloc.destroy(h);
}

// ── Collection access ───────────────────────────────────────────────────────

/// Get or create a named collection.
/// Returns an opaque handle, or null on failure.
export fn turbodb_collection(
    db_handle: *anyopaque,
    name: [*]const u8,
    name_len: usize,
) ?*anyopaque {
    const db = validateDbHandle(db_handle) orelse return null;
    const col = db.collection(name[0..name_len]) catch return null;
    const handle = alloc.create(ColHandle) catch return null;
    handle.* = .{ .ptr = col };
    return @ptrCast(handle);
}

export fn turbodb_collection_tenant(
    db_handle: *anyopaque,
    tenant: [*]const u8,
    tenant_len: usize,
    name: [*]const u8,
    name_len: usize,
) ?*anyopaque {
    const db = validateDbHandle(db_handle) orelse return null;
    const col = db.collectionForTenant(tenant[0..tenant_len], name[0..name_len]) catch return null;
    const handle = alloc.create(ColHandle) catch return null;
    handle.* = .{ .ptr = col };
    return @ptrCast(handle);
}

/// Drop a named collection.
export fn turbodb_drop_collection(
    db_handle: *anyopaque,
    name: [*]const u8,
    name_len: usize,
) void {
    const db = validateDbHandle(db_handle) orelse return;
    db.dropCollection(name[0..name_len]);
}

export fn turbodb_drop_collection_tenant(
    db_handle: *anyopaque,
    tenant: [*]const u8,
    tenant_len: usize,
    name: [*]const u8,
    name_len: usize,
) void {
    const db = validateDbHandle(db_handle) orelse return;
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
    const col = validateColHandle(col_handle) orelse return -1;
    const doc_id = col.insert(key[0..key_len], val[0..val_len]) catch return -1;
    out_id.* = doc_id;
    return 0;
}

/// Insert with a pre-computed embedding (no JSON parsing). Fast path for vector inserts.
export fn turbodb_insert_with_embedding(
    col_handle: *anyopaque,
    key: [*]const u8, key_len: usize,
    val: [*]const u8, val_len: usize,
    embedding: [*]const f32, dims: u32,
    out_id: *u64,
) c_int {
    const col = validateColHandle(col_handle) orelse return -1;
    const doc_id = col.insertWithEmbedding(key[0..key_len], val[0..val_len], embedding[0..dims]) catch return -1;
    out_id.* = doc_id;
    return 0;
}

/// Batch insert from a packed buffer. Wire format (caller packs):
///   repeated { u32 key_len | key_bytes | u32 val_len | val_bytes }
/// `count` records must be present. `out_ids` receives the doc_id for each,
/// in input order. Returns 0 on full success, -1 on any parse/insert failure
/// (after which `out_ids[0..N]` contains the IDs of the N items that
/// succeeded before the failure; caller reads `*out_inserted` to know N).
///
/// Saves (count - 1) boundary crossings vs looping `turbodb_insert` from the
/// host language.
export fn turbodb_insert_many(
    col_handle: *anyopaque,
    packed_ptr: [*]const u8,
    packed_len: usize,
    count: u32,
    out_ids: [*]u64,
    out_inserted: *u32,
) c_int {
    const col = validateColHandle(col_handle) orelse return -1;
    const buf = packed_ptr[0..packed_len];

    var off: usize = 0;
    var i: u32 = 0;
    out_inserted.* = 0;
    while (i < count) : (i += 1) {
        if (off + 4 > buf.len) return -1;
        const key_len = std.mem.readInt(u32, buf[off..][0..4], .little);
        off += 4;
        if (off + key_len > buf.len) return -1;
        const key = buf[off..][0..key_len];
        off += key_len;

        if (off + 4 > buf.len) return -1;
        const val_len = std.mem.readInt(u32, buf[off..][0..4], .little);
        off += 4;
        if (off + val_len > buf.len) return -1;
        const val = buf[off..][0..val_len];
        off += val_len;

        const doc_id = col.insert(key, val) catch return -1;
        out_ids[i] = doc_id;
        out_inserted.* = i + 1;
    }
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
    const col = validateColHandle(col_handle) orelse return -1;
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
    const col = validateColHandle(col_handle) orelse return -1;
    const updated = col.update(key[0..key_len], val[0..val_len]) catch return -1;
    return if (updated) 0 else -1;
}

/// Delete a document by key. Returns 0 on success, -1 if not found.
export fn turbodb_delete(
    col_handle: *anyopaque,
    key: [*]const u8,
    key_len: usize,
) c_int {
    const col = validateColHandle(col_handle) orelse return -1;
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
    const col = validateColHandle(col_handle) orelse return -1;
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
    const col = validateColHandle(col_handle) orelse return -1;
    const query = query_ptr[0..query_len];
    const result = col.searchText(query, limit, alloc) catch return -1;
    out.docs_ptr = result.docs.ptr;
    out.count = @intCast(result.docs.len);
    return 0;
}

export fn turbodb_flush_index(col_handle: *anyopaque) void {
    const col = validateColHandle(col_handle) orelse return;
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

// ── Collection-level vector integration ─────────────────────────────────────

/// Configure a vector embedding column on a Collection.
/// field_name is the JSON key that holds the embedding array (e.g. "embedding").
export fn turbodb_configure_vectors(col_handle: *anyopaque, dims: u32, field_name: [*]const u8, field_len: u32) c_int {
    const col = validateColHandle(col_handle) orelse return -1;
    col.configureVectors(dims, field_name[0..field_len]) catch return -1;
    return 0;
}

/// Search the collection's vector column. Returns result count written to out arrays.
/// metric: 0=cosine, 1=dot_product, 2=l2.
export fn turbodb_collection_search_vectors(
    col_handle: *anyopaque,
    query: [*]const f32,
    dims: u32,
    k: u32,
    metric: u8,
    out_indices: [*]u32,
    out_scores: [*]f32,
) c_int {
    const col = validateColHandle(col_handle) orelse return -1;
    const m: vector.Metric = switch (metric) {
        0 => .cosine,
        1 => .dot_product,
        2 => .l2,
        else => return -1,
    };
    const result = col.searchVectors(query[0..dims], k, m) catch return -1;
    defer result.deinit();
    for (result.docs[0..result.count], 0..) |d, i| {
        out_indices[i] = @intCast(d.header.doc_id);
        out_scores[i] = result.scores[i];
    }
    return @intCast(result.count);
}

/// Hybrid search: combines text + vector scoring.
/// Returns results as a ScanHandle (use turbodb_scan_count/doc/free).
export fn turbodb_search_hybrid(
    col_handle: *anyopaque,
    text_query: [*]const u8,
    text_len: u32,
    vector_query: [*]const f32,
    dims: u32,
    k: u32,
    text_weight: f32,
    vector_weight: f32,
    out: *TurboScanHandle,
) c_int {
    const col = validateColHandle(col_handle) orelse return -1;
    const result = col.searchHybrid(
        text_query[0..text_len],
        vector_query[0..dims],
        text_weight,
        vector_weight,
        k,
        alloc,
    ) catch return -1;
    out.docs_ptr = result.docs.ptr;
    out.count = @intCast(result.docs.len);
    return 0;
}

// ── Branch operations ────────────────────────────────────────────────────────

export fn turbodb_enable_branching(col_handle: *anyopaque) c_int {
    const col = validateColHandle(col_handle) orelse return -1;
    col.enableBranching() catch return -1;
    return 0;
}

export fn turbodb_create_branch(col_handle: *anyopaque, name_ptr: [*]const u8, name_len: u32, agent_ptr: [*]const u8, agent_len: u32) c_int {
    const col = validateColHandle(col_handle) orelse return -1;
    _ = col.createBranch(name_ptr[0..name_len], agent_ptr[0..agent_len]) catch return -1;
    return 0;
}

export fn turbodb_branch_write(col_handle: *anyopaque, branch_name: [*]const u8, branch_len: u32, key: [*]const u8, key_len: u32, val: [*]const u8, val_len: u32) c_int {
    const col = validateColHandle(col_handle) orelse return -1;
    const br = col.getBranch(branch_name[0..branch_len]) orelse return -1;
    col.writeOnBranch(br, key[0..key_len], val[0..val_len]) catch return -1;
    return 0;
}

export fn turbodb_branch_read(col_handle: *anyopaque, branch_name: [*]const u8, branch_len: u32, key: [*]const u8, key_len: u32, out_val: *[*]const u8, out_len: *u32) c_int {
    const col = validateColHandle(col_handle) orelse return -1;
    const br = col.getBranch(branch_name[0..branch_len]) orelse return -1;
    const val = col.getOnBranch(br, key[0..key_len]) orelse return -1;
    out_val.* = val.ptr;
    out_len.* = @intCast(val.len);
    return 0;
}

export fn turbodb_branch_merge(col_handle: *anyopaque, branch_name: [*]const u8, branch_len: u32) c_int {
    const col = validateColHandle(col_handle) orelse return -1;
    const br = col.getBranch(branch_name[0..branch_len]) orelse return -1;
    var result = col.mergeBranch(br, alloc) catch return -1;
    defer result.deinit();
    if (result.conflicts.len > 0) return @intCast(result.conflicts.len); // positive = conflict count
    return 0; // 0 = success, no conflicts
}

/// Get line-level diff for a branch. Returns JSON with per-file diffs.
/// Format: {"files":[{"key":"src/auth.zig","lines":[{"no":1,"kind":"same","text":"..."},{"no":2,"kind":"added","text":"..."},...]},...]}
export fn turbodb_branch_diff(
    col_handle: *anyopaque,
    branch_name: [*]const u8,
    branch_len: u32,
    out_json: *[*]u8,
    out_len: *u32,
) c_int {
    const col = validateColHandle(col_handle) orelse return -1;
    const br = col.getBranch(branch_name[0..branch_len]) orelse return -1;
    const branch_mod = @import("branch.zig");

    var aw: std.Io.Writer.Allocating = .init(alloc);
    defer aw.deinit();
    const w = &aw.writer;
    w.writeAll("{\"files\":[") catch return -1;

    var first_file = true;
    var it = br.writes.iterator();
    while (it.next()) |entry| {
        const bw = entry.value_ptr.*;
        if (bw.deleted) continue;

        if (!first_file) w.writeByte(',') catch return -1;
        first_file = false;

        // Get main version for comparison
        const main_doc = col.get(bw.key);
        const old_val = if (main_doc) |d| d.value else "";

        // Compute line diff
        const diffs = branch_mod.lineDiff(old_val, bw.value, alloc) catch return -1;
        defer alloc.free(diffs);

        w.writeAll("{\"key\":\"") catch return -1;
        w.writeAll(bw.key) catch return -1;
        w.writeAll("\",\"lines\":[") catch return -1;

        for (diffs, 0..) |d, di| {
            if (di > 0) w.writeByte(',') catch return -1;
            const kind_str = switch (d.kind) {
                .same => "same",
                .added => "added",
                .removed => "removed",
            };
            w.print("{{\"no\":{d},\"kind\":\"{s}\",\"text\":\"", .{ d.line_no, kind_str }) catch return -1;
            // Escape text for JSON
            for (d.text) |c| {
                switch (c) {
                    '"' => w.writeAll("\\\"") catch return -1,
                    '\\' => w.writeAll("\\\\") catch return -1,
                    '\n' => w.writeAll("\\n") catch return -1,
                    '\r' => w.writeAll("\\r") catch return -1,
                    '\t' => w.writeAll("\\t") catch return -1,
                    else => w.writeByte(c) catch return -1,
                }
            }
            w.writeAll("\"}") catch return -1;
        }
        w.writeAll("]}") catch return -1;
    }

    w.writeAll("]}") catch return -1;
    const json = aw.toOwnedSlice() catch return -1;
    out_json.* = json.ptr;
    out_len.* = @intCast(json.len);
    return 0;
}

export fn turbodb_branch_search(
    col_handle: *anyopaque,
    branch_name: [*]const u8,
    branch_len: u32,
    query_ptr: [*]const u8,
    query_len: u32,
    limit: u32,
    out: *TurboScanHandle,
) c_int {
    const col = validateColHandle(col_handle) orelse return -1;
    const br = col.getBranch(branch_name[0..branch_len]) orelse return -1;
    const result = col.searchOnBranch(br, query_ptr[0..query_len], limit, alloc) catch return -1;
    out.docs_ptr = result.docs.ptr;
    out.count = @intCast(result.docs.len);
    return 0;
}

export fn turbodb_list_branches(
    col_handle: *anyopaque,
    out_json: *[*]u8,
    out_len: *u32,
) c_int {
    const col = validateColHandle(col_handle) orelse return -1;
    const names = col.listBranches(alloc) catch return -1;
    defer alloc.free(names);

    // Build JSON array of branch names
    var buf: std.ArrayList(u8) = .empty;
    buf.append(alloc, '[') catch return -1;
    for (names, 0..) |name, i| {
        if (i > 0) buf.append(alloc, ',') catch return -1;
        buf.append(alloc, '"') catch return -1;
        buf.appendSlice(alloc, name) catch return -1;
        buf.append(alloc, '"') catch return -1;
    }
    buf.append(alloc, ']') catch return -1;

    const owned = buf.toOwnedSlice(alloc) catch return -1;
    out_json.* = owned.ptr;
    out_len.* = @intCast(owned.len);
    return 0;
}

export fn turbodb_free_json(ptr: [*]u8, len: u32) void {
    alloc.free(ptr[0..len]);
}

/// Discover context for an agent task — returns matching files, callers, tests in one call.
/// Output is a JSON string with the context.
export fn turbodb_discover_context(
    col_handle: *anyopaque,
    query: [*]const u8,
    query_len: u32,
    limit: u32,
    out_json: *[*]u8,
    out_len: *u32,
) c_int {
    const col = validateColHandle(col_handle) orelse return -1;
    var result = col.discoverContext(query[0..query_len], limit, alloc) catch return -1;
    defer result.deinit();

    // Format as JSON
    var aw: std.Io.Writer.Allocating = .init(alloc);
    defer aw.deinit();
    const w = &aw.writer;

    w.writeAll("{\"matching_files\":[") catch return -1;
    for (result.matching_files, 0..) |d, i| {
        if (i > 0) w.writeByte(',') catch return -1;
        w.print("{{\"key\":\"{s}\",\"size\":{d}}}", .{ d.key, d.value.len }) catch return -1;
    }
    w.writeAll("],\"related_files\":[") catch return -1;
    for (result.related_files, 0..) |d, i| {
        if (i > 0) w.writeByte(',') catch return -1;
        w.print("{{\"key\":\"{s}\"}}", .{d.key}) catch return -1;
    }
    w.writeAll("],\"test_files\":[") catch return -1;
    for (result.test_files, 0..) |d, i| {
        if (i > 0) w.writeByte(',') catch return -1;
        w.print("{{\"key\":\"{s}\"}}", .{d.key}) catch return -1;
    }
    w.print("],\"recent_versions\":{d},\"total_files\":{d}}}", .{ result.recent_versions, result.total_files }) catch return -1;
    const json = aw.toOwnedSlice() catch return -1;
    // Copy to output
    out_json.* = json.ptr;
    out_len.* = @intCast(json.len);
    return 0;
}
