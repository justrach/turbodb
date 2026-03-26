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

/// Drop a named collection.
export fn turbodb_drop_collection(
    db_handle: *anyopaque,
    name: [*]const u8,
    name_len: usize,
) void {
    const db: *Database = @ptrCast(@alignCast(db_handle));
    db.dropCollection(name[0..name_len]);
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

// ── Version info ────────────────────────────────────────────────────────────

export fn turbodb_version() [*:0]const u8 {
    return "0.1.0";
}
