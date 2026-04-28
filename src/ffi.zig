/// TurboDB — C ABI FFI layer
///
/// Exposes TurboDB's core operations through C-compatible exported functions.
/// All pointers use explicit lengths (no null-termination assumed).
/// Error handling uses integer return codes: 0 = success, -1 = error.
///
/// Memory: Uses std.heap.c_allocator (libc malloc/free) so that foreign
/// callers can reason about memory ownership without Zig-specific abstractions.
const std = @import("std");
const compat = @import("compat");
const collection = @import("collection.zig");
const doc_mod = @import("doc.zig");
const crypto = @import("crypto.zig");
const vector = @import("vector.zig");
const Database = collection.Database;
const Collection = collection.Collection;
const Doc = doc_mod.Doc;

const alloc = std.heap.c_allocator;
const BULK_INSERT_CHUNK_ROWS: usize = 4096;
const BULK_INSERT_CHUNK_BYTES: usize = 4 * 1024 * 1024;

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
    // Ensure data directory exists.
    const dir_slice = dir[0..dir_len];
    compat.cwd().makeDir(dir_slice) catch |e| switch (e) {
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

/// Bulk insert NDJSON lines shaped as {"key":"...","value":...}.
/// Returns 0 if the request was processed; per-row failures are counted.
export fn turbodb_insert_bulk_ndjson(
    col_handle: *anyopaque,
    body: [*]const u8,
    body_len: usize,
    out_inserted: *u32,
    out_errors: *u32,
) c_int {
    const col = validateColHandle(col_handle) orelse return -1;
    var inserted: u32 = 0;
    var errors: u32 = 0;
    const input = body[0..body_len];
    var rows: std.ArrayList(Collection.BulkInsertRow) = .empty;
    defer rows.deinit(alloc);
    var chunk_bytes: usize = 0;

    const BulkFlush = struct {
        fn run(
            col_arg: *Collection,
            rows_arg: *std.ArrayList(Collection.BulkInsertRow),
            chunk_bytes_arg: *usize,
            inserted_arg: *u32,
            errors_arg: *u32,
        ) !void {
            if (rows_arg.items.len == 0) return;
            const result = try col_arg.insertBulk(rows_arg.items);
            inserted_arg.* += result.inserted;
            errors_arg.* += result.errors;
            rows_arg.clearRetainingCapacity();
            chunk_bytes_arg.* = 0;
        }
    };

    var pos: usize = 0;
    while (pos < input.len) {
        const line_end = std.mem.indexOfScalarPos(u8, input, pos, '\n') orelse input.len;
        const line = std.mem.trim(u8, input[pos..line_end], " \t\r");
        pos = if (line_end < input.len) line_end + 1 else line_end;
        if (line.len == 0) continue;

        const parsed = parseBulkLine(line) orelse {
            errors += 1;
            continue;
        };
        const key = parsed.key;
        const value = parsed.value;
        const row_bytes = key.len + value.len + 128;
        if (rows.items.len > 0 and chunk_bytes + row_bytes > BULK_INSERT_CHUNK_BYTES) {
            BulkFlush.run(col, &rows, &chunk_bytes, &inserted, &errors) catch return -1;
        }
        rows.append(alloc, .{ .key = key, .value = value, .line_len = line.len }) catch return -1;
        chunk_bytes += row_bytes;

        if (rows.items.len >= BULK_INSERT_CHUNK_ROWS) {
            BulkFlush.run(col, &rows, &chunk_bytes, &inserted, &errors) catch return -1;
        }
    }
    BulkFlush.run(col, &rows, &chunk_bytes, &inserted, &errors) catch return -1;

    out_inserted.* = inserted;
    out_errors.* = errors;
    return 0;
}

/// Insert with a pre-computed embedding (no JSON parsing). Fast path for vector inserts.
export fn turbodb_insert_with_embedding(
    col_handle: *anyopaque,
    key: [*]const u8,
    key_len: usize,
    val: [*]const u8,
    val_len: usize,
    embedding: [*]const f32,
    dims: u32,
    out_id: *u64,
) c_int {
    const col = validateColHandle(col_handle) orelse return -1;
    const doc_id = col.insertWithEmbedding(key[0..key_len], val[0..val_len], embedding[0..dims]) catch return -1;
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
    const col = validateColHandle(col_handle) orelse return -1;
    const d = col.get(key[0..key_len]) orelse return -1;
    out.* = docToResult(d);
    return 0;
}

/// Read many newline-delimited or JSON-array keys in one FFI call.
/// The output reports found/missing counts and total key+value bytes read.
export fn turbodb_get_many_keys(
    col_handle: *anyopaque,
    keys_body: [*]const u8,
    keys_len: usize,
    out_found: *u32,
    out_missing: *u32,
    out_bytes: *usize,
) c_int {
    const col = validateColHandle(col_handle) orelse return -1;
    var iter = KeyIter.init(keys_body[0..keys_len]);
    var found: u32 = 0;
    var missing: u32 = 0;
    var bytes_read: usize = 0;

    while (iter.next()) |key| {
        if (key.len == 0) continue;
        if (col.get(key)) |d| {
            found += 1;
            bytes_read += d.key.len + d.value.len;
        } else {
            missing += 1;
        }
    }

    out_found.* = found;
    out_missing.* = missing;
    out_bytes.* = bytes_read;
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

const KeyIter = struct {
    body: []const u8,
    pos: usize = 0,
    array_mode: bool = false,

    fn init(body: []const u8) KeyIter {
        const trimmed = std.mem.trim(u8, body, " \t\r\n");
        if (trimmed.len == 0) return .{ .body = trimmed };
        if (trimmed[0] == '{') {
            if (jsonValue(trimmed, "keys")) |keys| {
                if (keys.len > 0 and keys[0] == '[') {
                    return .{ .body = keys, .pos = 1, .array_mode = true };
                }
            }
        }
        if (trimmed[0] == '[') return .{ .body = trimmed, .pos = 1, .array_mode = true };
        return .{ .body = trimmed };
    }

    fn next(self: *KeyIter) ?[]const u8 {
        if (self.array_mode) return self.nextArray();
        return self.nextLine();
    }

    fn nextLine(self: *KeyIter) ?[]const u8 {
        while (self.pos < self.body.len) {
            const line_end = std.mem.indexOfScalarPos(u8, self.body, self.pos, '\n') orelse self.body.len;
            const raw_line = std.mem.trim(u8, self.body[self.pos..line_end], " \t\r");
            self.pos = if (line_end < self.body.len) line_end + 1 else line_end;
            if (raw_line.len == 0) continue;
            if (raw_line[0] == '{') return jsonStr(raw_line, "key") orelse continue;
            if (raw_line.len >= 2 and raw_line[0] == '"' and raw_line[raw_line.len - 1] == '"')
                return raw_line[1 .. raw_line.len - 1];
            return raw_line;
        }
        return null;
    }

    fn nextArray(self: *KeyIter) ?[]const u8 {
        while (self.pos < self.body.len) {
            while (self.pos < self.body.len and
                (self.body[self.pos] == ' ' or self.body[self.pos] == '\t' or
                    self.body[self.pos] == '\r' or self.body[self.pos] == '\n' or
                    self.body[self.pos] == ',')) : (self.pos += 1)
            {}
            if (self.pos >= self.body.len or self.body[self.pos] == ']') return null;

            if (self.body[self.pos] == '"') {
                const start = self.pos + 1;
                var i = start;
                while (i < self.body.len) : (i += 1) {
                    if (self.body[i] == '\\' and i + 1 < self.body.len) {
                        i += 1;
                        continue;
                    }
                    if (self.body[i] == '"') {
                        self.pos = i + 1;
                        return self.body[start..i];
                    }
                }
                self.pos = self.body.len;
                return null;
            }

            const start = self.pos;
            while (self.pos < self.body.len and self.body[self.pos] != ',' and self.body[self.pos] != ']') : (self.pos += 1) {}
            const raw_key = std.mem.trim(u8, self.body[start..self.pos], " \t\r\n");
            if (raw_key.len == 0) continue;
            return raw_key;
        }
        return null;
    }
};

const BulkLine = struct {
    key: []const u8,
    value: []const u8,
};

fn parseBulkLine(line: []const u8) ?BulkLine {
    if (parseBulkLineFast(line)) |parsed| return parsed;
    const key = jsonStr(line, "key") orelse return null;
    const value = jsonValue(line, "value") orelse line;
    return .{ .key = key, .value = value };
}

fn parseBulkLineFast(line: []const u8) ?BulkLine {
    if (line.len < "{\"key\":\"\",\"value\":}".len or line[0] != '{') return null;
    var row_end = line.len;
    while (row_end > 0 and (line[row_end - 1] == ' ' or line[row_end - 1] == '\t' or line[row_end - 1] == '\r')) row_end -= 1;
    if (row_end == 0 or line[row_end - 1] != '}') return null;
    row_end -= 1;

    var i: usize = 1;
    skipJsonSpaces(line, &i, row_end);
    if (!std.mem.startsWith(u8, line[i..row_end], "\"key\"")) return null;
    i += "\"key\"".len;
    skipJsonSpaces(line, &i, row_end);
    if (i >= row_end or line[i] != ':') return null;
    i += 1;
    skipJsonSpaces(line, &i, row_end);
    const key = parseJsonStringToken(line, &i, row_end) orelse return null;
    skipJsonSpaces(line, &i, row_end);
    if (i >= row_end or line[i] != ',') return null;
    i += 1;
    skipJsonSpaces(line, &i, row_end);
    if (!std.mem.startsWith(u8, line[i..row_end], "\"value\"")) return null;
    i += "\"value\"".len;
    skipJsonSpaces(line, &i, row_end);
    if (i >= row_end or line[i] != ':') return null;
    i += 1;
    skipJsonSpaces(line, &i, row_end);
    if (i >= row_end) return null;

    var value_end = row_end;
    while (value_end > i and (line[value_end - 1] == ' ' or line[value_end - 1] == '\t')) value_end -= 1;
    if (value_end <= i) return null;
    return .{ .key = key, .value = line[i..value_end] };
}

fn skipJsonSpaces(data: []const u8, pos: *usize, limit: usize) void {
    while (pos.* < limit and (data[pos.*] == ' ' or data[pos.*] == '\t')) pos.* += 1;
}

fn parseJsonStringToken(data: []const u8, pos: *usize, limit: usize) ?[]const u8 {
    if (pos.* >= limit or data[pos.*] != '"') return null;
    pos.* += 1;
    const start = pos.*;
    while (pos.* < limit) : (pos.* += 1) {
        if (data[pos.*] == '\\' and pos.* + 1 < limit) {
            pos.* += 1;
            continue;
        }
        if (data[pos.*] == '"') {
            const end = pos.*;
            pos.* += 1;
            return data[start..end];
        }
    }
    return null;
}

fn jsonStr(json: []const u8, key: []const u8) ?[]const u8 {
    var kbuf: [64]u8 = undefined;
    const needle = std.fmt.bufPrint(&kbuf, "\"{s}\":", .{key}) catch return null;
    const pos = std.mem.indexOf(u8, json, needle) orelse return null;
    var start = pos + needle.len;
    while (start < json.len and (json[start] == ' ' or json[start] == '\t')) start += 1;
    if (start >= json.len or json[start] != '"') return null;
    start += 1;
    const end = std.mem.indexOfScalarPos(u8, json, start, '"') orelse return null;
    return json[start..end];
}

fn jsonValue(json: []const u8, key: []const u8) ?[]const u8 {
    var kbuf: [64]u8 = undefined;
    const needle = std.fmt.bufPrint(&kbuf, "\"{s}\":", .{key}) catch return null;
    const pos = std.mem.indexOf(u8, json, needle) orelse return null;
    var start = pos + needle.len;
    while (start < json.len and (json[start] == ' ' or json[start] == '\t')) start += 1;
    if (start >= json.len) return null;

    const ch = json[start];
    if (ch == '"') {
        var i = start + 1;
        while (i < json.len) : (i += 1) {
            if (json[i] == '\\' and i + 1 < json.len) {
                i += 1;
                continue;
            }
            if (json[i] == '"') return json[start .. i + 1];
        }
        return null;
    } else if (ch == '{' or ch == '[') {
        const close: u8 = if (ch == '{') '}' else ']';
        var depth: u32 = 1;
        var i = start + 1;
        var in_str = false;
        while (i < json.len and depth > 0) : (i += 1) {
            if (json[i] == '\\' and in_str) {
                i += 1;
                continue;
            }
            if (json[i] == '"') {
                in_str = !in_str;
                continue;
            }
            if (in_str) continue;
            if (json[i] == ch) depth += 1;
            if (json[i] == close) {
                depth -= 1;
                if (depth == 0) return json[start .. i + 1];
            }
        }
        return null;
    } else {
        var end = start;
        while (end < json.len and json[end] != ',' and json[end] != '}' and json[end] != '\n' and json[end] != '\r') : (end += 1) {}
        return std.mem.trim(u8, json[start..end], " \t");
    }
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

    var buf: std.ArrayList(u8) = .empty;
    const w = compat.arrayListWriter(&buf, alloc);
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
            compat.format(w, "{{\"no\":{d},\"kind\":\"{s}\",\"text\":\"", .{ d.line_no, kind_str }) catch return -1;
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

    const json = buf.toOwnedSlice(alloc) catch return -1;
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
    var buf: std.ArrayList(u8) = .empty;
    const w = compat.arrayListWriter(&buf, alloc);

    w.writeAll("{\"matching_files\":[") catch return -1;
    for (result.matching_files, 0..) |d, i| {
        if (i > 0) w.writeByte(',') catch return -1;
        compat.format(w, "{{\"key\":\"{s}\",\"size\":{d}}}", .{ d.key, d.value.len }) catch return -1;
    }
    w.writeAll("],\"related_files\":[") catch return -1;
    for (result.related_files, 0..) |d, i| {
        if (i > 0) w.writeByte(',') catch return -1;
        compat.format(w, "{{\"key\":\"{s}\"}}", .{d.key}) catch return -1;
    }
    w.writeAll("],\"test_files\":[") catch return -1;
    for (result.test_files, 0..) |d, i| {
        if (i > 0) w.writeByte(',') catch return -1;
        compat.format(w, "{{\"key\":\"{s}\"}}", .{d.key}) catch return -1;
    }
    compat.format(w, "],\"recent_versions\":{d},\"total_files\":{d}}}", .{ result.recent_versions, result.total_files }) catch return -1;

    // Copy to output
    const json = buf.toOwnedSlice(alloc) catch return -1;
    out_json.* = json.ptr;
    out_len.* = @intCast(json.len);
    return 0;
}
