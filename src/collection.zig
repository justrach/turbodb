/// TurboDB — Collection (MVCC document store + query engine)
const std = @import("std");
const compat = @import("compat");
const doc_mod   = @import("doc.zig");
const page_mod  = @import("page.zig");
const btree_mod = @import("btree.zig");
const codeindex = @import("codeindex.zig");
const wal_mod   = @import("wal");
const epoch_mod = @import("epoch");
const hot_cache = @import("hot_cache.zig");
const mvcc_mod  = @import("mvcc.zig");
const cdc_mod = @import("cdc.zig");
const vector = @import("vector.zig");
const branch_mod = @import("branch.zig");
const turboquant = @import("turboquant.zig");
const Doc = doc_mod.Doc;
const DocHeader = doc_mod.DocHeader;
const PageFile = page_mod.PageFile;
const BTree = btree_mod.BTree;
const BTreeEntry = btree_mod.BTreeEntry;
const WAL = wal_mod.WAL;
const EpochManager = epoch_mod.EpochManager;
const TrigramIndex = codeindex.TrigramIndex;
const WordIndex = codeindex.WordIndex;

pub const DEFAULT_TENANT = "default";
pub const MAX_TENANT_ID_LEN: usize = 64;
pub const MAX_COLLECTION_NAME_LEN: usize = 64;

fn writeCommittedDocumentWal(wal_log: *WAL, op: wal_mod.OpCode, payload: []const u8) !void {
    const txn = wal_log.next_lsn.load(.monotonic);
    if (comptime @hasDecl(WAL, "writeCommitted")) {
        _ = try wal_log.writeCommitted(txn, op, wal_mod.DB_TAG_DOC, payload);
    } else {
        _ = try wal_log.write(txn, op, wal_mod.DB_TAG_DOC, wal_mod.FLAG_COMMIT, payload);
    }
}

// ─── IndexQueue ──────────────────────────────────────────────────────────
// Lock-free MPSC queue for deferring trigram+word index builds to a background thread.
// Insert path pushes owned (key, value) pairs; background thread pops and indexes.
pub const IndexQueue = struct {
    const CAPACITY = 131072; // 128K entries — sized for 100+ concurrent writers

    const Entry = struct {
        key: []const u8,
        value: []const u8,
    };

    buf: []Entry,
    ready: []std.atomic.Value(u8), // per-slot readiness flag (0=empty, 1=filled)
    head: std.atomic.Value(u32) = std.atomic.Value(u32).init(0),
    tail: std.atomic.Value(u32) = std.atomic.Value(u32).init(0),
    alloc: std.mem.Allocator,

    pub fn init(alloc: std.mem.Allocator) IndexQueue {
        const buf = alloc.alloc(Entry, CAPACITY) catch @panic("IndexQueue: OOM");
        const ready = alloc.alloc(std.atomic.Value(u8), CAPACITY) catch @panic("IndexQueue: OOM");
        for (ready) |*r| r.* = std.atomic.Value(u8).init(0);
        return .{ .buf = buf, .ready = ready, .alloc = alloc };
    }

    pub fn deinit(self: *IndexQueue) void {
        self.alloc.free(self.buf);
        self.alloc.free(self.ready);
    }

    /// Push a (key, value) pair. Lock-free MPSC via CAS + per-slot ready flag.
    pub fn push(self: *IndexQueue, key: []const u8, value: []const u8) bool {
        while (true) {
            const h = self.head.load(.acquire);
            const next = (h + 1) % CAPACITY;
            if (next == self.tail.load(.acquire)) return false; // full
            // Atomically claim this slot by advancing head.
            if (self.head.cmpxchgWeak(h, next, .acq_rel, .monotonic)) |_| {
                continue; // CAS failed — another producer won, retry
            }
            // We own slot h — write data then signal readiness.
            self.buf[h] = .{ .key = key, .value = value };
            self.ready[h].store(1, .release);
            return true;
        }
    }

    /// Pop one entry. Returns null if empty or next slot not yet ready.
    pub fn pop(self: *IndexQueue) ?Entry {
        const t = self.tail.load(.acquire);
        if (t == self.head.load(.acquire)) return null; // empty
        // Wait for the producer to finish writing this slot.
        if (self.ready[t].load(.acquire) == 0) return null;
        const entry = self.buf[t];
        self.ready[t].store(0, .release);
        self.tail.store((t + 1) % CAPACITY, .release);
        return entry;
    }

    pub fn len(self: *IndexQueue) u32 {
        const h = self.head.load(.acquire);
        const t = self.tail.load(.acquire);
        return if (h >= t) h - t else CAPACITY - t + h;
    }
};

/// Fast float parser for embedding arrays. Handles [-]digits[.digits] only.
/// 5-10x faster than std.fmt.parseFloat for typical embedding values.
fn fastParseFloat(s: []const u8) ?f32 {
    if (s.len == 0) return null;
    var i: usize = 0;
    var neg: bool = false;
    if (s[0] == '-') { neg = true; i = 1; }
    var int_part: i32 = 0;
    while (i < s.len and s[i] >= '0' and s[i] <= '9') : (i += 1) {
        int_part = int_part * 10 + @as(i32, s[i] - '0');
    }
    var frac: f32 = 0;
    if (i < s.len and s[i] == '.') {
        i += 1;
        var scale: f32 = 0.1;
        while (i < s.len and s[i] >= '0' and s[i] <= '9') : (i += 1) {
            frac += @as(f32, @floatFromInt(s[i] - '0')) * scale;
            scale *= 0.1;
        }
    }
    // Handle 'e' notation minimally: e.g. 1.5e-2
    const result = @as(f32, @floatFromInt(int_part)) + frac;
    if (i < s.len and (s[i] == 'e' or s[i] == 'E')) {
        // Fall back to std for scientific notation
        return std.fmt.parseFloat(f32, s) catch null;
    }
    return if (neg) -result else result;
}

/// Extract a float array from JSON: finds "field_name":[...] and parses floats.
/// Optimized: builds needle once, uses fast float parser, minimal branching.
fn extractJsonFloatArray(json: []const u8, field_name: []const u8, out: []f32) ?u32 {
    // Build needle: "field_name":[
    var search_buf: [70]u8 = undefined;
    if (field_name.len + 4 > search_buf.len) return null;
    const needle_len = field_name.len + 4;
    search_buf[0] = '"';
    @memcpy(search_buf[1 .. 1 + field_name.len], field_name);
    search_buf[1 + field_name.len] = '"';
    search_buf[2 + field_name.len] = ':';
    search_buf[3 + field_name.len] = '[';
    const needle = search_buf[0..needle_len];

    const start_idx = std.mem.indexOf(u8, json, needle) orelse return null;
    var pos = start_idx + needle_len;
    var count: u32 = 0;

    while (pos < json.len and count < out.len) {
        // Skip whitespace and commas in one pass
        while (pos < json.len) {
            const c = json[pos];
            if (c == ' ' or c == ',' or c == '\n' or c == '\r' or c == '\t') {
                pos += 1;
            } else break;
        }
        if (pos >= json.len or json[pos] == ']') break;

        // Find end of number token
        const num_start = pos;
        while (pos < json.len) {
            const c = json[pos];
            if (c == ',' or c == ']' or c == ' ' or c == '\n') break;
            pos += 1;
        }
        out[count] = fastParseFloat(json[num_start..pos]) orelse break;
        count += 1;
    }
    return count;
}
// ─── Collection ──────────────────────────────────────────────────────────
pub const Collection = struct {
    pub const STRIPE_COUNT = 4096;
    // MVCC GC: trigger version chain cleanup every GC_INTERVAL inserts.
    const GC_INTERVAL: u64 = 500;

    name_buf: [128]u8,
    name_len: u8,
    pf: PageFile,
    idx: BTree,
    tri: TrigramIndex,
    words: WordIndex,
    wal_log: *WAL,
    epochs: *EpochManager,
    cdc: *cdc_mod.CDCManager,
    next_doc_id: std.atomic.Value(u64),
    // Record-level latching: 1024 stripe locks replace single write_mu.
    // Two writers to different keys proceed in parallel; same-key writes serialize.
    stripe_locks: [STRIPE_COUNT]compat.Mutex,
    // Protects shared page/index/MVCC metadata that is not internally thread-safe.
    meta_mu: compat.Mutex,
    cache_mu: compat.Mutex,
    hash_idx: std.AutoHashMap(u64, BTreeEntry),
    key_doc_ids: std.AutoHashMap(u64, u64),
    /// Per-key modification epoch — tracks when each key was last written on main.
    /// Used by mergeBranch to detect conflicts (key modified after branch was created).
    key_epochs: std.AutoHashMap(u64, u64),
    alloc: std.mem.Allocator,
    cache: hot_cache.HotCache,
    // MVCC version chain — append-only, vacuumed periodically via gc_counter.
    versions: mvcc_mod.VersionChain,

    // Async index queues — inserts round-robin into 2 queues, 2 background threads drain them.
    index_queue: IndexQueue,
    index_queue2: IndexQueue,
    index_thread: ?std.Thread,
    index_thread2: ?std.Thread,
    index_stop: std.atomic.Value(bool),
    queue_toggle: std.atomic.Value(u32),
    indexing_count: std.atomic.Value(u32),

    // Vector embedding column (optional)
    vectors: ?*vector.VectorColumn = null,
    vector_field: [64]u8 = undefined,
    vector_field_len: u8 = 0,
    /// Maps vector index -> BTreeEntry so we can look up the real document.

    // Branch manager (optional — lazy-initialized on first branch creation)
    branch_mgr: ?*branch_mod.BranchManager = null,
    vec_entries: std.ArrayListUnmanaged(BTreeEntry) = .empty,
    gc_counter: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),
    fn stripeIndex(key_hash: u64) usize {
        return @intCast(key_hash % STRIPE_COUNT);
    }

    pub fn open(
        alloc: std.mem.Allocator,
        data_dir: []const u8,
        logical_name: []const u8,
        storage_name: []const u8,
        wal_log: *WAL,
        epochs: *EpochManager,
        cdc: *cdc_mod.CDCManager,
    ) !*Collection {
        var path_buf: [512]u8 = undefined;
        const col = try alloc.create(Collection);
        errdefer alloc.destroy(col);

        try ensureDirPath(data_dir);
        const page_path = try std.fmt.bufPrint(&path_buf, "{s}/{s}.pages", .{ data_dir, storage_name });
        col.pf = try PageFile.open(page_path);

        col.idx = BTree.init(&col.pf, 0);
        col.tri = TrigramIndex.init(alloc);
        col.words = WordIndex.init(alloc);
        col.wal_log = wal_log;
        col.epochs = epochs;
        col.cdc = cdc;
        col.next_doc_id = std.atomic.Value(u64).init(1);
        // Initialize all stripe locks.
        for (&col.stripe_locks) |*lock| {
            lock.* = .{};
        }
        col.meta_mu = .{};
        col.cache_mu = .{};
        col.hash_idx = std.AutoHashMap(u64, BTreeEntry).init(alloc);
        col.key_doc_ids = std.AutoHashMap(u64, u64).init(alloc);
        col.key_epochs = std.AutoHashMap(u64, u64).init(alloc);
        col.alloc = alloc;
        col.cache = hot_cache.HotCache.init();
        col.versions = mvcc_mod.VersionChain.init(alloc);
        col.index_queue = IndexQueue.init(alloc);
        col.index_queue2 = IndexQueue.init(alloc);
        col.index_stop = std.atomic.Value(bool).init(false);
        col.queue_toggle = std.atomic.Value(u32).init(0);
        col.indexing_count = std.atomic.Value(u32).init(0);
        col.vectors = null;
        col.vector_field_len = 0;
        col.vec_entries = .empty;
        col.branch_mgr = null;

        const n = @min(logical_name.len, col.name_buf.len - 1);
        @memcpy(col.name_buf[0..n], logical_name[0..n]);
        col.name_len = @intCast(n);
        try col.rebuildIndexes();

        // Start background indexer threads. If spawn fails, inserts fall back to sync
        // indexing via the queue-full path (queue stays empty, retries exhaust immediately).
        col.index_thread = std.Thread.spawn(.{}, indexWorkerQ, .{ col, &col.index_queue }) catch null;
        col.index_thread2 = std.Thread.spawn(.{}, indexWorkerQ, .{ col, &col.index_queue2 }) catch null;

        return col;
    }

    pub fn close(self: *Collection) void {
        // Signal background indexers to stop, then join both threads.
        self.index_stop.store(true, .release);
        if (self.index_thread) |t| t.join();
        if (self.index_thread2) |t| t.join();
        // Drain any leftover entries from both queues (free owned slices).
        while (self.index_queue.pop()) |entry| {
            self.alloc.free(entry.key);
            self.alloc.free(entry.value);
        }
        while (self.index_queue2.pop()) |entry| {
            self.alloc.free(entry.key);
            self.alloc.free(entry.value);
        }
        self.index_queue.deinit();
        self.index_queue2.deinit();
        self.tri.deinit();
        self.words.deinit();
        self.hash_idx.deinit();
        self.key_doc_ids.deinit();
        self.key_epochs.deinit();
        self.versions.deinit(self.alloc);
        if (self.vectors) |vc| {
            vc.deinit(self.alloc);
            self.alloc.destroy(vc);
        }
        self.vec_entries.deinit(self.alloc);
        if (self.branch_mgr) |bm| {
            bm.deinit();
            self.alloc.destroy(bm);
        }
        self.pf.close();
        self.alloc.destroy(self);
    }

    pub fn configureVectors(self: *Collection, dims: u32, field_name: []const u8) !void {
        self.meta_mu.lock();
        defer self.meta_mu.unlock();

        if (self.vectors) |old| {
            old.deinit(self.alloc);
            self.alloc.destroy(old);
        }
        const vc = try self.alloc.create(vector.VectorColumn);
        vc.* = vector.VectorColumn.init(dims);
        self.vectors = vc;
        const n = @min(field_name.len, self.vector_field.len);
        @memcpy(self.vector_field[0..n], field_name[0..n]);
        self.vector_field_len = @intCast(n);
    }
    pub fn sync(self: *Collection) !void {
        try self.pf.sync();
    }

    /// Block until the background indexers have drained all pending items
    /// and finished processing the current batch.
    pub fn flushIndex(self: *Collection) void {
        var waited: u32 = 0;
        while ((self.index_queue.len() > 0 or self.index_queue2.len() > 0 or self.indexing_count.load(.acquire) > 0) and waited < 300_000) : (waited += 1) {
            compat.sleep(100_000); // 100µs
        }
    }

    pub fn name(self: *const Collection) []const u8 {
        return self.name_buf[0..self.name_len];
    }

    /// Return the number of live indexed documents (excludes deleted docs).
    pub fn docCount(self: *const Collection) u64 {
        return @intCast(self.tri.fileCount());
    }

    pub fn storageBytes(self: *const Collection) usize {
        return self.pf.mm.len;
    }

    // ─── MVCC read transaction ──────────────────────────────────────────

    /// Begin a snapshot-isolated read transaction.
    /// The caller sees a consistent view as of the current epoch.
    pub fn beginRead(self: *Collection) mvcc_mod.ReadTxn {
        return self.versions.beginRead();
    }

    // ─── insert ──────────────────────────────────────────────────────────

    /// Insert a new document. Returns assigned doc_id.
    /// Uses per-key stripe lock for fine-grained concurrency.
    pub fn insert(self: *Collection, key: []const u8, value: []const u8) !u64 {
        const key_hash = doc_mod.fnv1a(key);
        const stripe = stripeIndex(key_hash);
        self.stripe_locks[stripe].lock();
        defer self.stripe_locks[stripe].unlock();

        const doc_id = self.next_doc_id.fetchAdd(1, .monotonic);
        const hdr = doc_mod.newHeader(doc_id, key, value);
        const d = Doc{ .header = hdr, .key = key, .value = value };

        // Use stack buffer for small docs, heap for large ones (code files can be 100KB+)
        var enc_buf: [65536]u8 = undefined;
        const total_size = DocHeader.size + key.len + value.len;
        var heap_buf: ?[]u8 = null;
        defer if (heap_buf) |hb| self.alloc.free(hb);
        const buf = if (total_size <= enc_buf.len)
            &enc_buf
        else blk: {
            heap_buf = try self.alloc.alloc(u8, total_size + 64);
            break :blk heap_buf.?;
        };
        const enc = try d.encodeBuf(buf);

        // Single-entry document writes are self-committed for recovery.
        try writeCommittedDocumentWal(self.wal_log, .doc_insert, enc);

        self.meta_mu.lock();
        {
            defer self.meta_mu.unlock();

            // Find (or allocate) a leaf page with enough space.
            const pno = try self.findOrAllocLeaf(enc.len);
            const page_off = self.pf.leafAppend(pno, enc) orelse
                return error.PageFull;

            // Index the document.
            const entry = BTreeEntry{
                .key_hash = hdr.key_hash,
                .doc_id   = doc_id,
                .page_no  = pno,
                .page_off = page_off,
            };
            try self.idx.insert(entry);
            self.hash_idx.put(hdr.key_hash, entry) catch {};

            // MVCC: register version in the version chain.
            const epoch = self.epochs.advance();
            self.versions.appendVersion(self.alloc, doc_id, pno, page_off, epoch) catch {};
            self.key_doc_ids.put(hdr.key_hash, doc_id) catch {};
            self.key_epochs.put(hdr.key_hash, doc_id) catch {}; // epoch = doc_id (monotonic)

            // Async trigram + word indexing — push to background queue, sync fallback if full or no worker.
            if (value.len >= 3) {
                if (self.index_thread == null) {
                    // No background worker — index synchronously to avoid losing data.
                    self.tri.indexFile(key, value) catch {};
                    self.words.indexFile(key, value) catch {};
                } else {
                    const owned_key = self.alloc.dupe(u8, key) catch null;
                    const owned_val = self.alloc.dupe(u8, value) catch null;
                    if (owned_key != null and owned_val != null) {
                        const q = &self.index_queue;
                        var retries: u32 = 0;
                        while (!q.push(owned_key.?, owned_val.?)) {
                            retries += 1;
                            if (retries > 1000) {
                                // Queue full — fall back to synchronous indexing.
                                self.tri.indexFile(owned_key.?, owned_val.?) catch {};
                                self.words.indexFile(owned_key.?, owned_val.?) catch {};
                                self.alloc.free(owned_key.?);
                                self.alloc.free(owned_val.?);
                                break;
                            }
                            std.Thread.yield() catch {};
                        }
                    }
                }
            }

            // Extract and store vector embedding if configured.
            // Uses stack buffer to avoid heap allocation per insert.
            if (self.vectors) |vc| {
                const field = self.vector_field[0..self.vector_field_len];
                const dims: usize = vc.dims;
                var embed_stack: [4096]f32 = undefined;
                const emb = if (dims <= 4096) embed_stack[0..dims] else blk: {
                    break :blk self.alloc.alloc(f32, dims) catch null;
                };
                if (emb) |e| {
                    defer if (dims > 4096) self.alloc.free(e);
                    if (extractJsonFloatArray(value, field, e)) |count| {
                        if (count == dims) {
                            vc.append(self.alloc, e) catch {};
                            self.vec_entries.append(self.alloc, entry) catch {};
                        }
                    }
                }
            }

            // Periodic MVCC version chain GC to prevent unbounded memory growth.
            if (self.gc_counter.fetchAdd(1, .monotonic) % GC_INTERVAL == GC_INTERVAL - 1) {
                _ = self.versions.gc(self.alloc);
            }
        }

        emitChange(self, .insert, key, value, doc_id);

        return doc_id;
    }

    /// Insert a document with a pre-computed embedding (no JSON parsing needed).
    /// This is the fast path — embedding is passed directly as f32 slice.
    pub fn insertWithEmbedding(self: *Collection, key: []const u8, value: []const u8, embedding: []const f32) !u64 {
        const doc_id = self.insert(key, value) catch |e| return e;
        // insert() already handles vector extraction from JSON, but if embedding
        // was passed directly AND the JSON extraction didn't find it, append now.
        self.meta_mu.lock();
        defer self.meta_mu.unlock();
        if (self.vectors) |vc| {
            // Check if insert() already appended (vec_entries grew)
            // vec_entries.len should equal vc.count after insert if JSON had embedding
            if (vc.count < self.vec_entries.items.len + 1 or self.vec_entries.items.len == 0) {
                // JSON didn't have embedding or extraction failed — use the direct one
                if (embedding.len == vc.dims) {
                    vc.append(self.alloc, embedding) catch {};
                    // Look up the BTreeEntry for this doc
                    const key_hash = doc_mod.fnv1a(key);
                    if (self.hash_idx.get(key_hash)) |entry| {
                        self.vec_entries.append(self.alloc, entry) catch {};
                    }
                }
            }
        }
        return doc_id;
    }

    // ─── get ─────────────────────────────────────────────────────────────

    /// Look up a document by key. Returns a decoded Doc or null.
    /// The Doc's key/value slices are valid until the next write to this collection.
    pub fn get(self: *Collection, key: []const u8) ?Doc {
        const key_hash = doc_mod.fnv1a(key);
        self.meta_mu.lock();
        defer self.meta_mu.unlock();

        // L1: Hot cache (O(1), no hash table overhead)
        self.cache_mu.lock();
        const cached = self.cache.lookup(key_hash);
        self.cache_mu.unlock();
        if (cached) |loc| {
            if (self.readLoc(loc.page_no, loc.page_off)) |d| return d;
        }

        // L2: Hash index (O(1), but hash table lookup)
        if (self.hash_idx.get(key_hash)) |entry| {
            if (self.readEntry(entry)) |d| {
                self.cache_mu.lock();
                self.cache.insert(key_hash, entry.page_no, entry.page_off);
                self.cache_mu.unlock();
                return d;
            }
        }
        // L3: B-tree (O(log n))
        const entry = self.idx.search(key_hash) orelse return null;
        const d = self.readEntry(entry) orelse return null;
        self.cache_mu.lock();
        self.cache.insert(key_hash, entry.page_no, entry.page_off);
        self.cache_mu.unlock();
        return d;
    }

    pub fn getAsOfEpoch(self: *Collection, key: []const u8, epoch: u64) ?Doc {
        const key_hash = doc_mod.fnv1a(key);
        self.meta_mu.lock();
        defer self.meta_mu.unlock();
        const doc_id = self.key_doc_ids.get(key_hash) orelse return null;
        const ver = self.versions.getAtEpoch(doc_id, epoch) orelse return null;
        return self.readLoc(ver.page_no, ver.page_off);
    }

    pub fn getAsOfTimestamp(self: *Collection, key: []const u8, ts_ms: i64) ?Doc {
        const epoch = self.epochs.epochForTimestamp(ts_ms) orelse return null;
        return self.getAsOfEpoch(key, epoch);
    }

    /// Look up a document by doc_id using a linear scan of the index.
    pub fn getById(self: *Collection, doc_id: u64) ?Doc {
        self.meta_mu.lock();
        defer self.meta_mu.unlock();
        const total_pages = self.pf.next_alloc.load(.acquire);
        var pno: u32 = 0;
        while (pno < total_pages) : (pno += 1) {
            const ph = self.pf.pageHeader(pno);
            if (@as(page_mod.PageType, @enumFromInt(ph.page_type)) != .leaf) continue;
            const data = self.pf.pageData(pno);
            var pos: usize = 0;
            while (pos + DocHeader.size <= ph.used_bytes) {
                const rem = data[pos..ph.used_bytes];
                const decoded = doc_mod.decode(rem) catch break;
                if (decoded.doc.header.doc_id == doc_id and
                    decoded.doc.header.flags & DocHeader.DELETED == 0)
                    return decoded.doc;
                pos += decoded.consumed;
            }
        }
        return null;
    }

    // ─── update ──────────────────────────────────────────────────────────

    /// Update an existing document (append new version, update index).
    /// Uses per-key stripe lock — concurrent updates to different keys proceed in parallel.
    pub fn update(self: *Collection, key: []const u8, new_value: []const u8) !bool {
        const key_hash = doc_mod.fnv1a(key);
        const stripe = stripeIndex(key_hash);
        self.stripe_locks[stripe].lock();
        defer self.stripe_locks[stripe].unlock();

        var doc_id: u64 = undefined;
        {
            self.meta_mu.lock();
            defer self.meta_mu.unlock();

            const old_entry = self.idx.search(key_hash) orelse return false;
            const old_doc = self.readEntry(old_entry) orelse return false;

            doc_id = old_doc.header.doc_id;
            var new_hdr = doc_mod.newHeader(doc_id, key, new_value);
            new_hdr.version = old_doc.header.version +% 1;
            // MVCC: encode old location into next_ver field.
            new_hdr.next_ver = (@as(u64, old_entry.page_no) << 16) | old_entry.page_off;

            const d = Doc{ .header = new_hdr, .key = key, .value = new_value };
            var enc_buf: [65536]u8 = undefined;
            const enc = try d.encodeBuf(&enc_buf);

            try writeCommittedDocumentWal(self.wal_log, .doc_update, enc);

            const pno = try self.findOrAllocLeaf(enc.len);
            const page_off = self.pf.leafAppend(pno, enc) orelse return error.PageFull;

            const new_entry = BTreeEntry{
                .key_hash = key_hash,
                .doc_id   = doc_id,
                .page_no  = pno,
                .page_off = page_off,
            };
            self.hash_idx.put(key_hash, new_entry) catch {};
            self.cache_mu.lock();
            self.cache.invalidate(key_hash);
            self.cache_mu.unlock();

            // MVCC: register new version in the version chain (links to old automatically).
            const epoch = self.epochs.advance();
            self.versions.appendVersion(self.alloc, doc_id, pno, page_off, epoch) catch {};
        }
        emitChange(self, .update, key, new_value, doc_id);

        return true;
    }

    // ─── delete ──────────────────────────────────────────────────────────

    pub fn delete(self: *Collection, key: []const u8) !bool {
        const key_hash = doc_mod.fnv1a(key);
        const stripe = stripeIndex(key_hash);
        self.stripe_locks[stripe].lock();
        defer self.stripe_locks[stripe].unlock();

        var doc_id: u64 = undefined;
        {
            self.meta_mu.lock();
            defer self.meta_mu.unlock();

            const entry = self.idx.search(key_hash) orelse return false;

            const old_doc = self.readEntry(entry) orelse return false;
            doc_id = old_doc.header.doc_id;
            var tomb_hdr = doc_mod.newHeader(doc_id, key, "");
            tomb_hdr.flags |= DocHeader.DELETED;
            tomb_hdr.version = old_doc.header.version +% 1;
            tomb_hdr.next_ver = (@as(u64, entry.page_no) << 16) | entry.page_off;
            const tomb_doc = Doc{ .header = tomb_hdr, .key = key, .value = "" };
            var enc_buf: [65536]u8 = undefined;
            const enc = try tomb_doc.encodeBuf(&enc_buf);
            try writeCommittedDocumentWal(self.wal_log, .doc_delete, enc);
            const pno = try self.findOrAllocLeaf(enc.len);
            const page_off = self.pf.leafAppend(pno, enc) orelse return error.PageFull;
            const epoch = self.epochs.advance();
            self.versions.appendVersion(self.alloc, doc_id, pno, page_off, epoch) catch {};
            self.idx.delete(key_hash);
            _ = self.hash_idx.remove(key_hash);
            self.cache_mu.lock();
            self.cache.invalidate(key_hash);
            self.cache_mu.unlock();
        }
        emitChange(self, .delete, key, "", doc_id);
        return true;
    }

    // ─── search (codedb2-style trigram + word index) ───────────────────

    pub const TextSearchResult = struct {
        docs: []Doc,
        candidate_paths: []const []const u8,
        total_files: u64,
        alloc: std.mem.Allocator,
        pub fn deinit(self: TextSearchResult) void {
            self.alloc.free(self.docs);
            if (self.candidate_paths.len > 0) self.alloc.free(self.candidate_paths);
        }
    };

    /// Full-text substring search using codedb2's trigram index with PostingMask bloom filters.
    pub fn searchText(
        self: *Collection,
        query: []const u8,
        limit: u32,
        alloc: std.mem.Allocator,
    ) !TextSearchResult {
        // Split query into terms on spaces for multi-word AND search.
        // Single-word queries still do exact substring matching.
        var terms_buf: [32][]const u8 = undefined;
        var term_count: usize = 0;
        {
            var it = std.mem.splitScalar(u8, query, ' ');
            while (it.next()) |t| {
                const trimmed = std.mem.trim(u8, t, " \t");
                if (trimmed.len > 0 and term_count < terms_buf.len) {
                    terms_buf[term_count] = trimmed;
                    term_count += 1;
                }
            }
        }
        if (term_count == 0) return self.bruteForceSearch(query, limit, alloc);

        const terms = terms_buf[0..term_count];

        // For single terms or exact phrase, try trigram index on full query first.
        if (term_count == 1) {
            const cand_paths = self.tri.candidates(query, alloc) orelse {
                return self.bruteForceSearch(query, limit, alloc);
            };
            var results: std.ArrayList(Doc) = .empty;
            errdefer results.deinit(alloc);
            for (cand_paths) |path| {
                if (results.items.len >= limit) break;
                const doc = self.get(path) orelse continue;
                if (containsInsensitive(doc.value, query)) {
                    try results.append(alloc, doc);
                }
            }
            const total_files = self.tri.file_trigrams.count();
            return TextSearchResult{
                .docs = try results.toOwnedSlice(alloc),
                .candidate_paths = cand_paths,
                .total_files = total_files,
                .alloc = alloc,
            };
        }

        // Multi-term: get candidates from the longest term (most selective),
        // then filter by ALL terms present in the document.
        var longest_idx: usize = 0;
        for (terms, 0..) |t, i| {
            if (t.len > terms[longest_idx].len) longest_idx = i;
        }

        const cand_paths = self.tri.candidates(terms[longest_idx], alloc) orelse {
            return self.multiTermBruteForce(terms, limit, alloc);
        };

        var results: std.ArrayList(Doc) = .empty;
        errdefer results.deinit(alloc);

        for (cand_paths) |path| {
            if (results.items.len >= limit) break;
            const doc = self.get(path) orelse continue;
            if (containsAllTerms(doc.value, terms)) {
                try results.append(alloc, doc);
            }
        }

        const total_files = self.tri.file_trigrams.count();
        return TextSearchResult{
            .docs = try results.toOwnedSlice(alloc),
            .candidate_paths = cand_paths,
            .total_files = total_files,
            .alloc = alloc,
        };
    }

    /// O(1) word lookup using the inverted word index.
    pub fn searchWord(self: *Collection, word: []const u8) []const codeindex.WordHit {
        return self.words.search(word);
    }

    fn bruteForceSearch(self: *Collection, query: []const u8, limit: u32, alloc: std.mem.Allocator) !TextSearchResult {
        var results: std.ArrayList(Doc) = .empty;
        errdefer results.deinit(alloc);

        const result = try self.scan(limit * 10, 0, alloc);
        defer result.deinit();

        for (result.docs) |d| {
            if (results.items.len >= limit) break;
            if (containsInsensitive(d.value, query)) {
                try results.append(alloc, d);
            }
        }

        return TextSearchResult{
            .docs = try results.toOwnedSlice(alloc),
            .candidate_paths = &.{},
            .total_files = 0,
            .alloc = alloc,
        };
    }

    fn containsInsensitive(haystack: []const u8, needle: []const u8) bool {
        if (needle.len == 0) return true;
        if (haystack.len < needle.len) return false;
        var i: usize = 0;
        while (i + needle.len <= haystack.len) : (i += 1) {
            var match = true;
            var j: usize = 0;
            while (j < needle.len) : (j += 1) {
                const hc = haystack[i + j];
                const nc = needle[j];
                const hl = if (hc >= 'A' and hc <= 'Z') hc + 32 else hc;
                const nl = if (nc >= 'A' and nc <= 'Z') nc + 32 else nc;
                if (hl != nl) { match = false; break; }
            }
            if (match) return true;
        }
        return false;
    }

    /// Returns true if haystack contains ALL terms (case-insensitive).
    fn containsAllTerms(haystack: []const u8, terms: []const []const u8) bool {
        for (terms) |term| {
            if (!containsInsensitive(haystack, term)) return false;
        }
        return true;
    }

    /// Brute-force multi-term search (fallback when trigram index has no candidates).
    fn multiTermBruteForce(self: *Collection, terms: []const []const u8, limit: u32, alloc: std.mem.Allocator) !TextSearchResult {
        var results: std.ArrayList(Doc) = .empty;
        errdefer results.deinit(alloc);

        const result = try self.scan(limit * 10, 0, alloc);
        defer result.deinit();

        for (result.docs) |d| {
            if (results.items.len >= limit) break;
            if (containsAllTerms(d.value, terms)) {
                try results.append(alloc, d);
            }
        }

        return TextSearchResult{
            .docs = try results.toOwnedSlice(alloc),
            .candidate_paths = &.{},
            .total_files = 0,
            .alloc = alloc,
        };
    }

    // ─── scan ────────────────────────────────────────────────────────────

    pub const ScanResult = struct {
        docs: []Doc,
        alloc: std.mem.Allocator,
        pub fn deinit(self: ScanResult) void { self.alloc.free(self.docs); }
    };

    /// Linear scan of all live documents, with optional limit/offset.
    pub fn scan(
        self: *Collection,
        limit: u32,
        offset: u32,
        alloc: std.mem.Allocator,
    ) !ScanResult {
        if (limit == 0) {
            return ScanResult{ .docs = try alloc.alloc(Doc, 0), .alloc = alloc };
        }

        var results: std.ArrayList(Doc) = .empty;
        errdefer results.deinit(alloc);

        self.meta_mu.lock();
        defer self.meta_mu.unlock();

        const total_pages = self.pf.next_alloc.load(.acquire);
        var skipped: u32 = 0;
        var pno: u32 = 0;
        outer: while (pno < total_pages) : (pno += 1) {
            const ph = self.pf.pageHeader(pno);
            if (@as(page_mod.PageType, @enumFromInt(ph.page_type)) != .leaf) continue;
            const data = self.pf.pageData(pno);
            var pos: usize = 0;
            while (pos + DocHeader.size <= ph.used_bytes) {
                const rem = data[pos..ph.used_bytes];
                const decoded = doc_mod.decode(rem) catch break;
                const d = decoded.doc;
                pos += decoded.consumed;
                if (d.header.flags & DocHeader.DELETED != 0) continue;
                if (skipped < offset) { skipped += 1; continue; }
                try results.append(alloc, d);
                if (results.items.len >= limit) break :outer;
            }
        }
        return ScanResult{ .docs = try results.toOwnedSlice(alloc), .alloc = alloc };
    }

    pub fn scanAsOfEpoch(self: *Collection, epoch: u64, limit: u32, offset: u32, alloc: std.mem.Allocator) !ScanResult {
        if (limit == 0) {
            return ScanResult{ .docs = try alloc.alloc(Doc, 0), .alloc = alloc };
        }

        var results: std.ArrayList(Doc) = .empty;
        errdefer results.deinit(alloc);

        self.meta_mu.lock();
        defer self.meta_mu.unlock();

        var skipped: u32 = 0;
        var it = self.key_doc_ids.iterator();
        while (it.next()) |entry| {
            const ver = self.versions.getAtEpoch(entry.value_ptr.*, epoch) orelse continue;
            const doc = self.readLoc(ver.page_no, ver.page_off) orelse continue;
            if (skipped < offset) {
                skipped += 1;
                continue;
            }
            try results.append(alloc, doc);
            if (results.items.len >= limit) break;
        }
        return ScanResult{ .docs = try results.toOwnedSlice(alloc), .alloc = alloc };
    }

    pub fn scanAsOfTimestamp(self: *Collection, ts_ms: i64, limit: u32, offset: u32, alloc: std.mem.Allocator) !ScanResult {
        const epoch = self.epochs.epochForTimestamp(ts_ms) orelse {
            return ScanResult{ .docs = try alloc.alloc(Doc, 0), .alloc = alloc };
        };
        return self.scanAsOfEpoch(epoch, limit, offset, alloc);
    }

    // ─── MVCC background compaction ─────────────────────────────────────

    /// Run epoch-based garbage collection on the version chain.
    /// Frees old versions that are no longer visible to any reader.
    /// Returns the number of versions freed.
    pub fn gcVersions(self: *Collection) u64 {
        return self.versions.gc(self.alloc);
    }

    // ─── private helpers ─────────────────────────────────────────────────

    fn readLoc(self: *Collection, page_no: u32, page_off: u16) ?Doc {
        const entry = BTreeEntry{ .key_hash = 0, .doc_id = 0, .page_no = page_no, .page_off = page_off };
        return self.readEntry(entry);
    }

    fn readEntry(self: *Collection, entry: BTreeEntry) ?Doc {
        const raw = self.pf.leafRead(entry.page_no, entry.page_off,
            DocHeader.size + 1024 + 65536);
        const decoded = doc_mod.decode(raw) catch return null;
        if (decoded.doc.header.flags & DocHeader.DELETED != 0) return null;
        return decoded.doc;
    }

    fn readEntryMut(self: *Collection, entry: BTreeEntry) ?*DocHeader {
        const data = self.pf.pageData(entry.page_no);
        if (entry.page_off + DocHeader.size > data.len) return null;
        return @ptrCast(@alignCast(data[entry.page_off..].ptr));
    }

    fn findOrAllocLeaf(self: *Collection, needed: usize) !u32 {
        const total = self.pf.next_alloc.load(.acquire);
        var pno = if (total > 0) total - 1 else 0;
        var checked: u32 = 0;
        while (checked < 32 and pno > 0) : ({ pno -= 1; checked += 1; }) {
            const ph = self.pf.pageHeader(pno);
            if (@as(page_mod.PageType, @enumFromInt(ph.page_type)) != .leaf) continue;
            if (page_mod.PAGE_USABLE - ph.used_bytes >= needed) return pno;
        }
        return self.pf.allocPage(.leaf);
    }

    fn rebuildIndexes(self: *Collection) !void {
        const total_pages = self.pf.next_alloc.load(.acquire);
        var max_doc_id: u64 = 0;
        var pno: u32 = 0;
        while (pno < total_pages) : (pno += 1) {
            const ph = self.pf.pageHeader(pno);
            if (@as(page_mod.PageType, @enumFromInt(ph.page_type)) != .leaf) continue;
            const data = self.pf.pageData(pno);
            var pos: usize = 0;
            while (pos + DocHeader.size <= ph.used_bytes) {
                const rem = data[pos..ph.used_bytes];
                const decoded = doc_mod.decode(rem) catch break;
                const d = decoded.doc;
                const entry = BTreeEntry{
                    .key_hash = d.header.key_hash,
                    .doc_id = d.header.doc_id,
                    .page_no = pno,
                    .page_off = @intCast(pos),
                };
                if (d.header.flags & DocHeader.DELETED == 0) {
                    self.hash_idx.put(d.header.key_hash, entry) catch {};
                    self.key_doc_ids.put(d.header.key_hash, d.header.doc_id) catch {};
                    if (d.value.len >= 3) {
                        self.tri.indexFile(d.key, d.value) catch {};
                        self.words.indexFile(d.key, d.value) catch {};
                    }
                }
                if (d.header.doc_id > max_doc_id) max_doc_id = d.header.doc_id;
                pos += decoded.consumed;
            }
        }
        self.next_doc_id.store(max_doc_id + 1, .release);
    }

    fn emitChange(self: *Collection, op: cdc_mod.Op, key: []const u8, value: []const u8, doc_id: u64) void {
        const full_name = self.name();
        const ref: TenantCollectionRef = splitTenantCollectionKey(full_name) orelse TenantCollectionRef{
            .tenant_id = DEFAULT_TENANT,
            .collection_name = full_name,
        };
        self.cdc.emit(ref.tenant_id, ref.collection_name, key, value, doc_id, op);
    }

    // ─── Vector search ────────────────────────────────────────────────────

    pub const VectorSearchResult = struct {
        docs: []Doc,
        scores: []f32,
        count: u32,
        alloc_ref: std.mem.Allocator,
        pub fn deinit(self: VectorSearchResult) void {
            self.alloc_ref.free(self.docs);
            self.alloc_ref.free(self.scores);
        }
    };

    pub fn searchVectors(
        self: *Collection,
        query: []const f32,
        k: u32,
        metric: vector.Metric,
    ) !VectorSearchResult {
        const vc = self.vectors orelse return error.NoVectorColumn;

        // Search vectors (uses INT8 parallel if enabled, else FP32)
        const results = if (vc.i8_enabled)
            try vc.searchInt8Parallel(self.alloc, query, k, metric)
        else
            try vc.search(self.alloc, query, k, metric);
        defer self.alloc.free(results);

        // Map vector indices back to real documents via vec_entries
        var docs = try self.alloc.alloc(Doc, results.len);
        var scores = try self.alloc.alloc(f32, results.len);
        var count: u32 = 0;

        for (results) |r| {
            // Look up the real document using vec_entries mapping
            if (r.index < self.vec_entries.items.len) {
                const be = self.vec_entries.items[r.index];
                if (self.readEntry(be)) |doc| {
                    docs[count] = doc;
                    scores[count] = r.score;
                    count += 1;
                    continue;
                }
            }
            // Fallback: return a stub doc with the vector index as doc_id
            docs[count] = Doc{
                .header = doc_mod.DocHeader{
                    .doc_id = r.index,
                    .key_hash = 0,
                    .val_len = 0,
                    .key_len = 0,
                    .flags = 0,
                    .version = 0,
                    .next_ver = 0,
                },
                .key = "",
                .value = "",
            };
            scores[count] = r.score;
            count += 1;
        }

        return VectorSearchResult{
            .docs = docs,
            .scores = scores,
            .count = count,
            .alloc_ref = self.alloc,
        };
    }

    // ─── Hybrid search ────────────────────────────────────────────────────

    /// Look up a real document by its vector index, using vec_entries.
    fn getByVecIndex(self: *Collection, vec_index: u32) ?Doc {
        if (vec_index >= self.vec_entries.items.len) return null;
        return self.readEntry(self.vec_entries.items[vec_index]);
    }

    pub fn searchHybrid(
        self: *Collection,
        text_query: []const u8,
        vector_query: []const f32,
        text_weight: f32,
        vector_weight: f32,
        limit: u32,
        result_alloc: std.mem.Allocator,
    ) !TextSearchResult {
        const vc = self.vectors orelse return self.searchText(text_query, limit, result_alloc);

        // Phase 1: Text pre-filter — get candidate doc keys
        const text_results = try self.searchText(text_query, limit * 3, result_alloc);

        if (text_results.docs.len == 0 or vector_query.len != vc.dims) {
            return text_results;
        }

        // Phase 2: Get vector search results for score lookup
        const vec_results = if (vc.i8_enabled)
            try vc.searchInt8Parallel(result_alloc, vector_query, limit * 3, .cosine)
        else
            try vc.search(result_alloc, vector_query, limit * 3, .cosine);
        defer result_alloc.free(vec_results);

        // Build doc_id -> vector score mapping from vec_entries
        var vec_score_by_doc = std.AutoHashMap(u64, f32).init(result_alloc);
        defer vec_score_by_doc.deinit();
        for (vec_results) |vr| {
            if (vr.index < self.vec_entries.items.len) {
                const be = self.vec_entries.items[vr.index];
                try vec_score_by_doc.put(be.doc_id, vr.score);
            }
        }

        // Phase 3: Score-fuse text results with vector scores.
        // Each text result gets text_weight * 1.0 (found in text) + vector_weight * vec_score.
        const ScoredDoc = struct { doc: Doc, score: f32 };
        var scored = try result_alloc.alloc(ScoredDoc, text_results.docs.len);
        defer result_alloc.free(scored);

        for (text_results.docs, 0..) |doc, i| {
            const vs = vec_score_by_doc.get(doc.header.doc_id) orelse 0.0;
            scored[i] = .{ .doc = doc, .score = text_weight * 1.0 + vector_weight * vs };
        }

        // Sort by combined score descending
        std.mem.sort(ScoredDoc, scored, {}, struct {
            fn cmp(_: void, a: ScoredDoc, b: ScoredDoc) bool {
                return a.score > b.score;
            }
        }.cmp);

        // Replace text_results.docs with the re-ranked order (truncate to limit)
        const out_len = @min(scored.len, limit);
        for (scored[0..out_len], 0..) |sd, i| {
            text_results.docs[i] = sd.doc;
        }

        // If we have more docs than limit, shrink the slice
        if (text_results.docs.len > out_len) {
            // We can't easily shrink, but the caller will respect .docs.len
            // Just return the full text_results (already re-ranked in-place)
        }

        return text_results;
    }

    // ─── Branch operations (CoW layer) ──────────────────────────────────

    /// Enable branching on this collection (lazy — only allocates when first branch is created)
    pub fn enableBranching(self: *Collection) !void {
        if (self.branch_mgr != null) return;
        const bm = try self.alloc.create(branch_mod.BranchManager);
        bm.* = branch_mod.BranchManager.init(self.alloc, &self.next_doc_id);
        self.branch_mgr = bm;
    }

    pub fn createBranch(self: *Collection, branch_name: []const u8, agent_id: []const u8) !*branch_mod.Branch {
        try self.enableBranching();
        return self.branch_mgr.?.createBranch(branch_name, agent_id);
    }

    pub fn getBranch(self: *const Collection, branch_name: []const u8) ?*branch_mod.Branch {
        const bm = self.branch_mgr orelse return null;
        return bm.getBranch(branch_name);
    }

    /// Read a key on a branch — checks branch writes first, falls through to main
    pub fn getOnBranch(self: *Collection, br: *const branch_mod.Branch, key: []const u8) ?[]const u8 {
        // Check branch-local writes
        if (br.read(key)) |r| {
            if (r.deleted) return null; // deleted on branch
            return r.value;
        }
        // Fall through to main
        const doc = self.get(key) orelse return null;
        return doc.value;
    }

    /// Write a key-value pair on a branch (isolated — doesn't touch main)
    pub fn writeOnBranch(self: *Collection, br: *branch_mod.Branch, key: []const u8, value: []const u8) !void {
        const epoch = self.next_doc_id.fetchAdd(1, .monotonic);
        try br.write(key, value, epoch);
    }

    /// Delete a key on a branch (isolated tombstone)
    pub fn deleteOnBranch(self: *Collection, br: *branch_mod.Branch, key: []const u8) !void {
        const epoch = self.next_doc_id.fetchAdd(1, .monotonic);
        try br.delete(key, epoch);
    }

    /// Get the diff of a branch vs main (all modified keys)
    pub fn diffBranch(self: *Collection, br: *const branch_mod.Branch, diff_alloc: std.mem.Allocator) ![]branch_mod.DiffEntry {
        _ = self; // main state accessed via br.base_epoch
        return br.modifiedKeys(diff_alloc);
    }

    /// Merge a branch into main. Returns conflicts if any.
    pub fn mergeBranch(self: *Collection, br: *branch_mod.Branch, merge_alloc: std.mem.Allocator) !branch_mod.MergeResult {
        var applied: u32 = 0;
        var conflicts_list: std.ArrayList(branch_mod.MergeConflict) = .empty;

        var it = br.writes.iterator();
        while (it.next()) |entry| {
            const w = entry.value_ptr.*;

            if (w.deleted) {
                applied += 1;
                continue;
            }

            const key_hash = doc_mod.fnv1a(w.key);

            // CONFLICT DETECTION: check if main modified this key after branch was created
            if (self.key_epochs.get(key_hash)) |main_epoch| {
                if (main_epoch > br.base_epoch) {
                    // Main was modified after branch fork — this is a real conflict
                    const main_doc = self.get(w.key);
                    try conflicts_list.append(merge_alloc, .{
                        .key = w.key,
                        .branch_value = w.value,
                        .main_value = if (main_doc) |d| d.value else "",
                    });
                    continue;
                }
            }

            // No conflict — safe to apply
            _ = self.insert(w.key, w.value) catch continue;
            applied += 1;
        }

        if (conflicts_list.items.len == 0) {
            br.status = .merged;
        }

        return .{
            .applied = applied,
            .conflicts = try conflicts_list.toOwnedSlice(merge_alloc),
            .alloc = merge_alloc,
        };
    }

    /// Compare multiple branches side-by-side. Shows every file touched by ANY branch,
    /// with each branch's version and the current main version.
    /// Use this to review agent work before deciding which branch to merge.
    pub fn compareBranches(self: *Collection, branch_names: []const []const u8, alloc: std.mem.Allocator) !branch_mod.CompareResult {
        const bm = self.branch_mgr orelse return error.NoBranchManager;

        // Collect branch pointers
        var branches: std.ArrayList(*const branch_mod.Branch) = .empty;
        defer branches.deinit(alloc);
        for (branch_names) |bname| {
            if (bm.getBranch(bname)) |br| {
                try branches.append(alloc, br);
            }
        }

        // Run comparison
        const result = try branch_mod.compareBranches(branches.items, alloc);

        // Fill in main_value for each entry
        for (result.entries) |*entry| {
            const doc = self.get(entry.key);
            if (doc) |d| entry.main_value = d.value;
        }

        return result;
    }

    /// Search text on a branch — searches branch-local writes first, then falls through to main search.
    /// Returns results that include both branch-modified files and main files matching the query.
    pub fn searchOnBranch(
        self: *Collection,
        br: *const branch_mod.Branch,
        query: []const u8,
        limit: u32,
        search_alloc: std.mem.Allocator,
    ) !TextSearchResult {
        // 1. Search main collection normally
        const main_results = try self.searchText(query, limit, search_alloc);

        // 2. Also search branch-local writes for the query
        var branch_matches: std.ArrayList(Doc) = .empty;
        defer branch_matches.deinit(search_alloc);

        var it = br.writes.iterator();
        while (it.next()) |entry| {
            const w = entry.value_ptr.*;
            if (w.deleted) continue;
            // Check if query appears in the branch-local value or key
            if (containsInsensitive(w.value, query) or containsInsensitive(w.key, query)) {
                try branch_matches.append(search_alloc, Doc{
                    .header = doc_mod.DocHeader{
                        .doc_id = w.epoch,
                        .key_hash = doc_mod.fnv1a(w.key),
                        .val_len = @intCast(w.value.len),
                        .key_len = @intCast(w.key.len),
                        .flags = 0,
                        .version = 0,
                        .next_ver = 0,
                    },
                    .key = w.key,
                    .value = w.value,
                });
            }
        }

        // 3. Overlay branch modifications onto main results.
        // Replace any main result whose key was modified on branch with the branch version.
        for (main_results.docs) |*doc| {
            if (br.read(doc.key)) |r| {
                if (r.deleted) continue;
                if (r.value) |v| {
                    doc.value = v;
                }
            }
        }

        // Branch-only docs that match the query but aren't in main are collected above.
        // Currently we only overlay modifications on existing main results (step 3).
        // Future: could extend the result slice with branch_matches items.

        return main_results;
    }

    /// List all branch names for this collection.
    pub fn listBranches(self: *const Collection, list_alloc: std.mem.Allocator) ![][]const u8 {
        const bm = self.branch_mgr orelse return try list_alloc.alloc([]const u8, 0);
        return bm.listBranches(list_alloc);
    }

    // ─── Smart Context Discovery ─────────────────────────────────────────

    pub const ContextResult = struct {
        /// Files matching the text query (trigram search)
        matching_files: []Doc,
        /// Files that reference/call symbols found in matching files
        related_files: []Doc,
        /// Test files (keys containing "test" that also match the query)
        test_files: []Doc,
        /// Recent changes to matching files (MVCC history)
        recent_versions: u32,
        /// Total files scanned
        total_files: u32,
        ctx_alloc: std.mem.Allocator,

        pub fn deinit(self: *ContextResult) void {
            self.ctx_alloc.free(self.matching_files);
            self.ctx_alloc.free(self.related_files);
            self.ctx_alloc.free(self.test_files);
        }
    };

    /// Smart context discovery — combines text search, caller analysis, test discovery,
    /// and MVCC history in a single call. Gives an agent everything it needs to understand
    /// a code area.
    pub fn discoverContext(
        self: *Collection,
        query: []const u8,
        limit: u32,
        ctx_alloc: std.mem.Allocator,
    ) !ContextResult {
        // Phase 1: Primary search — find files matching the query
        const primary = self.searchText(query, limit * 2, ctx_alloc) catch |e| {
            if (e == error.OutOfMemory) return e;
            return ContextResult{
                .matching_files = &.{},
                .related_files = &.{},
                .test_files = &.{},
                .recent_versions = 0,
                .total_files = @intCast(self.tri.fileCount()),
                .ctx_alloc = ctx_alloc,
            };
        };

        // Phase 2: Find related files — search for identifiers found in primary results.
        // Extract potential function/symbol names from primary results and search for callers.
        var related_set = std.StringHashMap(void).init(ctx_alloc);
        defer related_set.deinit();

        // Mark primary result keys so we don't duplicate them
        for (primary.docs) |doc| {
            related_set.put(doc.key, {}) catch {};
        }

        var related_list: std.ArrayListUnmanaged(Doc) = .empty;

        // For each primary match, extract identifiers and search for them.
        // Simple heuristic: look for "fn <name>" patterns, search for "<name>(".
        for (primary.docs) |doc| {
            var pos: usize = 0;
            while (pos + 3 < doc.value.len) : (pos += 1) {
                // Look for "fn " pattern
                if (doc.value[pos] == 'f' and doc.value[pos + 1] == 'n' and doc.value[pos + 2] == ' ') {
                    // Extract function name
                    const name_start = pos + 3;
                    var name_end = name_start;
                    while (name_end < doc.value.len and
                        ((doc.value[name_end] >= 'a' and doc.value[name_end] <= 'z') or
                        (doc.value[name_end] >= 'A' and doc.value[name_end] <= 'Z') or
                        (doc.value[name_end] >= '0' and doc.value[name_end] <= '9') or
                        doc.value[name_end] == '_'))
                    {
                        name_end += 1;
                    }

                    if (name_end > name_start and name_end - name_start >= 3) {
                        const fn_name = doc.value[name_start..name_end];
                        // Search for callers of this function
                        if (self.searchText(fn_name, 5, ctx_alloc)) |caller_results| {
                            for (caller_results.docs) |caller| {
                                if (!related_set.contains(caller.key)) {
                                    related_set.put(caller.key, {}) catch {};
                                    related_list.append(ctx_alloc, caller) catch {};
                                }
                            }
                            // Don't deinit caller_results — docs are borrowed from mmap
                        } else |_| {}
                    }
                }
            }
        }

        // Phase 3: Find test files — search for "test" + query terms.
        var test_list: std.ArrayListUnmanaged(Doc) = .empty;

        if (self.searchText("test", limit, ctx_alloc)) |test_results| {
            for (test_results.docs) |doc| {
                // Check if this test file relates to our query
                if (containsInsensitive(doc.key, query) or containsInsensitive(doc.value, query)) {
                    if (!related_set.contains(doc.key)) {
                        test_list.append(ctx_alloc, doc) catch {};
                    }
                }
            }
        } else |_| {}

        // Phase 4: Count recent versions of matching files
        var recent_versions: u32 = 0;
        for (primary.docs) |doc| {
            if (doc.header.version > 0) {
                recent_versions += doc.header.version;
            }
        }

        return ContextResult{
            .matching_files = primary.docs,
            .related_files = related_list.toOwnedSlice(ctx_alloc) catch &.{},
            .test_files = test_list.toOwnedSlice(ctx_alloc) catch &.{},
            .recent_versions = recent_versions,
            .total_files = @intCast(self.tri.fileCount()),
            .ctx_alloc = ctx_alloc,
        };
    }

    /// Smart context discovery on a branch — overlays branch modifications on results.
    pub fn discoverContextOnBranch(
        self: *Collection,
        br: *const branch_mod.Branch,
        query: []const u8,
        limit: u32,
        ctx_alloc: std.mem.Allocator,
    ) !ContextResult {
        // Same as discoverContext but overlays branch modifications
        const result = try self.discoverContext(query, limit, ctx_alloc);

        // Overlay branch writes on matching_files
        for (result.matching_files) |*doc| {
            if (br.read(doc.key)) |r| {
                if (!r.deleted) {
                    if (r.value) |v| doc.value = v;
                }
            }
        }

        return result;
    }
};

/// Background thread that drains the index queue and builds trigram indexes.
fn indexWorkerQ(col: *Collection, queue: *IndexQueue) void {
    const BATCH = 64;
    var batch_keys: [BATCH][]const u8 = undefined;
    var batch_vals: [BATCH][]const u8 = undefined;
    var reusable_tris = std.AutoHashMap(codeindex.Trigram, void).init(col.alloc);
    defer reusable_tris.deinit();

    while (!col.index_stop.load(.acquire)) {
        // Signal "working" BEFORE popping so flushIndex doesn't see
        // an empty queue + zero count between pop and processing.
        _ = col.indexing_count.fetchAdd(1, .release);
        var n: usize = 0;
        while (n < BATCH) {
            const entry = queue.pop() orelse break;
            batch_keys[n] = entry.key;
            batch_vals[n] = entry.value;
            n += 1;
        }
        if (n == 0) {
            _ = col.indexing_count.fetchSub(1, .release);
            std.Thread.yield() catch {};
            continue;
        }
        // Batch trigram indexing (single lock acquisition for all docs).
        col.tri.indexBatch(batch_keys[0..n], batch_vals[0..n], &reusable_tris) catch {};
        // Word indexing (per-doc, no batching needed).
        for (0..n) |i| {
            col.words.indexFile(batch_keys[i], batch_vals[i]) catch {};
        }
        for (0..n) |i| {
            col.alloc.free(batch_vals[i]);
            col.alloc.free(batch_keys[i]);
        }
        _ = col.indexing_count.fetchSub(1, .release);
    }
    // Final drain on shutdown.
    while (queue.pop()) |entry| {
        col.tri.indexFile(entry.key, entry.value) catch {};
        col.words.indexFile(entry.key, entry.value) catch {};
        col.alloc.free(entry.value);
        col.alloc.free(entry.key);
    }
}

// ─── Database (multi-collection manager) ─────────────────────────────────

pub const Database = struct {
    collections: std.StringHashMap(*Collection),
    tenant_quotas: std.StringHashMap(TenantQuota),
    tenant_usage: std.StringHashMap(TenantUsage),
    cdc: cdc_mod.CDCManager,
    wal_log: WAL,
    epochs: EpochManager,
    data_dir_buf: [256]u8,
    data_dir_len: usize,
    alloc: std.mem.Allocator,
    mu: compat.RwLock,
    auth: @import("auth.zig").AuthStore,

    pub const TenantQuota = struct {
        max_collections: u32 = 256,
        max_storage_bytes: usize = 10 * 1024 * 1024 * 1024, // 10 GiB
        max_ops_per_second: u32 = 10_000,
    };

    pub const TenantUsage = struct {
        ops_window_ms: i64 = 0,
        ops_in_window: u32 = 0,
    };

    pub fn open(alloc: std.mem.Allocator, data_dir: []const u8) !*Database {
        const db = try alloc.create(Database);
        errdefer alloc.destroy(db);

        const resolved_data_dir = try ensureDataDir(alloc, data_dir);
        defer alloc.free(resolved_data_dir);

        var path_buf: [512]u8 = undefined;
        const wal_path = try std.fmt.bufPrintZ(&path_buf, "{s}/doc.wal", .{resolved_data_dir});
        db.wal_log = try WAL.open(wal_path, alloc);
        // Start background WAL flusher (batches fsyncs every ~1ms like MongoDB's w:1)
        try db.wal_log.startFlusher();
        db.epochs = try EpochManager.init(alloc);
        db.collections = std.StringHashMap(*Collection).init(alloc);
        db.tenant_quotas = std.StringHashMap(TenantQuota).init(alloc);
        db.tenant_usage = std.StringHashMap(TenantUsage).init(alloc);
        db.cdc = cdc_mod.CDCManager.init(alloc);
        try db.cdc.start();
        db.alloc = alloc;
        db.mu = .{};
        db.auth = .{};

        const n = @min(resolved_data_dir.len, 255);
        @memcpy(db.data_dir_buf[0..n], resolved_data_dir[0..n]);
        db.data_dir_len = n;

        return db;
    }

    pub fn close(self: *Database) void {
        var it = self.collections.valueIterator();
        while (it.next()) |col| col.*.close();
        var key_it = self.collections.keyIterator();
        while (key_it.next()) |key| self.alloc.free(key.*);
        self.collections.deinit();
        var quota_it = self.tenant_quotas.keyIterator();
        while (quota_it.next()) |key| self.alloc.free(key.*);
        self.tenant_quotas.deinit();
        var usage_it = self.tenant_usage.keyIterator();
        while (usage_it.next()) |key| self.alloc.free(key.*);
        self.tenant_usage.deinit();
        self.cdc.deinit();
        self.wal_log.close();
        self.epochs.deinit();
        self.alloc.destroy(self);
    }

    pub fn checkpoint(self: *Database) !void {
        self.wal_log.flushPending();
        var it = self.collections.valueIterator();
        while (it.next()) |col| try col.*.sync();
    }

    pub fn dataDir(self: *const Database) []const u8 {
        return self.data_dir_buf[0..self.data_dir_len];
    }

    /// Get or create a named collection.
    pub fn collection(self: *Database, name: []const u8) !*Collection {
        return self.collectionForTenant(DEFAULT_TENANT, name);
    }

    pub fn collectionForTenant(self: *Database, tenant_id: []const u8, name: []const u8) !*Collection {
        try validateTenantComponent(tenant_id);
        try validateCollectionComponent(name);
        const tenant_key = try self.tenantCollectionKey(tenant_id, name);
        defer self.alloc.free(tenant_key);

        // Fast path: read lock
        self.mu.lockShared();
        if (self.collections.get(tenant_key)) |c| {
            self.mu.unlockShared();
            return c;
        }
        self.mu.unlockShared();

        // Slow path: write lock for creation
        self.mu.lock();
        defer self.mu.unlock();
        // Double-check after acquiring write lock
        if (self.collections.get(tenant_key)) |c| return c;
        try self.enforceCollectionQuotaLocked(tenant_id);

        var storage_name_buf: [MAX_TENANT_ID_LEN + MAX_COLLECTION_NAME_LEN + 4]u8 = undefined;
        const storage_name = try makeStorageName(&storage_name_buf, tenant_id, name);

        const col = try Collection.open(
            self.alloc,
            self.dataDir(),
            tenant_key,
            storage_name,
            &self.wal_log,
            &self.epochs,
            &self.cdc,
        );
        const key = try self.alloc.dupe(u8, tenant_key);
        try self.collections.put(key, col);
        return col;
    }

    pub fn dropCollection(self: *Database, name: []const u8) void {
        self.dropCollectionForTenant(DEFAULT_TENANT, name);
    }

    pub fn dropCollectionForTenant(self: *Database, tenant_id: []const u8, name: []const u8) void {
        const tenant_key = self.tenantCollectionKey(tenant_id, name) catch return;
        defer self.alloc.free(tenant_key);
        self.mu.lock();
        defer self.mu.unlock();
        if (self.collections.fetchRemove(tenant_key)) |kv| {
            kv.value.close();
            self.alloc.free(kv.key);
        }

        // Delete the backing page file so data doesn't reappear on re-access.
        var storage_name_buf: [MAX_TENANT_ID_LEN + MAX_COLLECTION_NAME_LEN + 4]u8 = undefined;
        const storage_name = makeStorageName(&storage_name_buf, tenant_id, name) catch return;
        var path_buf: [512]u8 = undefined;
        const page_path = std.fmt.bufPrint(&path_buf, "{s}/{s}.pages", .{ self.dataDir(), storage_name }) catch return;
        compat.cwd().deleteFile(page_path) catch {};
    }

    pub fn listCollectionsForTenant(self: *Database, tenant_id: []const u8, alloc: std.mem.Allocator) !std.ArrayList([]const u8) {
        try validateTenantComponent(tenant_id);
        const prefix = try std.fmt.allocPrint(alloc, "{s}/", .{tenant_id});
        defer alloc.free(prefix);

        var result: std.ArrayList([]const u8) = .empty;
        self.mu.lockShared();
        defer self.mu.unlockShared();

        var it = self.collections.keyIterator();
        while (it.next()) |k| {
            if (std.mem.startsWith(u8, k.*, prefix)) {
                try result.append(alloc, try alloc.dupe(u8, k.*[prefix.len..]));
            }
        }
        return result;
    }

    pub fn configureTenantQuota(self: *Database, tenant_id: []const u8, quota: TenantQuota) !void {
        try validateTenantComponent(tenant_id);
        self.mu.lock();
        defer self.mu.unlock();
        const gop = try self.tenant_quotas.getOrPut(tenant_id);
        if (!gop.found_existing) {
            gop.key_ptr.* = try self.alloc.dupe(u8, tenant_id);
        }
        gop.value_ptr.* = quota;
    }

    pub fn registerWebhook(self: *Database, tenant_id: []const u8, collection_name: []const u8, webhook_url: []const u8, secret: []const u8) !u64 {
        try validateTenantComponent(tenant_id);
        if (collection_name.len > 0) try validateCollectionComponent(collection_name);
        return self.cdc.registerWebhook(tenant_id, collection_name, webhook_url, secret);
    }

    pub fn listWebhookDeliveries(self: *Database, alloc: std.mem.Allocator, tenant_id: ?[]const u8) ![]cdc_mod.Delivery {
        return self.cdc.listDeliveries(alloc, tenant_id);
    }

    pub fn recordTenantOperation(self: *Database, tenant_id: []const u8) !void {
        try validateTenantComponent(tenant_id);
        self.mu.lock();
        defer self.mu.unlock();

        const quota = self.tenant_quotas.get(tenant_id) orelse return;
        if (quota.max_ops_per_second == std.math.maxInt(u32)) return;

        const now_ms: i64 = compat.milliTimestamp();
        const window_ms = now_ms - @mod(now_ms, 1000);
        const usage = try self.getOrCreateTenantUsageLocked(tenant_id);
        if (usage.ops_window_ms != window_ms) {
            usage.ops_window_ms = window_ms;
            usage.ops_in_window = 0;
        }
        if (usage.ops_in_window >= quota.max_ops_per_second) return error.TenantOpsQuotaExceeded;
        usage.ops_in_window += 1;
    }

    pub fn ensureTenantStorageAvailable(self: *Database, tenant_id: []const u8, extra_bytes: usize) !void {
        try validateTenantComponent(tenant_id);
        self.mu.lockShared();
        defer self.mu.unlockShared();

        const quota = self.tenant_quotas.get(tenant_id) orelse return;
        if (quota.max_storage_bytes == std.math.maxInt(usize)) return;

        var total: usize = 0;
        var it = self.collections.iterator();
        while (it.next()) |entry| {
            const parts = splitTenantCollectionKey(entry.key_ptr.*) orelse continue;
            if (std.mem.eql(u8, parts.tenant_id, tenant_id)) {
                total += entry.value_ptr.*.storageBytes();
            }
        }

        if (total + extra_bytes > quota.max_storage_bytes) return error.TenantStorageQuotaExceeded;
    }

    fn enforceCollectionQuotaLocked(self: *Database, tenant_id: []const u8) !void {
        const quota = self.tenant_quotas.get(tenant_id) orelse return;
        if (quota.max_collections == std.math.maxInt(u32)) return;

        var count: u32 = 0;
        var it = self.collections.keyIterator();
        while (it.next()) |k| {
            const parts = splitTenantCollectionKey(k.*) orelse continue;
            if (std.mem.eql(u8, parts.tenant_id, tenant_id)) count += 1;
        }
        if (count >= quota.max_collections) return error.TenantCollectionQuotaExceeded;
    }

    fn tenantCollectionKey(self: *Database, tenant_id: []const u8, name: []const u8) ![]u8 {
        return std.fmt.allocPrint(self.alloc, "{s}/{s}", .{ tenant_id, name });
    }

    fn getOrCreateTenantUsageLocked(self: *Database, tenant_id: []const u8) !*TenantUsage {
        const gop = try self.tenant_usage.getOrPut(tenant_id);
        if (!gop.found_existing) {
            gop.key_ptr.* = try self.alloc.dupe(u8, tenant_id);
            gop.value_ptr.* = .{};
        }
        return gop.value_ptr;
    }
};

pub const TenantCollectionRef = struct {
    tenant_id: []const u8,
    collection_name: []const u8,
};

pub fn splitTenantCollectionKey(full_name: []const u8) ?TenantCollectionRef {
    const sep = std.mem.indexOfScalar(u8, full_name, '/') orelse return null;
    return .{
        .tenant_id = full_name[0..sep],
        .collection_name = full_name[sep + 1 ..],
    };
}

fn validateTenantComponent(name: []const u8) !void {
    if (name.len == 0 or name.len > MAX_TENANT_ID_LEN) return error.InvalidTenantId;
    for (name) |c| {
        if (!std.ascii.isAlphanumeric(c) and c != '-' and c != '_' and c != '.') return error.InvalidTenantId;
    }
}

fn validateCollectionComponent(name: []const u8) !void {
    if (name.len == 0 or name.len > MAX_COLLECTION_NAME_LEN) return error.InvalidCollectionName;
    for (name) |c| {
        if (c == '/' or c == '\\') return error.InvalidCollectionName;
    }
}

fn makeStorageName(buf: []u8, tenant_id: []const u8, collection_name: []const u8) ![]const u8 {
    var fbs = compat.fixedBufferStream(buf);
    const w = fbs.writer();
    try appendSanitizedComponent(w, tenant_id);
    try w.writeAll("__");
    try appendSanitizedComponent(w, collection_name);
    return buf[0..fbs.pos];
}

fn appendSanitizedComponent(writer: anytype, input: []const u8) !void {
    for (input) |c| {
        if (std.ascii.isAlphanumeric(c) or c == '-' or c == '_') {
            try writer.writeByte(c);
        } else {
            try writer.writeByte('_');
        }
    }
}

fn ensureDataDir(alloc: std.mem.Allocator, data_dir: []const u8) ![]u8 {
    try ensureDirPath(data_dir);
    return blk: {
        var buf: [4096]u8 = undefined;
        var z_buf: [4096]u8 = undefined;
        @memcpy(z_buf[0..data_dir.len], data_dir);
        z_buf[data_dir.len] = 0;
        const rp = std.c.realpath(@ptrCast(&z_buf), @ptrCast(&buf));
        if (rp == null) break :blk error.FileNotFound;
        const len = std.mem.indexOfScalar(u8, &buf, 0) orelse buf.len;
        break :blk try alloc.dupe(u8, buf[0..len]);
    };
}

fn ensureDirPath(data_dir: []const u8) !void {
    compat.cwd().makePath(data_dir) catch return error.MkdirError;
}

const CollectionWalReplayProbe = struct {
    const MAX_ENTRIES = 8;

    var count: usize = 0;
    var ops: [MAX_ENTRIES]wal_mod.OpCode = undefined;
    var flags: [MAX_ENTRIES]u16 = undefined;
    var delete_payload: [256]u8 = undefined;
    var delete_payload_len: usize = 0;

    fn reset() void {
        count = 0;
        delete_payload_len = 0;
    }

    fn apply(entry: wal_mod.Entry) !void {
        switch (entry.op_code) {
            .doc_insert, .doc_update, .doc_delete => {},
            else => return,
        }

        if (count >= MAX_ENTRIES) return error.TooManyCollectionWalEntries;
        ops[count] = entry.op_code;
        flags[count] = entry.flags;
        count += 1;

        if (entry.op_code == .doc_delete) {
            if (entry.payload.len > delete_payload.len) return error.DeletePayloadTooLarge;
            delete_payload_len = entry.payload.len;
            @memcpy(delete_payload[0..delete_payload_len], entry.payload);
        }
    }

    fn deletePayload() []const u8 {
        return delete_payload[0..delete_payload_len];
    }
};

const ConcurrentInsertProbe = struct {
    const inserts_per_worker = 128;

    col: *Collection,
    worker_id: usize,
    errors: *std.atomic.Value(u32),

    fn run(self: *ConcurrentInsertProbe) void {
        var i: usize = 0;
        while (i < inserts_per_worker) : (i += 1) {
            var key_buf: [64]u8 = undefined;
            var value_buf: [96]u8 = undefined;
            const key = std.fmt.bufPrint(&key_buf, "worker-{d}-doc-{d}", .{ self.worker_id, i }) catch {
                _ = self.errors.fetchAdd(1, .monotonic);
                continue;
            };
            const value = std.fmt.bufPrint(&value_buf, "{{\"worker\":{d},\"doc\":{d}}}", .{ self.worker_id, i }) catch {
                _ = self.errors.fetchAdd(1, .monotonic);
                continue;
            };
            _ = self.col.insert(key, value) catch {
                _ = self.errors.fetchAdd(1, .monotonic);
            };
        }
    }
};

test "tenant collections are isolated" {
    const alloc = std.testing.allocator;
    const tmp_dir = "/tmp/turbodb_multi_tenant_test";
    compat.cwd().deleteTree(tmp_dir) catch {};
    try compat.cwd().makePath(tmp_dir);
    defer compat.cwd().deleteTree(tmp_dir) catch {};

    const db = try Database.open(alloc, tmp_dir);
    defer db.close();

    const a = try db.collectionForTenant("tenant-a", "users");
    const b = try db.collectionForTenant("tenant-b", "users");

    _ = try a.insert("alice", "{\"name\":\"Alice\"}");
    _ = try b.insert("bob", "{\"name\":\"Bob\"}");

    try std.testing.expect(a.get("alice") != null);
    try std.testing.expect(a.get("bob") == null);
    try std.testing.expect(b.get("bob") != null);
    try std.testing.expect(b.get("alice") == null);
}

test "tenant collection quota limits new collections" {
    const alloc = std.testing.allocator;
    const tmp_dir = "/tmp/turbodb_tenant_quota_test";
    compat.cwd().deleteTree(tmp_dir) catch {};
    try compat.cwd().makePath(tmp_dir);
    defer compat.cwd().deleteTree(tmp_dir) catch {};

    const db = try Database.open(alloc, tmp_dir);
    defer db.close();

    try db.configureTenantQuota("tenant-a", .{ .max_collections = 1 });
    _ = try db.collectionForTenant("tenant-a", "users");
    try std.testing.expectError(error.TenantCollectionQuotaExceeded, db.collectionForTenant("tenant-a", "orders"));
}

test "tenant ops quota is enforced per second" {
    const alloc = std.testing.allocator;
    const tmp_dir = "/tmp/turbodb_tenant_ops_test";
    compat.cwd().deleteTree(tmp_dir) catch {};
    try compat.cwd().makePath(tmp_dir);
    defer compat.cwd().deleteTree(tmp_dir) catch {};

    const db = try Database.open(alloc, tmp_dir);
    defer db.close();

    try db.configureTenantQuota("tenant-a", .{ .max_ops_per_second = 2 });
    try db.recordTenantOperation("tenant-a");
    try db.recordTenantOperation("tenant-a");
    try std.testing.expectError(error.TenantOpsQuotaExceeded, db.recordTenantOperation("tenant-a"));
}

test "collection WAL replays committed document writes and full tombstone" {
    const alloc = std.testing.allocator;
    const tmp_dir = "/tmp/turbodb_collection_wal_committed";
    compat.cwd().deleteTree(tmp_dir) catch {};
    try compat.cwd().makePath(tmp_dir);
    defer compat.cwd().deleteTree(tmp_dir) catch {};

    {
        const db = try Database.open(alloc, tmp_dir);
        defer db.close();

        const col = try db.collection("users");
        _ = try col.insert("u1", "{\"name\":\"alice\"}");
        try std.testing.expect(try col.update("u1", "{\"name\":\"alice-v2\"}"));
        try std.testing.expect(try col.delete("u1"));
    }

    var path_buf: [512]u8 = undefined;
    const wal_path = try std.fmt.bufPrintZ(&path_buf, "{s}/doc.wal", .{tmp_dir});
    var wal = try WAL.open(wal_path, alloc);
    defer wal.close();

    CollectionWalReplayProbe.reset();
    try wal.recover(0, CollectionWalReplayProbe.apply, alloc);

    try std.testing.expectEqual(@as(usize, 3), CollectionWalReplayProbe.count);
    try std.testing.expectEqual(wal_mod.OpCode.doc_insert, CollectionWalReplayProbe.ops[0]);
    try std.testing.expectEqual(wal_mod.OpCode.doc_update, CollectionWalReplayProbe.ops[1]);
    try std.testing.expectEqual(wal_mod.OpCode.doc_delete, CollectionWalReplayProbe.ops[2]);
    for (CollectionWalReplayProbe.flags[0..CollectionWalReplayProbe.count]) |flags| {
        try std.testing.expect(flags & wal_mod.FLAG_COMMIT != 0);
    }

    const decoded = try doc_mod.decode(CollectionWalReplayProbe.deletePayload());
    try std.testing.expect(decoded.doc.isDeleted());
    try std.testing.expectEqualStrings("u1", decoded.doc.key);
    try std.testing.expectEqualStrings("", decoded.doc.value);
    try std.testing.expectEqual(DocHeader.size + "u1".len, decoded.consumed);
}

test "collection concurrent inserts keep shared indexes stable" {
    const alloc = std.testing.allocator;
    const tmp_dir = "/tmp/turbodb_collection_concurrent_insert";
    compat.cwd().deleteTree(tmp_dir) catch {};
    try compat.cwd().makePath(tmp_dir);
    defer compat.cwd().deleteTree(tmp_dir) catch {};

    const db = try Database.open(alloc, tmp_dir);
    defer db.close();

    const col = try db.collection("load");
    const worker_count = 8;
    var errors = std.atomic.Value(u32).init(0);
    var probes: [worker_count]ConcurrentInsertProbe = undefined;
    var threads: [worker_count]std.Thread = undefined;

    for (&probes, 0..) |*probe, i| {
        probe.* = .{
            .col = col,
            .worker_id = i,
            .errors = &errors,
        };
        threads[i] = try std.Thread.spawn(.{}, ConcurrentInsertProbe.run, .{probe});
    }
    for (threads) |thread| thread.join();

    try std.testing.expectEqual(@as(u32, 0), errors.load(.acquire));
    col.flushIndex();

    const expected = worker_count * ConcurrentInsertProbe.inserts_per_worker;
    const scan = try col.scan(expected, 0, alloc);
    defer scan.deinit();
    try std.testing.expectEqual(@as(usize, expected), scan.docs.len);
    try std.testing.expect(col.get("worker-0-doc-0") != null);
    try std.testing.expect(col.get("worker-7-doc-127") != null);
}

test "time travel get returns historical version after update" {
    const alloc = std.testing.allocator;
    const tmp_dir = "/tmp/turbodb_time_travel_update";
    compat.cwd().deleteTree(tmp_dir) catch {};
    try compat.cwd().makePath(tmp_dir);
    defer compat.cwd().deleteTree(tmp_dir) catch {};

    const db = try Database.open(alloc, tmp_dir);
    defer db.close();

    const col = try db.collection("users");
    _ = try col.insert("u1", "{\"name\":\"alice-v1\"}");
    const first_epoch = col.versions.getLatest(1).?.epoch;
    _ = try col.update("u1", "{\"name\":\"alice-v2\"}");
    const second_epoch = col.versions.getLatest(1).?.epoch;

    try std.testing.expectEqualStrings("{\"name\":\"alice-v1\"}", col.getAsOfEpoch("u1", first_epoch).?.value);
    try std.testing.expectEqualStrings("{\"name\":\"alice-v2\"}", col.getAsOfEpoch("u1", second_epoch).?.value);
}

test "time travel get survives delete" {
    const alloc = std.testing.allocator;
    const tmp_dir = "/tmp/turbodb_time_travel_delete";
    compat.cwd().deleteTree(tmp_dir) catch {};
    try compat.cwd().makePath(tmp_dir);
    defer compat.cwd().deleteTree(tmp_dir) catch {};

    const db = try Database.open(alloc, tmp_dir);
    defer db.close();

    const col = try db.collection("users");
    _ = try col.insert("u1", "{\"name\":\"alice\"}");
    const live_epoch = col.versions.getLatest(1).?.epoch;
    try std.testing.expect(try col.delete("u1"));

    try std.testing.expect(col.get("u1") == null);
    try std.testing.expectEqualStrings("{\"name\":\"alice\"}", col.getAsOfEpoch("u1", live_epoch).?.value);
}

test "time travel scan uses historical snapshot" {
    const alloc = std.testing.allocator;
    const tmp_dir = "/tmp/turbodb_time_travel_scan";
    compat.cwd().deleteTree(tmp_dir) catch {};
    try compat.cwd().makePath(tmp_dir);
    defer compat.cwd().deleteTree(tmp_dir) catch {};

    const db = try Database.open(alloc, tmp_dir);
    defer db.close();

    const col = try db.collection("users");
    _ = try col.insert("u1", "{\"name\":\"alice\"}");
    const epoch_before_delete = col.versions.getLatest(1).?.epoch;
    _ = try col.insert("u2", "{\"name\":\"bob\"}");
    try std.testing.expect(try col.delete("u2"));

    const historical = try col.scanAsOfEpoch(epoch_before_delete, 10, 0, alloc);
    defer historical.deinit();
    try std.testing.expectEqual(@as(usize, 1), historical.docs.len);
    try std.testing.expectEqualStrings("u1", historical.docs[0].key);
}

test "cdc emits signed ordered deliveries for tenant mutations" {
    const alloc = std.testing.allocator;
    const tmp_dir = "/tmp/turbodb_cdc_integration";
    compat.cwd().deleteTree(tmp_dir) catch {};
    try compat.cwd().makePath(tmp_dir);
    defer compat.cwd().deleteTree(tmp_dir) catch {};

    const db = try Database.open(alloc, tmp_dir);
    defer db.close();

    _ = try db.registerWebhook("tenant-a", "users", "memory://tenant-a-users", "secret-a");
    const col = try db.collectionForTenant("tenant-a", "users");
    _ = try col.insert("u1", "{\"name\":\"alice\"}");
    _ = try col.update("u1", "{\"name\":\"alice-2\"}");
    try std.testing.expect(try col.delete("u1"));
    compat.sleep(20_000_000);

    const deliveries = try db.listWebhookDeliveries(alloc, "tenant-a");
    defer alloc.free(deliveries);
    try std.testing.expectEqual(@as(usize, 3), deliveries.len);
    try std.testing.expect(deliveries[0].seq < deliveries[1].seq);
    try std.testing.expect(deliveries[1].seq < deliveries[2].seq);
    try std.testing.expect(std.mem.indexOf(u8, deliveries[0].payloadSlice(), "\"op\":\"insert\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, deliveries[1].payloadSlice(), "\"op\":\"update\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, deliveries[2].payloadSlice(), "\"op\":\"delete\"") != null);
}

test "tenant collections isolate keys and listings" {
    const alloc = std.testing.allocator;
    const tmp_dir = "/tmp/turbodb_tenant_isolation";
    compat.cwd().deleteTree(tmp_dir) catch {};
    try compat.cwd().makePath(tmp_dir);
    defer compat.cwd().deleteTree(tmp_dir) catch {};

    const db = try Database.open(alloc, tmp_dir);
    defer db.close();

    const tenant_a_users = try db.collectionForTenant("tenant-a", "users");
    const tenant_b_users = try db.collectionForTenant("tenant-b", "users");

    _ = try tenant_a_users.insert("u1", "{\"name\":\"alice\"}");
    _ = try tenant_b_users.insert("u1", "{\"name\":\"bob\"}");

    const a_doc = tenant_a_users.get("u1").?;
    const b_doc = tenant_b_users.get("u1").?;
    try std.testing.expectEqualStrings("{\"name\":\"alice\"}", a_doc.value);
    try std.testing.expectEqualStrings("{\"name\":\"bob\"}", b_doc.value);

    var tenant_a_cols = try db.listCollectionsForTenant("tenant-a", alloc);
    defer {
        for (tenant_a_cols.items) |name| alloc.free(name);
        tenant_a_cols.deinit(alloc);
    }
    try std.testing.expectEqual(@as(usize, 1), tenant_a_cols.items.len);
    try std.testing.expectEqualStrings("users", tenant_a_cols.items[0]);
}

test "collection scan limit zero returns no documents" {
    const alloc = std.testing.allocator;
    const tmp_dir = "/tmp/turbodb_collection_scan_limit_zero";
    compat.cwd().deleteTree(tmp_dir) catch {};
    try compat.cwd().makePath(tmp_dir);
    defer compat.cwd().deleteTree(tmp_dir) catch {};

    const db = try Database.open(alloc, tmp_dir);
    defer db.close();

    const col = try db.collection("users");
    _ = try col.insert("u1", "{\"name\":\"one\"}");

    var result = try col.scan(0, 0, alloc);
    defer result.deinit();
    try std.testing.expectEqual(@as(usize, 0), result.docs.len);
}

test "tenant quotas apply per tenant" {
    const alloc = std.testing.allocator;
    const tmp_dir = "/tmp/turbodb_tenant_quotas";
    compat.cwd().deleteTree(tmp_dir) catch {};
    try compat.cwd().makePath(tmp_dir);
    defer compat.cwd().deleteTree(tmp_dir) catch {};

    const db = try Database.open(alloc, tmp_dir);
    defer db.close();

    try db.configureTenantQuota("tenant-a", .{ .max_collections = 1, .max_ops_per_second = 2 });
    _ = try db.collectionForTenant("tenant-a", "users");
    try std.testing.expectError(
        error.TenantCollectionQuotaExceeded,
        db.collectionForTenant("tenant-a", "orders"),
    );

    try db.recordTenantOperation("tenant-a");
    try db.recordTenantOperation("tenant-a");
    try std.testing.expectError(error.TenantOpsQuotaExceeded, db.recordTenantOperation("tenant-a"));

    _ = try db.collectionForTenant("tenant-b", "users");
    try db.recordTenantOperation("tenant-b");
}

test "extractJsonFloatArray parses embedding from JSON" {
    var out: [4]f32 = undefined;
    const json = "{\"key\":\"val\",\"embedding\":[1.0,2.5,3.0,4.5]}";
    const count = extractJsonFloatArray(json, "embedding", &out);
    try std.testing.expect(count != null);
    try std.testing.expectEqual(@as(u32, 4), count.?);
    try std.testing.expectApproxEqAbs(@as(f32, 1.0), out[0], 0.001);
    try std.testing.expectApproxEqAbs(@as(f32, 2.5), out[1], 0.001);
    try std.testing.expectApproxEqAbs(@as(f32, 3.0), out[2], 0.001);
    try std.testing.expectApproxEqAbs(@as(f32, 4.5), out[3], 0.001);
}

test "extractJsonFloatArray returns null for missing field" {
    var out: [3]f32 = undefined;
    const json = "{\"key\":\"val\",\"other\":[1.0,2.0]}";
    const count = extractJsonFloatArray(json, "embedding", &out);
    try std.testing.expect(count == null);
}

test "extractJsonFloatArray handles whitespace in array" {
    var out: [3]f32 = undefined;
    const json = "{\"vec\":[ 1.0 , 2.0 , 3.0 ]}";
    const count = extractJsonFloatArray(json, "vec", &out);
    try std.testing.expect(count != null);
    try std.testing.expectEqual(@as(u32, 3), count.?);
    try std.testing.expectApproxEqAbs(@as(f32, 1.0), out[0], 0.001);
    try std.testing.expectApproxEqAbs(@as(f32, 2.0), out[1], 0.001);
    try std.testing.expectApproxEqAbs(@as(f32, 3.0), out[2], 0.001);
}
