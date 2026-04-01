/// TurboDB — Collection (MVCC document store + query engine)
const std = @import("std");
const doc_mod   = @import("doc.zig");
const page_mod  = @import("page.zig");
const btree_mod = @import("btree.zig");
const codeindex = @import("codeindex.zig");
const wal_mod   = @import("wal");
const epoch_mod = @import("epoch");
const hot_cache = @import("hot_cache.zig");
const mvcc_mod  = @import("mvcc.zig");
const cdc_mod = @import("cdc.zig");
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

// ─── IndexQueue ──────────────────────────────────────────────────────────
// Lock-free MPSC queue for deferring trigram+word index builds to a background thread.
// Insert path pushes owned (key, value) pairs; background thread pops and indexes.
pub const IndexQueue = struct {
    const CAPACITY = 32768; // 32K entries per queue

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

// ─── Collection ──────────────────────────────────────────────────────────
pub const Collection = struct {
    pub const STRIPE_COUNT = 1024;

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
    stripe_locks: [STRIPE_COUNT]std.Thread.Mutex,
    hash_idx: std.AutoHashMap(u64, BTreeEntry),
    key_doc_ids: std.AutoHashMap(u64, u64),
    alloc: std.mem.Allocator,
    cache: hot_cache.HotCache,
    // MVCC version chain — append-only, no vacuum.
    versions: mvcc_mod.VersionChain,

    // Async index queues — inserts round-robin into 2 queues, 2 background threads drain them.
    index_queue: IndexQueue,
    index_queue2: IndexQueue,
    index_thread: ?std.Thread,
    index_thread2: ?std.Thread,
    index_stop: std.atomic.Value(bool),
    queue_toggle: std.atomic.Value(u32),
    indexing_count: std.atomic.Value(u32),

    /// Get the stripe lock index for a key hash.
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
        col.hash_idx = std.AutoHashMap(u64, BTreeEntry).init(alloc);
        col.key_doc_ids = std.AutoHashMap(u64, u64).init(alloc);
        col.alloc = alloc;
        col.cache = hot_cache.HotCache.init();
        col.versions = mvcc_mod.VersionChain.init(alloc);
        col.index_queue = IndexQueue.init(alloc);
        col.index_queue2 = IndexQueue.init(alloc);
        col.index_stop = std.atomic.Value(bool).init(false);
        col.queue_toggle = std.atomic.Value(u32).init(0);
        col.indexing_count = std.atomic.Value(u32).init(0);

        const n = @min(logical_name.len, col.name_buf.len - 1);
        @memcpy(col.name_buf[0..n], logical_name[0..n]);
        col.name_len = @intCast(n);
        try col.rebuildIndexes();

        // Start background indexer thread. If spawn fails, inserts fall back to sync
        // indexing via the queue-full path (queue stays empty, retries exhaust immediately).
        col.index_thread = std.Thread.spawn(.{}, indexWorkerQ, .{ col, &col.index_queue }) catch null;
        col.index_thread2 = null; // Second worker slot — available for future use

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
        self.versions.deinit(self.alloc);
        self.pf.close();
        self.alloc.destroy(self);
    }

    pub fn sync(self: *Collection) !void {
        try self.pf.sync();
    }

    /// Block until the background indexers have drained all pending items
    /// and finished processing the current batch.
    pub fn flushIndex(self: *Collection) void {
        var waited: u32 = 0;
        while ((self.index_queue.len() > 0 or self.index_queue2.len() > 0 or self.indexing_count.load(.acquire) > 0) and waited < 300_000) : (waited += 1) {
            std.Thread.sleep(100_000); // 100µs
        }
    }

    pub fn name(self: *const Collection) []const u8 {
        return self.name_buf[0..self.name_len];
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

        var enc_buf: [65536]u8 = undefined;
        const enc = try d.encodeBuf(&enc_buf);

        // Write to WAL buffer (background flusher will commit periodically).
        const txn = self.wal_log.next_lsn.load(.monotonic);
        _ = try self.wal_log.write(txn, .doc_insert, 0, 0, enc);

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

        // Async trigram + word indexing — push to background queue, sync fallback if full.
        if (value.len >= 3) {
            const owned_key = self.alloc.dupe(u8, key) catch null;
            const owned_val = self.alloc.dupe(u8, value) catch null;
            if (owned_key != null and owned_val != null) {
                const q = &self.index_queue;
                var retries: u32 = 0;
                while (!q.push(owned_key.?, owned_val.?)) {
                    retries += 1;
                    if (retries > 1000) {
                        // Queue full — fall back to synchronous indexing so no docs are lost.
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

        emitChange(self, .insert, key, value, doc_id);

        return doc_id;
    }

    // ─── get ─────────────────────────────────────────────────────────────

    /// Look up a document by key. Returns a decoded Doc or null.
    /// The Doc's key/value slices are valid until the next write to this collection.
    pub fn get(self: *Collection, key: []const u8) ?Doc {
        const key_hash = doc_mod.fnv1a(key);

        // L1: Hot cache (O(1), no hash table overhead)
        if (self.cache.lookup(key_hash)) |loc| {
            if (self.readLoc(loc.page_no, loc.page_off)) |d| return d;
        }

        // L2: Hash index (O(1), but hash table lookup)
        if (self.hash_idx.get(key_hash)) |entry| {
            if (self.readEntry(entry)) |d| {
                self.cache.insert(key_hash, entry.page_no, entry.page_off);
                return d;
            }
        }
        // L3: B-tree (O(log n))
        const entry = self.idx.search(key_hash) orelse return null;
        const d = self.readEntry(entry) orelse return null;
        self.cache.insert(key_hash, entry.page_no, entry.page_off);
        return d;
    }

    pub fn getAsOfEpoch(self: *Collection, key: []const u8, epoch: u64) ?Doc {
        const key_hash = doc_mod.fnv1a(key);
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

        const old_entry = self.idx.search(key_hash) orelse return false;
        const old_doc = self.readEntry(old_entry) orelse return false;

        const doc_id = old_doc.header.doc_id;
        var new_hdr = doc_mod.newHeader(doc_id, key, new_value);
        new_hdr.version = old_doc.header.version +% 1;
        // MVCC: encode old location into next_ver field.
        new_hdr.next_ver = (@as(u64, old_entry.page_no) << 16) | old_entry.page_off;

        const d = Doc{ .header = new_hdr, .key = key, .value = new_value };
        var enc_buf: [65536]u8 = undefined;
        const enc = try d.encodeBuf(&enc_buf);

        const txn = self.wal_log.next_lsn.load(.monotonic);
        _ = try self.wal_log.write(txn, .doc_update, 0, 0, enc);

        const pno = try self.findOrAllocLeaf(enc.len);
        const page_off = self.pf.leafAppend(pno, enc) orelse return error.PageFull;

        const new_entry = BTreeEntry{
            .key_hash = key_hash,
            .doc_id   = doc_id,
            .page_no  = pno,
            .page_off = page_off,
        };
        self.hash_idx.put(key_hash, new_entry) catch {};
        self.cache.invalidate(key_hash);

        // MVCC: register new version in the version chain (links to old automatically).
        const epoch = self.epochs.advance();
        self.versions.appendVersion(self.alloc, doc_id, pno, page_off, epoch) catch {};
        emitChange(self, .update, key, new_value, doc_id);

        return true;
    }

    // ─── delete ──────────────────────────────────────────────────────────

    pub fn delete(self: *Collection, key: []const u8) !bool {
        const key_hash = doc_mod.fnv1a(key);
        const stripe = stripeIndex(key_hash);
        self.stripe_locks[stripe].lock();
        defer self.stripe_locks[stripe].unlock();

        const entry = self.idx.search(key_hash) orelse return false;

        const old_doc = self.readEntry(entry) orelse return false;
        var tomb_hdr = doc_mod.newHeader(old_doc.header.doc_id, key, "");
        tomb_hdr.flags |= DocHeader.DELETED;
        tomb_hdr.version = old_doc.header.version +% 1;
        tomb_hdr.next_ver = (@as(u64, entry.page_no) << 16) | entry.page_off;
        const txn = self.wal_log.next_lsn.load(.monotonic);
        _ = try self.wal_log.write(txn, .doc_delete, 0, 0, std.mem.asBytes(&tomb_hdr));
        const tomb_doc = Doc{ .header = tomb_hdr, .key = key, .value = "" };
        var enc_buf: [65536]u8 = undefined;
        const enc = try tomb_doc.encodeBuf(&enc_buf);
        const pno = try self.findOrAllocLeaf(enc.len);
        const page_off = self.pf.leafAppend(pno, enc) orelse return error.PageFull;
        const epoch = self.epochs.advance();
        self.versions.appendVersion(self.alloc, old_doc.header.doc_id, pno, page_off, epoch) catch {};
        self.idx.delete(key_hash);
        _ = self.hash_idx.remove(key_hash);
        self.cache.invalidate(key_hash);
        emitChange(self, .delete, key, "", old_doc.header.doc_id);
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
        var results: std.ArrayList(Doc) = .empty;
        errdefer results.deinit(alloc);

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
        var results: std.ArrayList(Doc) = .empty;
        errdefer results.deinit(alloc);

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
};

/// Background thread that drains the index queue and builds trigram indexes.
fn indexWorkerQ(col: *Collection, queue: *IndexQueue) void {
    const BATCH = 64;
    var batch_keys: [BATCH][]const u8 = undefined;
    var batch_vals: [BATCH][]const u8 = undefined;
    // Reusable trigram set — cleared between documents, never freed until shutdown.
    var reusable_tris = std.AutoHashMap(codeindex.Trigram, void).init(col.alloc);
    defer reusable_tris.deinit();

    while (!col.index_stop.load(.acquire)) {
        var n: usize = 0;
        while (n < BATCH) {
            const entry = queue.pop() orelse break;
            batch_keys[n] = entry.key;
            batch_vals[n] = entry.value;
            n += 1;
        }
        if (n == 0) {
            std.Thread.yield() catch {};
            continue;
        }
        _ = col.indexing_count.fetchAdd(1, .release);
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
    mu: std.Thread.RwLock,

    pub const TenantQuota = struct {
        max_collections: u32 = std.math.maxInt(u32),
        max_storage_bytes: usize = std.math.maxInt(usize),
        max_ops_per_second: u32 = std.math.maxInt(u32),
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

        const now_ms: i64 = std.time.milliTimestamp();
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
    var fbs = std.io.fixedBufferStream(buf);
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
    return try std.fs.realpathAlloc(alloc, data_dir);
}

fn ensureDirPath(data_dir: []const u8) !void {
    if (std.fs.path.isAbsolute(data_dir)) {
        var root = try std.fs.openDirAbsolute("/", .{});
        defer root.close();
        const rel = std.mem.trimLeft(u8, data_dir, "/");
        if (rel.len > 0) root.makePath(rel) catch |err| switch (err) {
            error.PathAlreadyExists => {},
            else => return err,
        };
    } else {
        std.fs.cwd().makePath(data_dir) catch |err| switch (err) {
            error.PathAlreadyExists => {},
            else => return err,
        };
    }
}

test "tenant collections are isolated" {
    const alloc = std.testing.allocator;
    const tmp_dir = "/tmp/turbodb_multi_tenant_test";
    std.fs.cwd().deleteTree(tmp_dir) catch {};
    try std.fs.cwd().makePath(tmp_dir);
    defer std.fs.cwd().deleteTree(tmp_dir) catch {};

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
    std.fs.cwd().deleteTree(tmp_dir) catch {};
    try std.fs.cwd().makePath(tmp_dir);
    defer std.fs.cwd().deleteTree(tmp_dir) catch {};

    const db = try Database.open(alloc, tmp_dir);
    defer db.close();

    try db.configureTenantQuota("tenant-a", .{ .max_collections = 1 });
    _ = try db.collectionForTenant("tenant-a", "users");
    try std.testing.expectError(error.TenantCollectionQuotaExceeded, db.collectionForTenant("tenant-a", "orders"));
}

test "tenant ops quota is enforced per second" {
    const alloc = std.testing.allocator;
    const tmp_dir = "/tmp/turbodb_tenant_ops_test";
    std.fs.cwd().deleteTree(tmp_dir) catch {};
    try std.fs.cwd().makePath(tmp_dir);
    defer std.fs.cwd().deleteTree(tmp_dir) catch {};

    const db = try Database.open(alloc, tmp_dir);
    defer db.close();

    try db.configureTenantQuota("tenant-a", .{ .max_ops_per_second = 2 });
    try db.recordTenantOperation("tenant-a");
    try db.recordTenantOperation("tenant-a");
    try std.testing.expectError(error.TenantOpsQuotaExceeded, db.recordTenantOperation("tenant-a"));
}

test "time travel get returns historical version after update" {
    const alloc = std.testing.allocator;
    const tmp_dir = "/tmp/turbodb_time_travel_update";
    std.fs.cwd().deleteTree(tmp_dir) catch {};
    try std.fs.cwd().makePath(tmp_dir);
    defer std.fs.cwd().deleteTree(tmp_dir) catch {};

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
    std.fs.cwd().deleteTree(tmp_dir) catch {};
    try std.fs.cwd().makePath(tmp_dir);
    defer std.fs.cwd().deleteTree(tmp_dir) catch {};

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
    std.fs.cwd().deleteTree(tmp_dir) catch {};
    try std.fs.cwd().makePath(tmp_dir);
    defer std.fs.cwd().deleteTree(tmp_dir) catch {};

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
    std.fs.cwd().deleteTree(tmp_dir) catch {};
    try std.fs.cwd().makePath(tmp_dir);
    defer std.fs.cwd().deleteTree(tmp_dir) catch {};

    const db = try Database.open(alloc, tmp_dir);
    defer db.close();

    _ = try db.registerWebhook("tenant-a", "users", "memory://tenant-a-users", "secret-a");
    const col = try db.collectionForTenant("tenant-a", "users");
    _ = try col.insert("u1", "{\"name\":\"alice\"}");
    _ = try col.update("u1", "{\"name\":\"alice-2\"}");
    try std.testing.expect(try col.delete("u1"));
    std.Thread.sleep(20_000_000);

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
    std.fs.cwd().deleteTree(tmp_dir) catch {};
    try std.fs.cwd().makePath(tmp_dir);
    defer std.fs.cwd().deleteTree(tmp_dir) catch {};

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

test "tenant quotas apply per tenant" {
    const alloc = std.testing.allocator;
    const tmp_dir = "/tmp/turbodb_tenant_quotas";
    std.fs.cwd().deleteTree(tmp_dir) catch {};
    try std.fs.cwd().makePath(tmp_dir);
    defer std.fs.cwd().deleteTree(tmp_dir) catch {};

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
