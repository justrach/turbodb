/// TurboDB — Collection (MVCC document store + query engine)
const std = @import("std");
const doc_mod   = @import("doc.zig");
const page_mod  = @import("page.zig");
const btree_mod = @import("btree.zig");
const trigram   = @import("trigram.zig");
const wal_mod   = @import("wal");
const epoch_mod = @import("epoch");
const Doc = doc_mod.Doc;
const DocHeader = doc_mod.DocHeader;
const PageFile = page_mod.PageFile;
const BTree = btree_mod.BTree;
const BTreeEntry = btree_mod.BTreeEntry;
const WAL = wal_mod.WAL;
const EpochManager = epoch_mod.EpochManager;
const TrigramIndex = trigram.TrigramIndex;


// ─── Collection ──────────────────────────────────────────────────────────
// ─── Collection ──────────────────────────────────────────────────────────
pub const Collection = struct {
    name_buf: [64]u8,
    name_len: u8,
    pf: PageFile,
    idx: BTree,
    tri: TrigramIndex,
    wal_log: *WAL,
    epochs: *EpochManager,
    next_doc_id: std.atomic.Value(u64),
    write_mu: std.Thread.Mutex,
    alloc: std.mem.Allocator,

    pub fn open(
        alloc: std.mem.Allocator,
        data_dir: []const u8,
        col_name: []const u8,
        wal_log: *WAL,
        epochs: *EpochManager,
    ) !*Collection {
        var path_buf: [512]u8 = undefined;
        const col = try alloc.create(Collection);
        errdefer alloc.destroy(col);

        const page_path = try std.fmt.bufPrint(&path_buf, "{s}/{s}.pages", .{ data_dir, col_name });
        col.pf = try PageFile.open(page_path);

        col.idx = BTree.init(&col.pf, 0);
        col.tri = TrigramIndex.init(alloc);
        col.wal_log = wal_log;
        col.epochs = epochs;
        col.next_doc_id = std.atomic.Value(u64).init(1);
        col.write_mu = .{};
        col.alloc = alloc;

        const n = @min(col_name.len, 63);
        @memcpy(col.name_buf[0..n], col_name[0..n]);
        col.name_len = @intCast(n);

        return col;
    }
    pub fn close(self: *Collection) void {
        self.tri.deinit();
        self.pf.close();
        self.alloc.destroy(self);
    }

    pub fn name(self: *const Collection) []const u8 {
        return self.name_buf[0..self.name_len];
    }

    // ─── insert ──────────────────────────────────────────────────────────

    /// Insert a new document. Returns assigned doc_id.
    pub fn insert(self: *Collection, key: []const u8, value: []const u8) !u64 {
        self.write_mu.lock();
        defer self.write_mu.unlock();

        const doc_id = self.next_doc_id.fetchAdd(1, .monotonic); // under write_mu
        const hdr = doc_mod.newHeader(doc_id, key, value);
        const d = Doc{ .header = hdr, .key = key, .value = value };

        var enc_buf: [65536]u8 = undefined;
        const enc = try d.encodeBuf(&enc_buf);

        // Write to WAL buffer (background flusher will commit periodically).
        const txn = self.wal_log.next_lsn.load(.monotonic);
        _ = try self.wal_log.write(txn, .doc_insert, 0, 0, enc);
        // NOTE: no commit() here — background flusher batches fsyncs every ~1ms

        // Find (or allocate) a leaf page with enough space.
        const pno = try self.findOrAllocLeaf(enc.len);
        const page_off = self.pf.leafAppend(pno, enc) orelse
            return error.PageFull; // should not happen after findOrAllocLeaf

        // Index the document.
        const entry = BTreeEntry{
            .key_hash = hdr.key_hash,
            .doc_id   = doc_id,
            .page_no  = pno,
            .page_off = page_off,
        };
        try self.idx.insert(entry);

        // Trigram index for full-text search
        self.tri.indexDoc(doc_id, value) catch {};

        return doc_id;
    }

    // ─── get ─────────────────────────────────────────────────────────────

    /// Look up a document by key. Returns a decoded Doc or null.
    /// The Doc's key/value slices are valid until the next write to this collection.
    pub fn get(self: *Collection, key: []const u8) ?Doc {
        const key_hash = doc_mod.fnv1a(key);
        const entry = self.idx.search(key_hash) orelse return null;
        return self.readEntry(entry);
    }

    /// Look up a document by doc_id using a linear scan of the index.
    pub fn getById(self: *Collection, doc_id: u64) ?Doc {
        // B-tree is keyed by key_hash, not doc_id.
        // For production, maintain a secondary doc_id→(page,off) map.
        // Here we do a page scan (acceptable for moderate collections).
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
    pub fn update(self: *Collection, key: []const u8, new_value: []const u8) !bool {
        self.write_mu.lock();
        defer self.write_mu.unlock();

        const key_hash = doc_mod.fnv1a(key);
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
        try self.idx.insert(new_entry); // overwrites old entry in B-tree

        // Update trigram index
        self.tri.removeDoc(doc_id, old_doc.value);
        self.tri.indexDoc(doc_id, new_value) catch {};

        return true;
    }

    // ─── delete ──────────────────────────────────────────────────────────

    pub fn delete(self: *Collection, key: []const u8) !bool {
        self.write_mu.lock();
        defer self.write_mu.unlock();

        const key_hash = doc_mod.fnv1a(key);
        const entry = self.idx.search(key_hash) orelse return false;

        // Write tombstone.
        var tomb_hdr: DocHeader = std.mem.zeroes(DocHeader);
        tomb_hdr.key_hash = key_hash;
        tomb_hdr.flags = DocHeader.DELETED;
        const txn = self.wal_log.next_lsn.load(.monotonic);
        _ = try self.wal_log.write(txn, .doc_delete, 0, 0, std.mem.asBytes(&tomb_hdr));

        // Mark the stored document as deleted.
        if (self.readEntryMut(entry)) |mut_hdr| {
            mut_hdr.flags |= DocHeader.DELETED;
        }
        self.idx.delete(key_hash);
        return true;
    }

    // ─── search (trigram-indexed) ─────────────────────────────────────

    pub const TextSearchResult = struct {
        docs: []Doc,
        candidates: u64,
        total_docs: u64,
        trigrams_used: u32,
        alloc: std.mem.Allocator,
        pub fn deinit(self: TextSearchResult) void { self.alloc.free(self.docs); }
    };

    /// Full-text substring search using the trigram index.
    /// Returns only documents whose value contains `query` (case-insensitive).
    pub fn searchText(
        self: *Collection,
        query: []const u8,
        limit: u32,
        alloc: std.mem.Allocator,
    ) !TextSearchResult {
        // Phase 1: trigram index narrows candidates
        var sr = try self.tri.search(query, alloc);
        defer sr.deinit(alloc);

        // Phase 2: verify candidates by loading docs and checking substring
        var results: std.ArrayList(Doc) = .empty;
        errdefer results.deinit(alloc);

        // Lowercase query for comparison
        var q_lower_buf: [1024]u8 = undefined;
        const q_len = @min(query.len, q_lower_buf.len);
        for (0..q_len) |i| {
            q_lower_buf[i] = if (query[i] >= 'A' and query[i] <= 'Z') query[i] + 32 else query[i];
        }
        const q_lower = q_lower_buf[0..q_len];

        for (sr.candidates) |doc_id| {
            if (results.items.len >= limit) break;
            const doc = self.getById(doc_id) orelse continue;
            // Case-insensitive substring match on value
            if (containsLower(doc.value, q_lower)) {
                try results.append(alloc, doc);
            }
        }

        return TextSearchResult{
            .docs = try results.toOwnedSlice(alloc),
            .candidates = @intCast(sr.candidates.len),
            .total_docs = sr.total_docs,
            .trigrams_used = sr.trigrams_used,
            .alloc = alloc,
        };
    }

    fn containsLower(haystack: []const u8, needle_lower: []const u8) bool {
        if (needle_lower.len == 0) return true;
        if (haystack.len < needle_lower.len) return false;
        for (0..haystack.len - needle_lower.len + 1) |i| {
            var match = true;
            for (0..needle_lower.len) |j| {
                const hc = haystack[i + j];
                const h_lower = if (hc >= 'A' and hc <= 'Z') hc + 32 else hc;
                if (h_lower != needle_lower[j]) {
                    match = false;
                    break;
                }
            }
            if (match) return true;
        }
        return false;
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

    // ─── private helpers ─────────────────────────────────────────────────

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
        // Search recent pages for one with enough space.
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
};

// ─── Database (multi-collection manager) ─────────────────────────────────

pub const Database = struct {
    collections: std.StringHashMap(*Collection),
    wal_log: WAL,
    epochs: EpochManager,
    data_dir_buf: [256]u8,
    data_dir_len: usize,
    alloc: std.mem.Allocator,
    mu: std.Thread.Mutex,

    pub fn open(alloc: std.mem.Allocator, data_dir: []const u8) !*Database {
        const db = try alloc.create(Database);
        errdefer alloc.destroy(db);

        var path_buf: [512]u8 = undefined;
        const wal_path = try std.fmt.bufPrintZ(&path_buf, "{s}/doc.wal", .{data_dir});
        db.wal_log = try WAL.open(wal_path, alloc);
        // Start background WAL flusher (batches fsyncs every ~1ms like MongoDB's w:1)
        try db.wal_log.startFlusher();
        db.epochs = try EpochManager.init(alloc);
        db.collections = std.StringHashMap(*Collection).init(alloc);
        db.alloc = alloc;
        db.mu = .{};

        const n = @min(data_dir.len, 255);
        @memcpy(db.data_dir_buf[0..n], data_dir[0..n]);
        db.data_dir_len = n;

        return db;
    }

    pub fn close(self: *Database) void {
        var it = self.collections.valueIterator();
        while (it.next()) |col| col.*.close();
        self.collections.deinit();
        self.wal_log.close();
        self.epochs.deinit();
        self.alloc.destroy(self);
    }

    pub fn dataDir(self: *const Database) []const u8 {
        return self.data_dir_buf[0..self.data_dir_len];
    }

    /// Get or create a named collection.
    pub fn collection(self: *Database, name: []const u8) !*Collection {
        self.mu.lock();
        defer self.mu.unlock();
        if (self.collections.get(name)) |c| return c;

        const col = try Collection.open(
            self.alloc,
            self.dataDir(),
            name,
            &self.wal_log,
            &self.epochs,
        );
        const key = try self.alloc.dupe(u8, name);
        try self.collections.put(key, col);
        return col;
    }

    pub fn dropCollection(self: *Database, name: []const u8) void {
        self.mu.lock();
        defer self.mu.unlock();
        if (self.collections.fetchRemove(name)) |kv| {
            kv.value.close();
            self.alloc.free(kv.key);
        }
    }
};
