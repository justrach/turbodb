/// TurboDB — B-tree index
/// Maps key_hash (u64) → (page_no u32, page_off u16) for O(log N) lookups.
/// Branching factor ~200 — a 3-level tree handles 8 million documents.
///
/// Internal node layout (usable area, PAGE_USABLE = 4064 bytes):
///   [n_keys: u16][child[0] u32][key[0] u64][child[1] u32] ... [key[n-1] u64][child[n] u32]
///   max n_keys = (PAGE_USABLE - 4) / 12 = 338 → branching factor 339
///
/// Leaf node layout:
///   [n_entries: u16][entry[0]: BTreeEntry][entry[1]: BTreeEntry]...
///   BTreeEntry = {key_hash u64, page_no u32, page_off u16, doc_id u64} = 22 bytes
///   max entries per leaf = (PAGE_USABLE - 2) / 22 = 184
const std = @import("std");
const compat = @import("compat");
const page_mod = @import("page.zig");
const PageFile = page_mod.PageFile;
const PAGE_USABLE = page_mod.PAGE_USABLE;

// ─── BTreeEntry (22 bytes) ────────────────────────────────────────────────

pub const BTreeEntry = extern struct {
    key_hash: u64 align(8), // 0
    doc_id:   u64,           // 8
    page_no:  u32,           // 16
    page_off: u16,           // 20
    // total: 24 (extern struct padded to align(8))
    pub const size = @sizeOf(BTreeEntry);
    comptime { std.debug.assert(size == 24); }
};

const MAX_ENTRIES_PER_LEAF: usize = (PAGE_USABLE - 2) / BTreeEntry.size;
const MAX_KEYS_PER_INTERNAL: usize = (PAGE_USABLE - 4) / 12; // 12 = 4 (child) + 8 (key)
const MIN_ENTRIES: usize = MAX_ENTRIES_PER_LEAF / 2;
const MIN_KEYS: usize = MAX_KEYS_PER_INTERNAL / 2;

// ─── BTree ────────────────────────────────────────────────────────────────

pub const BTree = struct {
    pf: *PageFile,
    root: u32,   // root page number (0 = not yet created)
    mu: compat.RwLock,

    pub fn init(pf: *PageFile, root_page: u32) BTree {
        return .{ .pf = pf, .root = root_page, .mu = .{} };
    }

    // ─── search ──────────────────────────────────────────────────────────

    /// Find an entry by key_hash. Returns null if not found.
    pub fn search(self: *BTree, key_hash: u64) ?BTreeEntry {
        self.mu.lockShared();
        defer self.mu.unlockShared();
        if (self.root == 0) return null;
        return self.searchPage(self.root, key_hash);
    }

    fn searchPage(self: *BTree, pno: u32, key_hash: u64) ?BTreeEntry {
        const ph = self.pf.pageHeader(pno);
        const data = self.pf.pageData(pno);
        if (@as(page_mod.PageType, @enumFromInt(ph.page_type)) == .btree_leaf) {
            return searchLeaf(data, key_hash);
        }
        // Internal: find child.
        const child_pno = internalFindChild(data, key_hash);
        if (child_pno == 0) return null;
        return self.searchPage(child_pno, key_hash);
    }

    // ─── insert ──────────────────────────────────────────────────────────

    /// Insert or update an entry. Returns error if allocation fails.
    pub fn insert(self: *BTree, entry: BTreeEntry) !void {
        self.mu.lock();
        defer self.mu.unlock();

        if (self.root == 0) {
            // Create first root leaf (btree_leaf, NOT document leaf).
            self.root = try self.pf.allocPage(.btree_leaf);
            leafSetCount(self.pf.pageData(self.root), 0);
        }

        if (try self.insertPage(self.root, entry)) |split| {
            // Root was split — create new root.
            const new_root = try self.pf.allocPage(.internal);
            const d = self.pf.pageData(new_root);
            internalInit(d, self.root, split.median, split.right);
            self.root = new_root;
        }
    }

    const SplitResult = struct { median: u64, right: u32 };

    fn insertPage(self: *BTree, pno: u32, entry: BTreeEntry) !?SplitResult {
        const ph = self.pf.pageHeader(pno);
        const data = self.pf.pageData(pno);
        if (@as(page_mod.PageType, @enumFromInt(ph.page_type)) == .btree_leaf) {
            return self.leafInsert(pno, data, entry);
        }
        // Internal node.
        const child_idx = internalFindChildIdx(data, entry.key_hash);
        const child_pno = internalGetChild(data, child_idx);
        const maybe_split = try self.insertPage(child_pno, entry);
        if (maybe_split) |split| {
            return self.internalInsertKey(pno, data, child_idx, split.median, split.right);
        }
        return null;
    }

    fn leafInsert(self: *BTree, pno: u32, data: []u8, entry: BTreeEntry) !?SplitResult {
        const n = leafCount(data);

        // Check for update (same key_hash → overwrite).
        const entries = leafEntriesMut(data, n);
        var lo: usize = 0;
        var hi: usize = n;
        while (lo < hi) {
            const mid = lo + (hi - lo) / 2;
            if (entries[mid].key_hash < entry.key_hash) lo = mid + 1
            else hi = mid;
        }
        if (lo < n and entries[lo].key_hash == entry.key_hash) {
            // Overwrite.
            entries[lo] = entry;
            return null;
        }
        // Insert at position lo (shift right).
        if (n < MAX_ENTRIES_PER_LEAF) {
            const slice = leafEntriesMut(data, n + 1);
            var i: usize = n;
            while (i > lo) : (i -= 1) slice[i] = slice[i - 1];
            slice[lo] = entry;
            leafSetCount(data, n + 1);
            return null;
        }
        // Leaf is full — split.
        const right_pno = try self.pf.allocPage(.btree_leaf);
        const rdata = self.pf.pageData(right_pno);
        const mid = n / 2;
        // Copy right half to new leaf.
        const all = leafEntries(data, n);
        const right_entries = leafEntriesMut(rdata, n - mid);
        @memcpy(right_entries, all[mid..n]);
        leafSetCount(rdata, n - mid);
        // Trim left leaf.
        leafSetCount(data, mid);
        // Determine insertion side and insert.
        if (lo <= mid) {
            _ = try self.leafInsert(pno, data, entry);
        } else {
            _ = try self.leafInsert(right_pno, rdata, entry);
        }
        return SplitResult{ .median = right_entries[0].key_hash, .right = right_pno };
    }

    fn internalInsertKey(
        self: *BTree,
        _: u32,
        data: []u8,
        child_idx: usize,
        median: u64,
        right_child: u32,
    ) !?SplitResult {
        const n = internalKeyCount(data);
        if (n < MAX_KEYS_PER_INTERNAL) {
            internalShiftRight(data, n, child_idx, median, right_child);
            return null;
        }
        // Split internal node.
        const right_pno = try self.pf.allocPage(.internal);
        const rdata = self.pf.pageData(right_pno);
        const split_median = internalSplit(data, rdata, n, child_idx, median, right_child);
        return SplitResult{ .median = split_median, .right = right_pno };
    }

    // ─── delete ──────────────────────────────────────────────────────────

    pub fn delete(self: *BTree, key_hash: u64) void {
        self.mu.lock();
        defer self.mu.unlock();
        if (self.root == 0) return;
        _ = self.deletePage(self.root, key_hash);
    }

    fn deletePage(self: *BTree, pno: u32, key_hash: u64) bool {
        const ph = self.pf.pageHeader(pno);
        const data = self.pf.pageData(pno);
        if (@as(page_mod.PageType, @enumFromInt(ph.page_type)) == .btree_leaf) {
            return leafDelete(data, key_hash);
        }
        const child_idx = internalFindChildIdx(data, key_hash);
        const child_pno = internalGetChild(data, child_idx);
        if (child_pno == 0) return false;
        return self.deletePage(child_pno, key_hash);
    }
};

// ─── leaf page helpers ────────────────────────────────────────────────────

fn leafCount(data: []const u8) usize {
    return @as(u16, @bitCast([2]u8{ data[0], data[1] }));
}

fn leafSetCount(data: []u8, n: usize) void {
    const bytes: [2]u8 = @bitCast(@as(u16, @intCast(n)));
    data[0] = bytes[0];
    data[1] = bytes[1];
}

fn leafEntries(data: []const u8, n: usize) []align(1) const BTreeEntry {
    const bytes = data[2..][0 .. n * BTreeEntry.size];
    return std.mem.bytesAsSlice(BTreeEntry, bytes);
}

fn leafEntriesMut(data: []u8, n: usize) []align(1) BTreeEntry {
    const bytes = data[2..][0 .. n * BTreeEntry.size];
    return std.mem.bytesAsSlice(BTreeEntry, bytes);
}
fn searchLeaf(data: []const u8, key_hash: u64) ?BTreeEntry {
    const n = leafCount(data);
    const entries = leafEntries(data, n);
    var lo: usize = 0;
    var hi: usize = n;
    while (lo < hi) {
        const mid = lo + (hi - lo) / 2;
        if (entries[mid].key_hash < key_hash) lo = mid + 1
        else hi = mid;
    }
    if (lo < n and entries[lo].key_hash == key_hash) return entries[lo];
    return null;
}

fn leafDelete(data: []u8, key_hash: u64) bool {
    const n = leafCount(data);
    const entries = leafEntriesMut(data, n);
    var lo: usize = 0;
    var hi: usize = n;
    while (lo < hi) {
        const mid = lo + (hi - lo) / 2;
        if (entries[mid].key_hash < key_hash) lo = mid + 1
        else hi = mid;
    }
    if (lo >= n or entries[lo].key_hash != key_hash) return false;
    var i = lo;
    while (i + 1 < n) : (i += 1) entries[i] = entries[i + 1];
    leafSetCount(data, n - 1);
    return true;
}

// ─── internal page helpers ────────────────────────────────────────────────
// Layout: [n_keys:u16][child0:u32][key0:u64][child1:u32][key1:u64]...[keyN-1:u64][childN:u32]

fn internalKeyCount(data: []const u8) usize {
    return @as(u16, @bitCast([2]u8{ data[0], data[1] }));
}

fn internalSetKeyCount(data: []u8, n: usize) void {
    const bytes: [2]u8 = @bitCast(@as(u16, @intCast(n)));
    data[0] = bytes[0];
    data[1] = bytes[1];
}

/// Offset of child[i] within data (after the 2-byte count + 2-byte pad).
fn childOff(i: usize) usize { return 4 + i * 12; }
fn keyOff(i: usize) usize { return 4 + i * 12 + 4; }

fn internalGetChild(data: []const u8, i: usize) u32 {
    const off = childOff(i);
    return std.mem.readInt(u32, data[off..][0..4], .little);
}

fn internalSetChild(data: []u8, i: usize, pno: u32) void {
    const off = childOff(i);
    std.mem.writeInt(u32, data[off..][0..4], pno, .little);
}

fn internalGetKey(data: []const u8, i: usize) u64 {
    const off = keyOff(i);
    return std.mem.readInt(u64, data[off..][0..8], .little);
}

fn internalSetKey(data: []u8, i: usize, k: u64) void {
    const off = keyOff(i);
    std.mem.writeInt(u64, data[off..][0..8], k, .little);
}

fn internalInit(data: []u8, left: u32, median: u64, right: u32) void {
    internalSetKeyCount(data, 1);
    internalSetChild(data, 0, left);
    internalSetKey(data, 0, median);
    internalSetChild(data, 1, right);
}

fn internalFindChildIdx(data: []const u8, key_hash: u64) usize {
    const n = internalKeyCount(data);
    var lo: u32 = 0;
    var hi: u32 = @intCast(n);
    while (lo < hi) {
        const mid = lo + (hi - lo) / 2;
        if (internalGetKey(data, mid) <= key_hash) {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }
    return lo;
}

fn internalFindChild(data: []const u8, key_hash: u64) u32 {
    return internalGetChild(data, internalFindChildIdx(data, key_hash));
}

fn internalShiftRight(
    data: []u8,
    n: usize,
    after_child: usize,
    median: u64,
    right_child: u32,
) void {
    // Shift keys and children right of after_child.
    var i = n;
    while (i > after_child) : (i -= 1) {
        internalSetKey(data, i, internalGetKey(data, i - 1));
        internalSetChild(data, i + 1, internalGetChild(data, i));
    }
    internalSetKey(data, after_child, median);
    internalSetChild(data, after_child + 1, right_child);
    internalSetKeyCount(data, n + 1);
}

fn internalSplit(
    ldata: []u8,
    rdata: []u8,
    n: usize,
    after_child: usize,
    median: u64,
    right_child: u32,
) u64 {
    // Build a temporary full array of keys+children in order.
    var tmp_keys: [MAX_KEYS_PER_INTERNAL + 1]u64 = undefined;
    var tmp_children: [MAX_KEYS_PER_INTERNAL + 2]u32 = undefined;
    var tk: usize = 0;
    var tc: usize = 0;
    var inserted = false;
    var i: usize = 0;
    while (i <= n) : (i += 1) {
        if (!inserted and i == after_child + 1) {
            tmp_keys[tk] = median;
            tmp_children[tc] = right_child;
            tk += 1;
            tc += 1;
            inserted = true;
        }
        if (i < n) { tmp_keys[tk] = internalGetKey(ldata, i); tk += 1; }
        tmp_children[tc] = internalGetChild(ldata, i); tc += 1;
    }
    if (!inserted) {
        tmp_keys[tk] = median;
        tmp_children[tc] = right_child;
        tk += 1;
        tc += 1;
    }

    const total_keys = tk;
    const mid = total_keys / 2;
    const split_median = tmp_keys[mid];

    // Left: keys 0..mid-1, children 0..mid
    internalSetKeyCount(ldata, mid);
    for (0..mid) |j| { internalSetKey(ldata, j, tmp_keys[j]); internalSetChild(ldata, j, tmp_children[j]); }
    internalSetChild(ldata, mid, tmp_children[mid]);

    // Right: keys mid+1..total_keys-1, children mid+1..total_keys
    const right_n = total_keys - mid - 1;
    internalSetKeyCount(rdata, right_n);
    for (0..right_n) |j| { internalSetKey(rdata, j, tmp_keys[mid + 1 + j]); internalSetChild(rdata, j, tmp_children[mid + 1 + j]); }
    internalSetChild(rdata, right_n, tmp_children[total_keys]);

    return split_median;
}

// ─── tests ────────────────────────────────────────────────────────────────

test "btree leaf insert/search" {
    // We cannot open a real file in a unit test easily, so just test the helpers.
    var data: [PAGE_USABLE]u8 = std.mem.zeroes([PAGE_USABLE]u8);
    leafSetCount(&data, 0);

    const e1 = BTreeEntry{ .key_hash = 100, .doc_id = 1, .page_no = 0, .page_off = 0 };
    const e2 = BTreeEntry{ .key_hash = 50,  .doc_id = 2, .page_no = 0, .page_off = 0 };
    const e3 = BTreeEntry{ .key_hash = 200, .doc_id = 3, .page_no = 0, .page_off = 0 };

    // Insert e1
    var n = leafCount(&data);
    const entries = leafEntriesMut(&data, n + 1);
    entries[0] = e1;
    leafSetCount(&data, 1);
    // Insert e2 at front
    n = leafCount(&data);
    const e2_entries = leafEntriesMut(&data, n + 1);
    e2_entries[1] = e2_entries[0];
    e2_entries[0] = e2;
    leafSetCount(&data, 2);
    // Insert e3 at end
    n = leafCount(&data);
    const e3_entries = leafEntriesMut(&data, n + 1);
    e3_entries[2] = e3;
    leafSetCount(&data, 3);

    try std.testing.expect(searchLeaf(&data, 100) != null);
    try std.testing.expect(searchLeaf(&data, 50)  != null);
    try std.testing.expect(searchLeaf(&data, 200) != null);
    try std.testing.expect(searchLeaf(&data, 999) == null);
    try std.testing.expectEqual(@as(u64, 1), searchLeaf(&data, 100).?.doc_id);
}
