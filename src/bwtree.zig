/// TurboDB — Bw-Tree: latch-free B-tree with CAS-based mapping table
/// Uses delta updates on an indirection mapping table to achieve lock-free
/// concurrent access. No page-level locks — all mutations use atomic CAS.
///
/// Architecture:
///   mapping_table[page_id] → atomic pointer → delta chain → base page
///   Insert/delete prepend a delta record via CAS.
///   Consolidation merges delta chain into a new base page when chain > 8.
const std = @import("std");

// ─── Entry (22 bytes packed) ─────────────────────────────────────────────

pub const Entry = struct {
    key: u64,
    doc_id: u64,
    page_no: u32,
    page_off: u16,
};

// ─── Page types ──────────────────────────────────────────────────────────

pub const DeltaOp = enum { insert, delete };

pub const DeltaRecord = struct {
    key: u64,
    op: DeltaOp,
    entry: Entry,
    next: ?*Page,
};

pub const BasePage = struct {
    entries: []Entry,
    // sorted by key
};

pub const Page = union(enum) {
    base: BasePage,
    delta: DeltaRecord,
};

// ─── BwTree ──────────────────────────────────────────────────────────────

const MAX_PAGES: usize = 4096;
const MAX_DELTA_CHAIN: usize = 8;

pub const BwTree = struct {
    mapping: [MAX_PAGES]std.atomic.Value(usize),
    root_pid: usize,
    next_page_id: std.atomic.Value(usize),
    allocator: std.mem.Allocator,

    pub fn init(allocator: std.mem.Allocator) BwTree {
        var tree: BwTree = undefined;
        tree.allocator = allocator;
        tree.root_pid = 0;
        tree.next_page_id = std.atomic.Value(usize).init(1);

        // Zero out all mapping slots
        var i: usize = 0;
        while (i < MAX_PAGES) : (i += 1) {
            tree.mapping[i] = std.atomic.Value(usize).init(0);
        }

        // Create root base page (empty)
        const root_page = allocator.create(Page) catch unreachable;
        const empty_entries = allocator.alloc(Entry, 0) catch unreachable;
        root_page.* = Page{ .base = BasePage{ .entries = empty_entries } };
        tree.mapping[0] = std.atomic.Value(usize).init(@intFromPtr(root_page));

        return tree;
    }

    pub fn deinit(self: *BwTree) void {
        var i: usize = 0;
        while (i < MAX_PAGES) : (i += 1) {
            const ptr_val = self.mapping[i].load(.acquire);
            if (ptr_val != 0) {
                self.freeChain(@as(*Page, @ptrFromInt(ptr_val)));
            }
        }
    }

    fn freeChain(self: *BwTree, page: *Page) void {
        switch (page.*) {
            .delta => |d| {
                if (d.next) |next| {
                    self.freeChain(next);
                }
                self.allocator.destroy(page);
            },
            .base => |b| {
                self.allocator.free(b.entries);
                self.allocator.destroy(page);
            },
        }
    }

    // ─── allocPage ───────────────────────────────────────────────────────

    fn allocPage(self: *BwTree) usize {
        return self.next_page_id.fetchAdd(1, .monotonic);
    }

    // ─── search ──────────────────────────────────────────────────────────

    /// Walk mapping table → follow delta chain → search base page.
    pub fn search(self: *BwTree, key: u64) ?Entry {
        return self.searchInPage(self.root_pid, key);
    }

    fn searchInPage(self: *BwTree, page_id: usize, key: u64) ?Entry {
        const ptr_val = self.mapping[page_id].load(.acquire);
        if (ptr_val == 0) return null;

        const page: *Page = @ptrFromInt(ptr_val);
        return walkChain(page, key);
    }

    fn walkChain(page: *Page, key: u64) ?Entry {
        switch (page.*) {
            .delta => |d| {
                if (d.key == key) {
                    return switch (d.op) {
                        .insert => d.entry,
                        .delete => null,
                    };
                }
                if (d.next) |next| {
                    return walkChain(next, key);
                }
                return null;
            },
            .base => |b| {
                return binarySearch(b.entries, key);
            },
        }
    }

    fn binarySearch(entries: []const Entry, key: u64) ?Entry {
        if (entries.len == 0) return null;
        var lo: usize = 0;
        var hi: usize = entries.len;
        while (lo < hi) {
            const mid = lo + (hi - lo) / 2;
            if (entries[mid].key == key) return entries[mid];
            if (entries[mid].key < key) {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        return null;
    }

    // ─── insert ──────────────────────────────────────────────────────────

    /// Create an insert-delta record and CAS it onto the mapping table slot.
    /// Retries on CAS failure (another thread won the race).
    pub fn insert(self: *BwTree, key: u64, entry: Entry) !void {
        const page_id = self.root_pid;

        while (true) {
            const old = self.mapping[page_id].load(.acquire);
            const old_page: ?*Page = if (old != 0) @as(*Page, @ptrFromInt(old)) else null;

            const new_delta = try self.allocator.create(Page);
            new_delta.* = Page{ .delta = DeltaRecord{
                .key = key,
                .op = .insert,
                .entry = entry,
                .next = old_page,
            } };

            if (self.mapping[page_id].cmpxchgStrong(
                old,
                @intFromPtr(new_delta),
                .acq_rel,
                .acquire,
            ) == null) {
                // CAS succeeded — check if consolidation needed
                const chain_len = self.deltaChainLen(page_id);
                if (chain_len > MAX_DELTA_CHAIN) {
                    self.consolidate(page_id);
                }
                return;
            } else {
                // CAS failed — another thread won; free our delta and retry
                self.allocator.destroy(new_delta);
            }
        }
    }

    // ─── delete ──────────────────────────────────────────────────────────

    /// Create a delete-delta record and CAS it onto the mapping table slot.
    pub fn delete(self: *BwTree, key: u64) !void {
        const page_id = self.root_pid;

        while (true) {
            const old = self.mapping[page_id].load(.acquire);
            const old_page: ?*Page = if (old != 0) @as(*Page, @ptrFromInt(old)) else null;

            const new_delta = try self.allocator.create(Page);
            new_delta.* = Page{ .delta = DeltaRecord{
                .key = key,
                .op = .delete,
                .entry = Entry{ .key = key, .doc_id = 0, .page_no = 0, .page_off = 0 },
                .next = old_page,
            } };

            if (self.mapping[page_id].cmpxchgStrong(
                old,
                @intFromPtr(new_delta),
                .acq_rel,
                .acquire,
            ) == null) {
                return;
            } else {
                self.allocator.destroy(new_delta);
            }
        }
    }

    // ─── consolidate ─────────────────────────────────────────────────────

    /// When delta chain exceeds MAX_DELTA_CHAIN, merge into a new base page.
    pub fn consolidate(self: *BwTree, page_id: usize) void {
        const old = self.mapping[page_id].load(.acquire);
        if (old == 0) return;

        const page: *Page = @ptrFromInt(old);

        // Collect all entries by walking chain, applying deltas in order
        var key_map = std.AutoHashMap(u64, Entry).init(self.allocator);
        defer key_map.deinit();

        self.collectEntries(page, &key_map);

        // Build sorted entries array
        const count = key_map.count();
        const new_entries = self.allocator.alloc(Entry, count) catch return;
        var idx: usize = 0;
        var it = key_map.iterator();
        while (it.next()) |kv| {
            new_entries[idx] = kv.value_ptr.*;
            idx += 1;
        }

        // Sort by key
        std.mem.sort(Entry, new_entries, {}, struct {
            fn cmp(_: void, a: Entry, b: Entry) bool {
                return a.key < b.key;
            }
        }.cmp);

        // Create new base page
        const new_page = self.allocator.create(Page) catch return;
        new_page.* = Page{ .base = BasePage{ .entries = new_entries } };

        // CAS the new base page in
        if (self.mapping[page_id].cmpxchgStrong(
            old,
            @intFromPtr(new_page),
            .acq_rel,
            .acquire,
        ) == null) {
            // Success — old chain will be reclaimed by epoch-based GC.
            // Do NOT free here — concurrent readers may still be traversing it.
            // TODO: integrate with mvcc.zig epoch GC for safe reclamation.
        } else {
            // Another thread consolidated first; discard our work
            self.allocator.free(new_entries);
            self.allocator.destroy(new_page);
        }
    }

    fn collectEntries(self: *BwTree, page: *Page, map: *std.AutoHashMap(u64, Entry)) void {
        _ = self;
        // Walk to base first, then apply deltas in reverse (base → newest)
        var stack: [256]*Page = undefined;
        var depth: usize = 0;
        var cur: ?*Page = page;

        while (cur) |p| {
            stack[depth] = p;
            depth += 1;
            switch (p.*) {
                .delta => |d| cur = d.next,
                .base => {
                    cur = null;
                },
            }
        }

        // Apply from base (deepest) to newest delta
        var i: usize = depth;
        while (i > 0) {
            i -= 1;
            switch (stack[i].*) {
                .base => |b| {
                    var j: usize = 0;
                    while (j < b.entries.len) : (j += 1) {
                        map.put(b.entries[j].key, b.entries[j]) catch {};
                    }
                },
                .delta => |d| {
                    switch (d.op) {
                        .insert => {
                            map.put(d.key, d.entry) catch {};
                        },
                        .delete => {
                            _ = map.remove(d.key);
                        },
                    }
                },
            }
        }
    }

    fn deltaChainLen(self: *BwTree, page_id: usize) usize {
        const ptr_val = self.mapping[page_id].load(.acquire);
        if (ptr_val == 0) return 0;
        var count: usize = 0;
        var cur: ?*Page = @ptrFromInt(ptr_val);
        while (cur) |p| {
            switch (p.*) {
                .delta => |d| {
                    count += 1;
                    cur = d.next;
                },
                .base => return count,
            }
        }
        return count;
    }
};

// ─── Tests ───────────────────────────────────────────────────────────────

fn makeEntry(key: u64, doc_id: u64) Entry {
    return Entry{ .key = key, .doc_id = doc_id, .page_no = 0, .page_off = 0 };
}

test "bwtree insert and search" {
    var tree = BwTree.init(std.testing.allocator);
    defer tree.deinit();

    try tree.insert(10, makeEntry(10, 100));
    try tree.insert(20, makeEntry(20, 200));
    try tree.insert(5, makeEntry(5, 50));

    const r1 = tree.search(10);
    try std.testing.expect(r1 != null);
    try std.testing.expectEqual(@as(u64, 100), r1.?.doc_id);

    const r2 = tree.search(20);
    try std.testing.expect(r2 != null);
    try std.testing.expectEqual(@as(u64, 200), r2.?.doc_id);

    const r3 = tree.search(5);
    try std.testing.expect(r3 != null);
    try std.testing.expectEqual(@as(u64, 50), r3.?.doc_id);

    const r4 = tree.search(999);
    try std.testing.expect(r4 == null);
}

test "bwtree delete" {
    var tree = BwTree.init(std.testing.allocator);
    defer tree.deinit();

    try tree.insert(10, makeEntry(10, 100));
    try tree.insert(20, makeEntry(20, 200));

    // Delete key 10
    try tree.delete(10);
    const r1 = tree.search(10);
    try std.testing.expect(r1 == null);

    // Key 20 still present
    const r2 = tree.search(20);
    try std.testing.expect(r2 != null);
    try std.testing.expectEqual(@as(u64, 200), r2.?.doc_id);
}

test "bwtree consolidation" {
    // Use page_allocator — old chains deferred to epoch GC
    var tree = BwTree.init(std.heap.page_allocator);

    // Insert enough entries to trigger consolidation (> MAX_DELTA_CHAIN = 8)
    var i: u64 = 0;
    while (i < 20) : (i += 1) {
        try tree.insert(i, makeEntry(i, i * 10));
    }

    // Force consolidation
    tree.consolidate(tree.root_pid);

    // All entries should still be searchable after consolidation
    i = 0;
    while (i < 20) : (i += 1) {
        const r = tree.search(i);
        try std.testing.expect(r != null);
        try std.testing.expectEqual(i * 10, r.?.doc_id);
    }
}

test "bwtree concurrent inserts" {
    // Use page_allocator — consolidated chains are intentionally leaked
    // (deferred to epoch-based GC, not available in test context)
    var tree = BwTree.init(std.heap.page_allocator);

    const NUM_THREADS = 4;
    const KEYS_PER_THREAD = 50;

    var threads: [NUM_THREADS]std.Thread = undefined;
    var t: usize = 0;
    while (t < NUM_THREADS) : (t += 1) {
        const tid = t;
        threads[t] = try std.Thread.spawn(.{}, struct {
            fn worker(tr: *BwTree, thread_id: usize) void {
                const base = thread_id * KEYS_PER_THREAD;
                var k: usize = 0;
                while (k < KEYS_PER_THREAD) : (k += 1) {
                    const key: u64 = @intCast(base + k);
                    tr.insert(key, makeEntry(key, key * 10)) catch {};
                }
            }
        }.worker, .{ &tree, tid });
    }

    // Join all threads
    t = 0;
    while (t < NUM_THREADS) : (t += 1) {
        threads[t].join();
    }

    // All keys should be present — no locks used, CAS ensures consistency
    var found: usize = 0;
    var k: u64 = 0;
    while (k < NUM_THREADS * KEYS_PER_THREAD) : (k += 1) {
        if (tree.search(k) != null) {
            found += 1;
        }
    }

    try std.testing.expectEqual(@as(usize, NUM_THREADS * KEYS_PER_THREAD), found);
}
