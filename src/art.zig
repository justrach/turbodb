const std = @import("std");
const btree = @import("btree.zig");
pub const BTreeEntry = btree.BTreeEntry;

// ─── Adaptive Radix Tree (Leis et al., ICDE 2013) ────────────────────────────
//
// ART with path compression and lazy expansion for secondary indexes.
// Supports optimistic lock coupling for concurrent readers.

const MAX_PREFIX_LEN: usize = 8;

const NodeType = enum(u8) {
    node4 = 0,
    node16 = 1,
    node48 = 2,
    node256 = 3,
    leaf = 4,
};

// ─── Node header (shared by all inner node types) ────────────────────────────

const NodeHeader = struct {
    node_type: NodeType,
    num_children: u8,
    prefix_len: u32,
    prefix: [MAX_PREFIX_LEN]u8,
    version: std.atomic.Value(u64),

    fn init(node_type: NodeType) NodeHeader {
        return .{
            .node_type = node_type,
            .num_children = 0,
            .prefix_len = 0,
            .prefix = [_]u8{0} ** MAX_PREFIX_LEN,
            .version = std.atomic.Value(u64).init(0),
        };
    }

    fn readLockVersion(self: *const NodeHeader) ?u64 {
        const v = self.version.load(.acquire);
        // Odd version means write in progress
        if (v & 1 != 0) return null;
        return v;
    }

    fn checkVersion(self: *const NodeHeader, expected: u64) bool {
        return self.version.load(.acquire) == expected;
    }

    fn writeLock(self: *NodeHeader) void {
        while (true) {
            const v = self.version.load(.acquire);
            if (v & 1 != 0) {
                std.atomic.spinLoopHint();
                continue;
            }
            if (self.version.cmpxchgWeak(v, v + 1, .acq_rel, .acquire)) |_| {
                std.atomic.spinLoopHint();
                continue;
            }
            return;
        }
    }

    fn writeUnlock(self: *NodeHeader) void {
        _ = self.version.fetchAdd(1, .release);
    }
};

// ─── Node union pointer ──────────────────────────────────────────────────────

const Node = union(NodeType) {
    node4: *Node4,
    node16: *Node16,
    node48: *Node48,
    node256: *Node256,
    leaf: *Leaf,

    fn header(self: Node) *NodeHeader {
        return switch (self) {
            .node4 => |n| &n.header,
            .node16 => |n| &n.header,
            .node48 => |n| &n.header,
            .node256 => |n| &n.header,
            .leaf => unreachable,
        };
    }

    fn isLeaf(self: Node) bool {
        return self == .leaf;
    }
};

// ─── Leaf ────────────────────────────────────────────────────────────────────

const Leaf = struct {
    key: []const u8,
    key_buf: ?[]u8, // owned copy
    entry: BTreeEntry,

    fn create(alloc: std.mem.Allocator, key: []const u8, entry: BTreeEntry) !*Leaf {
        const leaf = try alloc.create(Leaf);
        const buf = try alloc.alloc(u8, key.len);
        @memcpy(buf, key);
        leaf.* = .{
            .key = buf,
            .key_buf = buf,
            .entry = entry,
        };
        return leaf;
    }

    fn destroy(self: *Leaf, alloc: std.mem.Allocator) void {
        if (self.key_buf) |buf| alloc.free(buf);
        alloc.destroy(self);
    }

    fn matches(self: *const Leaf, key: []const u8) bool {
        return std.mem.eql(u8, self.key, key);
    }

    fn prefixMatches(self: *const Leaf, prefix: []const u8) bool {
        if (self.key.len < prefix.len) return false;
        return std.mem.eql(u8, self.key[0..prefix.len], prefix);
    }
};

// ─── Node4 ───────────────────────────────────────────────────────────────────

const Node4 = struct {
    header: NodeHeader,
    keys: [4]u8,
    children: [4]Node,

    fn create(alloc: std.mem.Allocator) !*Node4 {
        const n = try alloc.create(Node4);
        n.* = .{
            .header = NodeHeader.init(.node4),
            .keys = [_]u8{0} ** 4,
            .children = undefined,
        };
        return n;
    }

    fn destroy(self: *Node4, alloc: std.mem.Allocator) void {
        alloc.destroy(self);
    }

    fn findChild(self: *const Node4, byte: u8) ?Node {
        for (0..self.header.num_children) |i| {
            if (self.keys[i] == byte) return self.children[i];
        }
        return null;
    }

    fn addChild(self: *Node4, byte: u8, child: Node) void {
        var pos: usize = self.header.num_children;
        // Insert sorted
        while (pos > 0 and self.keys[pos - 1] > byte) {
            self.keys[pos] = self.keys[pos - 1];
            self.children[pos] = self.children[pos - 1];
            pos -= 1;
        }
        self.keys[pos] = byte;
        self.children[pos] = child;
        self.header.num_children += 1;
    }

    fn removeChild(self: *Node4, byte: u8) void {
        for (0..self.header.num_children) |i| {
            if (self.keys[i] == byte) {
                const nc = self.header.num_children;
                // Shift left
                var j = i;
                while (j + 1 < nc) : (j += 1) {
                    self.keys[j] = self.keys[j + 1];
                    self.children[j] = self.children[j + 1];
                }
                self.header.num_children -= 1;
                return;
            }
        }
    }

    fn isFull(self: *const Node4) bool {
        return self.header.num_children >= 4;
    }
};

// ─── Node16 ──────────────────────────────────────────────────────────────────

const Node16 = struct {
    header: NodeHeader,
    keys: [16]u8,
    children: [16]Node,

    fn create(alloc: std.mem.Allocator) !*Node16 {
        const n = try alloc.create(Node16);
        n.* = .{
            .header = NodeHeader.init(.node16),
            .keys = [_]u8{0} ** 16,
            .children = undefined,
        };
        return n;
    }

    fn destroy(self: *Node16, alloc: std.mem.Allocator) void {
        alloc.destroy(self);
    }

    fn findChild(self: *const Node16, byte: u8) ?Node {
        for (0..self.header.num_children) |i| {
            if (self.keys[i] == byte) return self.children[i];
        }
        return null;
    }

    fn addChild(self: *Node16, byte: u8, child: Node) void {
        var pos: usize = self.header.num_children;
        while (pos > 0 and self.keys[pos - 1] > byte) {
            self.keys[pos] = self.keys[pos - 1];
            self.children[pos] = self.children[pos - 1];
            pos -= 1;
        }
        self.keys[pos] = byte;
        self.children[pos] = child;
        self.header.num_children += 1;
    }

    fn removeChild(self: *Node16, byte: u8) void {
        for (0..self.header.num_children) |i| {
            if (self.keys[i] == byte) {
                const nc = self.header.num_children;
                var j = i;
                while (j + 1 < nc) : (j += 1) {
                    self.keys[j] = self.keys[j + 1];
                    self.children[j] = self.children[j + 1];
                }
                self.header.num_children -= 1;
                return;
            }
        }
    }

    fn isFull(self: *const Node16) bool {
        return self.header.num_children >= 16;
    }
};

// ─── Node48 ──────────────────────────────────────────────────────────────────

const EMPTY_SLOT: u8 = 0xFF;

const Node48 = struct {
    header: NodeHeader,
    index: [256]u8, // key byte -> child slot (0xFF = empty)
    children: [48]Node,

    fn create(alloc: std.mem.Allocator) !*Node48 {
        const n = try alloc.create(Node48);
        n.* = .{
            .header = NodeHeader.init(.node48),
            .index = [_]u8{EMPTY_SLOT} ** 256,
            .children = undefined,
        };
        return n;
    }

    fn destroy(self: *Node48, alloc: std.mem.Allocator) void {
        alloc.destroy(self);
    }

    fn findChild(self: *const Node48, byte: u8) ?Node {
        const slot = self.index[byte];
        if (slot == EMPTY_SLOT) return null;
        return self.children[slot];
    }

    fn addChild(self: *Node48, byte: u8, child: Node) void {
        const pos = self.header.num_children;
        self.index[byte] = @intCast(pos);
        self.children[pos] = child;
        self.header.num_children += 1;
    }

    fn removeChild(self: *Node48, byte: u8) void {
        const slot = self.index[byte];
        if (slot == EMPTY_SLOT) return;

        self.index[byte] = EMPTY_SLOT;
        const last = self.header.num_children - 1;

        if (slot != last) {
            // Move last child into the vacated slot
            self.children[slot] = self.children[last];
            // Update the index entry that pointed to 'last'
            for (&self.index) |*idx| {
                if (idx.* == @as(u8, @intCast(last))) {
                    idx.* = slot;
                    break;
                }
            }
        }
        self.header.num_children -= 1;
    }

    fn isFull(self: *const Node48) bool {
        return self.header.num_children >= 48;
    }
};

// ─── Node256 ─────────────────────────────────────────────────────────────────

const Node256 = struct {
    header: NodeHeader,
    children: [256]?Node,

    fn create(alloc: std.mem.Allocator) !*Node256 {
        const n = try alloc.create(Node256);
        n.* = .{
            .header = NodeHeader.init(.node256),
            .children = [_]?Node{null} ** 256,
        };
        return n;
    }

    fn destroy(self: *Node256, alloc: std.mem.Allocator) void {
        alloc.destroy(self);
    }

    fn findChild(self: *const Node256, byte: u8) ?Node {
        return self.children[byte];
    }

    fn addChild(self: *Node256, byte: u8, child: Node) void {
        self.children[byte] = child;
        self.header.num_children += 1;
    }

    fn removeChild(self: *Node256, byte: u8) void {
        if (self.children[byte] != null) {
            self.children[byte] = null;
            self.header.num_children -= 1;
        }
    }
};

// ─── ART public interface ────────────────────────────────────────────────────

pub const ART = struct {
    root: ?Node,
    size: usize,
    alloc: std.mem.Allocator,

    pub fn init(alloc: std.mem.Allocator) ART {
        return .{
            .root = null,
            .size = 0,
            .alloc = alloc,
        };
    }

    pub fn deinit(self: *ART) void {
        if (self.root) |root| {
            destroyNode(self.alloc, root);
        }
        self.root = null;
        self.size = 0;
    }

    pub fn count(self: *ART) usize {
        return self.size;
    }

    // ─── search ──────────────────────────────────────────────────────────

    pub fn search(self: *ART, key: []const u8) ?BTreeEntry {
        var node_opt = self.root;
        var depth: usize = 0;

        while (node_opt) |node| {
            if (node.isLeaf()) {
                const leaf = node.leaf;
                if (leaf.matches(key)) return leaf.entry;
                return null;
            }

            const hdr = node.header();

            // Optimistic read: grab version
            const ver = hdr.readLockVersion() orelse continue;

            // Check prefix
            const prefix_len = hdr.prefix_len;
            if (prefix_len > 0) {
                const check_len = @min(prefix_len, MAX_PREFIX_LEN);
                const remaining = key.len -| depth;
                if (remaining < prefix_len) return null;
                for (0..check_len) |i| {
                    if (key[depth + i] != hdr.prefix[i]) return null;
                }
                depth += prefix_len;
            }

            // Validate version hasn't changed
            if (!hdr.checkVersion(ver)) continue;

            if (depth >= key.len) return null;

            const next = findChildInNode(node, key[depth]);
            depth += 1;
            node_opt = next;
        }
        return null;
    }

    // ─── insert ──────────────────────────────────────────────────────────

    pub fn insert(self: *ART, key: []const u8, entry: BTreeEntry) !void {
        if (self.root == null) {
            const leaf = try Leaf.create(self.alloc, key, entry);
            self.root = Node{ .leaf = leaf };
            self.size = 1;
            return;
        }
        const replaced = try self.insertRecursive(&self.root.?, key, entry, 0);
        if (!replaced) self.size += 1;
    }

    fn insertRecursive(self: *ART, node_ref: *Node, key: []const u8, entry: BTreeEntry, depth_in: usize) !bool {
        var depth = depth_in;
        const node = node_ref.*;

        // ── leaf node ────────────────────────────────────────────────────
        if (node.isLeaf()) {
            const leaf = node.leaf;
            if (leaf.matches(key)) {
                // Update existing
                leaf.entry = entry;
                return true;
            }
            // Split: create a new Node4 with shared prefix
            const new_node = try Node4.create(self.alloc);
            const existing_key = leaf.key;

            // Determine shared prefix length
            var prefix_len: usize = 0;
            while (depth + prefix_len < existing_key.len and
                depth + prefix_len < key.len and
                existing_key[depth + prefix_len] == key[depth + prefix_len])
            {
                if (prefix_len < MAX_PREFIX_LEN) {
                    new_node.header.prefix[prefix_len] = key[depth + prefix_len];
                }
                prefix_len += 1;
            }
            new_node.header.prefix_len = @intCast(prefix_len);

            // Add existing leaf
            if (depth + prefix_len < existing_key.len) {
                new_node.addChild(existing_key[depth + prefix_len], node);
            } else {
                new_node.addChild(0, node);
            }

            // Add new leaf
            const new_leaf = try Leaf.create(self.alloc, key, entry);
            if (depth + prefix_len < key.len) {
                new_node.addChild(key[depth + prefix_len], Node{ .leaf = new_leaf });
            } else {
                new_node.addChild(0, Node{ .leaf = new_leaf });
            }

            node_ref.* = Node{ .node4 = new_node };
            return false;
        }

        // ── inner node ───────────────────────────────────────────────────
        const hdr = node.header();
        hdr.writeLock();

        // Check prefix mismatch
        const prefix_len = hdr.prefix_len;
        if (prefix_len > 0) {
            var mismatch: usize = 0;
            const check_len = @min(prefix_len, MAX_PREFIX_LEN);
            while (mismatch < check_len) : (mismatch += 1) {
                if (depth + mismatch >= key.len or hdr.prefix[mismatch] != key[depth + mismatch])
                    break;
            }

            if (mismatch < prefix_len) {
                // Prefix mismatch: split the node
                hdr.writeUnlock();
                const new_node = try Node4.create(self.alloc);

                // New node's prefix is the common part
                new_node.header.prefix_len = @intCast(mismatch);
                for (0..@min(mismatch, MAX_PREFIX_LEN)) |i| {
                    new_node.header.prefix[i] = hdr.prefix[i];
                }

                // Add existing node as child under the diverging byte
                if (mismatch < MAX_PREFIX_LEN) {
                    new_node.addChild(hdr.prefix[mismatch], node);
                } else {
                    new_node.addChild(0, node);
                }

                // Adjust existing node's prefix (remove consumed part)
                hdr.writeLock();
                const remaining = prefix_len - mismatch - 1;
                if (remaining > 0 and mismatch + 1 < MAX_PREFIX_LEN) {
                    var dst: [MAX_PREFIX_LEN]u8 = [_]u8{0} ** MAX_PREFIX_LEN;
                    const copy_len = @min(remaining, MAX_PREFIX_LEN);
                    const src_start = @min(mismatch + 1, MAX_PREFIX_LEN);
                    for (0..copy_len) |i| {
                        if (src_start + i < MAX_PREFIX_LEN)
                            dst[i] = hdr.prefix[src_start + i];
                    }
                    hdr.prefix = dst;
                }
                hdr.prefix_len = @intCast(remaining);
                hdr.writeUnlock();

                // Add new leaf
                const new_leaf = try Leaf.create(self.alloc, key, entry);
                if (depth + mismatch < key.len) {
                    new_node.addChild(key[depth + mismatch], Node{ .leaf = new_leaf });
                } else {
                    new_node.addChild(0, Node{ .leaf = new_leaf });
                }

                node_ref.* = Node{ .node4 = new_node };
                return false;
            }

            depth += prefix_len;
        }

        hdr.writeUnlock();

        if (depth >= key.len) {
            // Key exhausted at inner node; store as child with byte 0
            const child_opt = findChildInNode(node, 0);
            if (child_opt) |_| {
                const child_ref = findChildRefInNode(node, 0).?;
                return self.insertRecursive(child_ref, key, entry, depth);
            }
            const new_leaf = try Leaf.create(self.alloc, key, entry);
            try self.addChildToNode(node_ref, node, 0, Node{ .leaf = new_leaf });
            return false;
        }

        const byte = key[depth];
        const child_opt = findChildInNode(node, byte);
        if (child_opt) |_| {
            const child_ref = findChildRefInNode(node, byte).?;
            return self.insertRecursive(child_ref, key, entry, depth + 1);
        }

        // No child for this byte: insert a new leaf
        const new_leaf = try Leaf.create(self.alloc, key, entry);
        try self.addChildToNode(node_ref, node, byte, Node{ .leaf = new_leaf });
        return false;
    }

    // ─── delete ──────────────────────────────────────────────────────────

    pub fn delete(self: *ART, key: []const u8) !bool {
        if (self.root == null) return false;
        const removed = try self.deleteRecursive(&self.root.?, key, 0);
        if (removed) self.size -= 1;
        return removed;
    }

    fn deleteRecursive(self: *ART, node_ref: *Node, key: []const u8, depth_in: usize) !bool {
        var depth = depth_in;
        const node = node_ref.*;

        if (node.isLeaf()) {
            if (node.leaf.matches(key)) {
                node.leaf.destroy(self.alloc);
                node_ref.* = undefined;
                return true;
            }
            return false;
        }

        const hdr = node.header();
        const prefix_len = hdr.prefix_len;
        if (prefix_len > 0) {
            const check_len = @min(prefix_len, MAX_PREFIX_LEN);
            for (0..check_len) |i| {
                if (depth + i >= key.len or hdr.prefix[i] != key[depth + i])
                    return false;
            }
            depth += prefix_len;
        }

        if (depth >= key.len) return false;

        const byte = key[depth];
        const child_ref = findChildRefInNode(node, byte) orelse return false;
        const child = child_ref.*;

        if (child.isLeaf()) {
            if (child.leaf.matches(key)) {
                child.leaf.destroy(self.alloc);
                // Remove child from this node
                hdr.writeLock();
                removeChildFromNode(node, byte);
                hdr.writeUnlock();

                // Shrink if needed
                try self.maybeShrink(node_ref);
                return true;
            }
            return false;
        }

        const removed = try self.deleteRecursive(child_ref, key, depth + 1);
        if (removed) {
            try self.maybeShrink(child_ref);
        }
        return removed;
    }

    fn maybeShrink(self: *ART, node_ref: *Node) !void {
        const node = node_ref.*;
        if (node.isLeaf()) return;

        const hdr = node.header();
        const nc = hdr.num_children;

        switch (node) {
            .node256 => |n256| {
                if (nc <= 48) {
                    const n48 = try Node48.create(self.alloc);
                    n48.header.prefix_len = hdr.prefix_len;
                    n48.header.prefix = hdr.prefix;
                    for (0..256) |i| {
                        if (n256.children[i]) |child| {
                            n48.addChild(@intCast(i), child);
                        }
                    }
                    node_ref.* = Node{ .node48 = n48 };
                    n256.destroy(self.alloc);
                }
            },
            .node48 => |n48| {
                if (nc <= 16) {
                    const n16 = try Node16.create(self.alloc);
                    n16.header.prefix_len = hdr.prefix_len;
                    n16.header.prefix = hdr.prefix;
                    for (0..256) |i| {
                        if (n48.index[i] != EMPTY_SLOT) {
                            n16.addChild(@intCast(i), n48.children[n48.index[i]]);
                        }
                    }
                    node_ref.* = Node{ .node16 = n16 };
                    n48.destroy(self.alloc);
                }
            },
            .node16 => |n16| {
                if (nc <= 4) {
                    const n4 = try Node4.create(self.alloc);
                    n4.header.prefix_len = hdr.prefix_len;
                    n4.header.prefix = hdr.prefix;
                    for (0..nc) |i| {
                        n4.addChild(n16.keys[i], n16.children[i]);
                    }
                    node_ref.* = Node{ .node4 = n4 };
                    n16.destroy(self.alloc);
                }
            },
            .node4 => |n4| {
                if (nc == 1) {
                    // Path compression: collapse single-child node
                    const child = n4.children[0];
                    if (child.isLeaf()) {
                        node_ref.* = child;
                        n4.destroy(self.alloc);
                    } else {
                        // Merge prefixes
                        const child_hdr = child.header();
                        const old_prefix_len = hdr.prefix_len;
                        const new_prefix_len = old_prefix_len + 1 + child_hdr.prefix_len;
                        var new_prefix: [MAX_PREFIX_LEN]u8 = [_]u8{0} ** MAX_PREFIX_LEN;

                        const p1 = @min(old_prefix_len, MAX_PREFIX_LEN);
                        for (0..p1) |i| {
                            new_prefix[i] = hdr.prefix[i];
                        }
                        if (old_prefix_len < MAX_PREFIX_LEN) {
                            new_prefix[old_prefix_len] = n4.keys[0];
                        }
                        const start = old_prefix_len + 1;
                        const p2 = @min(child_hdr.prefix_len, MAX_PREFIX_LEN);
                        for (0..p2) |i| {
                            if (start + i < MAX_PREFIX_LEN)
                                new_prefix[start + i] = child_hdr.prefix[i];
                        }

                        child_hdr.prefix = new_prefix;
                        child_hdr.prefix_len = @intCast(new_prefix_len);

                        node_ref.* = child;
                        n4.destroy(self.alloc);
                    }
                }
            },
            .leaf => {},
        }
    }

    // ─── prefix scan ─────────────────────────────────────────────────────

    pub fn prefixScan(self: *ART, prefix: []const u8, result_alloc: std.mem.Allocator) ![]BTreeEntry {
        var results: std.ArrayList(BTreeEntry) = .empty;
        errdefer results.deinit(result_alloc);

        if (self.root) |root| {
            try collectPrefixImpl(root, prefix, 0, &results, result_alloc);
        }
        return results.toOwnedSlice(result_alloc);
    }

    fn collectPrefixImpl(node: Node, prefix: []const u8, depth_in: usize, results: *std.ArrayList(BTreeEntry), alloc: std.mem.Allocator) !void {
        var depth = depth_in;

        if (node.isLeaf()) {
            if (node.leaf.prefixMatches(prefix)) {
                try results.append(alloc, node.leaf.entry);
            }
            return;
        }

        const hdr = node.header();
        const prefix_len = hdr.prefix_len;

        // Check node prefix against search prefix
        if (prefix_len > 0) {
            const check_len = @min(prefix_len, MAX_PREFIX_LEN);
            for (0..check_len) |i| {
                if (depth + i >= prefix.len) {
                    // Search prefix exhausted within node prefix: collect all
                    try collectAllImpl(node, results, alloc);
                    return;
                }
                if (hdr.prefix[i] != prefix[depth + i]) return;
            }
            depth += prefix_len;
        }

        if (depth >= prefix.len) {
            try collectAllImpl(node, results, alloc);
            return;
        }

        // Continue down the matching child
        const byte = prefix[depth];
        const child_opt = findChildInNode(node, byte);
        if (child_opt) |child| {
            try collectPrefixImpl(child, prefix, depth + 1, results, alloc);
        }
    }

    fn collectAllImpl(node: Node, results: *std.ArrayList(BTreeEntry), alloc: std.mem.Allocator) !void {
        if (node.isLeaf()) {
            try results.append(alloc, node.leaf.entry);
            return;
        }

        switch (node) {
            .node4 => |n4| {
                for (0..n4.header.num_children) |i| {
                    try collectAllImpl(n4.children[i], results, alloc);
                }
            },
            .node16 => |n16| {
                for (0..n16.header.num_children) |i| {
                    try collectAllImpl(n16.children[i], results, alloc);
                }
            },
            .node48 => |n48| {
                for (0..256) |i| {
                    if (n48.index[i] != EMPTY_SLOT) {
                        try collectAllImpl(n48.children[n48.index[i]], results, alloc);
                    }
                }
            },
            .node256 => |n256| {
                for (0..256) |i| {
                    if (n256.children[i]) |child| {
                        try collectAllImpl(child, results, alloc);
                    }
                }
            },
            .leaf => unreachable, // handled above
        }
    }

    // ─── helpers ─────────────────────────────────────────────────────────

    fn findChildInNode(node: Node, byte: u8) ?Node {
        return switch (node) {
            .node4 => |n| n.findChild(byte),
            .node16 => |n| n.findChild(byte),
            .node48 => |n| n.findChild(byte),
            .node256 => |n| n.findChild(byte),
            .leaf => null,
        };
    }

    fn findChildRefInNode(node: Node, byte: u8) ?*Node {
        switch (node) {
            .node4 => |n| {
                for (0..n.header.num_children) |i| {
                    if (n.keys[i] == byte) return &n.children[i];
                }
            },
            .node16 => |n| {
                for (0..n.header.num_children) |i| {
                    if (n.keys[i] == byte) return &n.children[i];
                }
            },
            .node48 => |n| {
                const slot = n.index[byte];
                if (slot != EMPTY_SLOT) return &n.children[slot];
            },
            .node256 => |n| {
                if (n.children[byte] != null) return @constCast(&n.children[byte].?);
                return null;
            },
            .leaf => {},
        }
        return null;
    }

    fn removeChildFromNode(node: Node, byte: u8) void {
        switch (node) {
            .node4 => |n| n.removeChild(byte),
            .node16 => |n| n.removeChild(byte),
            .node48 => |n| n.removeChild(byte),
            .node256 => |n| n.removeChild(byte),
            .leaf => {},
        }
    }

    fn addChildToNode(self: *ART, node_ref: *Node, node: Node, byte: u8, child: Node) !void {
        switch (node) {
            .node4 => |n4| {
                if (n4.isFull()) {
                    const n16 = try Node16.create(self.alloc);
                    n16.header.prefix_len = n4.header.prefix_len;
                    n16.header.prefix = n4.header.prefix;
                    for (0..n4.header.num_children) |i| {
                        n16.addChild(n4.keys[i], n4.children[i]);
                    }
                    n16.addChild(byte, child);
                    node_ref.* = Node{ .node16 = n16 };
                    n4.destroy(self.alloc);
                } else {
                    n4.addChild(byte, child);
                }
            },
            .node16 => |n16| {
                if (n16.isFull()) {
                    const n48 = try Node48.create(self.alloc);
                    n48.header.prefix_len = n16.header.prefix_len;
                    n48.header.prefix = n16.header.prefix;
                    for (0..n16.header.num_children) |i| {
                        n48.addChild(n16.keys[i], n16.children[i]);
                    }
                    n48.addChild(byte, child);
                    node_ref.* = Node{ .node48 = n48 };
                    n16.destroy(self.alloc);
                } else {
                    n16.addChild(byte, child);
                }
            },
            .node48 => |n48| {
                if (n48.isFull()) {
                    const n256 = try Node256.create(self.alloc);
                    n256.header.prefix_len = n48.header.prefix_len;
                    n256.header.prefix = n48.header.prefix;
                    for (0..256) |i| {
                        if (n48.index[i] != EMPTY_SLOT) {
                            n256.addChild(@intCast(i), n48.children[n48.index[i]]);
                        }
                    }
                    n256.addChild(byte, child);
                    node_ref.* = Node{ .node256 = n256 };
                    n48.destroy(self.alloc);
                } else {
                    n48.addChild(byte, child);
                }
            },
            .node256 => |n256| {
                n256.addChild(byte, child);
            },
            .leaf => unreachable,
        }
    }

    fn destroyNode(alloc: std.mem.Allocator, node: Node) void {
        switch (node) {
            .leaf => |leaf| leaf.destroy(alloc),
            .node4 => |n4| {
                for (0..n4.header.num_children) |i| {
                    destroyNode(alloc, n4.children[i]);
                }
                n4.destroy(alloc);
            },
            .node16 => |n16| {
                for (0..n16.header.num_children) |i| {
                    destroyNode(alloc, n16.children[i]);
                }
                n16.destroy(alloc);
            },
            .node48 => |n48| {
                for (0..256) |i| {
                    if (n48.index[i] != EMPTY_SLOT) {
                        destroyNode(alloc, n48.children[n48.index[i]]);
                    }
                }
                n48.destroy(alloc);
            },
            .node256 => |n256| {
                for (0..256) |i| {
                    if (n256.children[i]) |child| {
                        destroyNode(alloc, child);
                    }
                }
                n256.destroy(alloc);
            },
        }
    }
};

// ─── Tests ───────────────────────────────────────────────────────────────────

fn makeEntry(doc_id: u64) BTreeEntry {
    return .{
        .key_hash = doc_id *% 0x9E3779B97F4A7C15,
        .doc_id = doc_id,
        .page_no = @intCast(doc_id & 0xFFFF),
        .page_off = 0,
    };
}

test "ART: insert and search single key" {
    var tree = ART.init(std.testing.allocator);
    defer tree.deinit();

    const key = "hello";
    const entry = makeEntry(1);
    try tree.insert(key, entry);

    const result = tree.search(key);
    try std.testing.expect(result != null);
    try std.testing.expectEqual(@as(u64, 1), result.?.doc_id);

    // Key not present
    try std.testing.expect(tree.search("world") == null);
}

test "ART: insert many keys and search all" {
    var tree = ART.init(std.testing.allocator);
    defer tree.deinit();

    const keys = [_][]const u8{
        "alpha", "beta", "gamma", "delta", "epsilon",
        "zeta",  "eta",  "theta", "iota",  "kappa",
        "lambda", "mu",  "nu",    "xi",    "omicron",
        "pi",    "rho",  "sigma", "tau",   "upsilon",
    };

    for (keys, 0..) |k, i| {
        try tree.insert(k, makeEntry(i + 1));
    }

    try std.testing.expectEqual(@as(usize, keys.len), tree.count());

    for (keys, 0..) |k, i| {
        const result = tree.search(k);
        try std.testing.expect(result != null);
        try std.testing.expectEqual(@as(u64, i + 1), result.?.doc_id);
    }
}

test "ART: delete and verify" {
    var tree = ART.init(std.testing.allocator);
    defer tree.deinit();

    try tree.insert("foo", makeEntry(1));
    try tree.insert("bar", makeEntry(2));
    try tree.insert("baz", makeEntry(3));

    try std.testing.expectEqual(@as(usize, 3), tree.count());

    const removed = try tree.delete("bar");
    try std.testing.expect(removed);
    try std.testing.expectEqual(@as(usize, 2), tree.count());
    try std.testing.expect(tree.search("bar") == null);
    try std.testing.expect(tree.search("foo") != null);
    try std.testing.expect(tree.search("baz") != null);

    // Delete non-existent key
    const removed2 = try tree.delete("nonexistent");
    try std.testing.expect(!removed2);
}

test "ART: prefix scan" {
    var tree = ART.init(std.testing.allocator);
    defer tree.deinit();

    try tree.insert("user:1:name", makeEntry(1));
    try tree.insert("user:1:email", makeEntry(2));
    try tree.insert("user:2:name", makeEntry(3));
    try tree.insert("post:1:title", makeEntry(4));

    const results = try tree.prefixScan("user:1:", std.testing.allocator);
    defer std.testing.allocator.free(results);

    try std.testing.expectEqual(@as(usize, 2), results.len);
}

test "ART: path compression" {
    var tree = ART.init(std.testing.allocator);
    defer tree.deinit();

    // Keys sharing long prefixes exercise path compression
    try tree.insert("prefix_shared_a", makeEntry(1));
    try tree.insert("prefix_shared_b", makeEntry(2));

    try std.testing.expect(tree.search("prefix_shared_a") != null);
    try std.testing.expect(tree.search("prefix_shared_b") != null);
    try std.testing.expect(tree.search("prefix_shared_c") == null);

    // The inner node should have compressed the "prefix_shared_" part
    try std.testing.expectEqual(@as(usize, 2), tree.count());
}

test "ART: node growth 4 -> 16 -> 48 -> 256" {
    var tree = ART.init(std.testing.allocator);
    defer tree.deinit();

    // Insert keys that share a prefix but diverge at one byte,
    // forcing node growth at that position.
    var buf: [2]u8 = undefined;
    buf[0] = 'K';

    // 5 keys -> Node4 must grow to Node16
    var i: u16 = 0;
    while (i < 5) : (i += 1) {
        buf[1] = @intCast(i + 1);
        try tree.insert(buf[0..2], makeEntry(i + 1));
    }
    try std.testing.expectEqual(@as(usize, 5), tree.count());

    // 17 keys -> Node16 must grow to Node48
    while (i < 17) : (i += 1) {
        buf[1] = @intCast(i + 1);
        try tree.insert(buf[0..2], makeEntry(i + 1));
    }
    try std.testing.expectEqual(@as(usize, 17), tree.count());

    // 49 keys -> Node48 must grow to Node256
    while (i < 49) : (i += 1) {
        buf[1] = @intCast(i + 1);
        try tree.insert(buf[0..2], makeEntry(i + 1));
    }
    try std.testing.expectEqual(@as(usize, 49), tree.count());

    // Verify all keys are searchable
    var j: u16 = 0;
    while (j < 49) : (j += 1) {
        buf[1] = @intCast(j + 1);
        const result = tree.search(buf[0..2]);
        try std.testing.expect(result != null);
        try std.testing.expectEqual(@as(u64, j + 1), result.?.doc_id);
    }
}

test "ART: update existing key" {
    var tree = ART.init(std.testing.allocator);
    defer tree.deinit();

    try tree.insert("key", makeEntry(1));
    try tree.insert("key", makeEntry(42));

    try std.testing.expectEqual(@as(usize, 1), tree.count());
    const result = tree.search("key");
    try std.testing.expect(result != null);
    try std.testing.expectEqual(@as(u64, 42), result.?.doc_id);
}

test "ART: concurrent insert and search" {
    const num_threads = 4;
    const keys_per_thread = 25;

    var tree = ART.init(std.testing.allocator);
    defer tree.deinit();

    // Serialize all writes with a mutex. ART's optimistic lock coupling
    // provides safe concurrent reads alongside a single writer.
    var write_mu = std.Thread.Mutex{};

    // Pre-insert keys under mutex (single-threaded here, but same pattern)
    for (0..keys_per_thread) |i| {
        var key_buf: [32]u8 = undefined;
        const key_slice = std.fmt.bufPrint(&key_buf, "pre_{d:0>4}", .{i}) catch unreachable;
        try tree.insert(key_slice, makeEntry(i + 1));
    }

    const initial_count = tree.count();
    try std.testing.expectEqual(keys_per_thread, initial_count);

    const Context = struct {
        tree: *ART,
        thread_id: usize,
        mu: *std.Thread.Mutex,

        fn insertWork(ctx: @This()) void {
            const base = (ctx.thread_id + 1) * 10000;
            for (0..keys_per_thread) |i| {
                var key_buf: [32]u8 = undefined;
                const key_slice = std.fmt.bufPrint(&key_buf, "thr_{d:0>4}", .{base + i}) catch continue;
                ctx.mu.lock();
                ctx.tree.insert(key_slice, makeEntry(base + i)) catch {};
                ctx.mu.unlock();
            }
        }

        fn searchWork(ctx: @This()) void {
            // Concurrent reads: may see null due to optimistic version mismatch,
            // but must not crash.
            for (0..keys_per_thread) |i| {
                var key_buf: [32]u8 = undefined;
                const key_slice = std.fmt.bufPrint(&key_buf, "pre_{d:0>4}", .{i}) catch continue;
                _ = ctx.tree.search(key_slice);
            }
        }
    };

    // Launch writer threads, then reader threads
    var threads: [num_threads * 2]std.Thread = undefined;
    for (0..num_threads) |t| {
        threads[t] = try std.Thread.spawn(.{}, Context.insertWork, .{Context{
            .tree = &tree,
            .thread_id = t,
            .mu = &write_mu,
        }});
    }
    for (num_threads..num_threads * 2) |t| {
        threads[t] = try std.Thread.spawn(.{}, Context.searchWork, .{Context{
            .tree = &tree,
            .thread_id = t,
            .mu = &write_mu,
        }});
    }

    for (&threads) |*thr| {
        thr.join();
    }

    // Post-join (single-threaded): all pre-inserted keys MUST be findable
    for (0..keys_per_thread) |i| {
        var key_buf: [32]u8 = undefined;
        const key_slice = std.fmt.bufPrint(&key_buf, "pre_{d:0>4}", .{i}) catch unreachable;
        const result = tree.search(key_slice);
        if (result == null) {
            std.debug.print("Missing pre-inserted key: {s}\n", .{key_slice});
        }
        try std.testing.expect(result != null);
    }

    // Thread-inserted keys should also be present
    try std.testing.expect(tree.count() > keys_per_thread);
}
