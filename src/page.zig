/// TurboDB — 4KB page allocator
const std = @import("std");
const mmap = @import("mmap");
pub const PAGE_SIZE: usize = 65536; // 64KB — supports code files up to ~65KB per doc
pub const PAGE_HEADER_SIZE: usize = 32;
pub const PAGE_USABLE: usize = PAGE_SIZE - PAGE_HEADER_SIZE;

// ─── PageHeader (32 bytes) ────────────────────────────────────────────────

pub const PageType = enum(u8) {
    free       = 0,
    leaf       = 1,  // document storage
    internal   = 2,  // B-tree internal node
    overflow   = 3,  // large document continuation
    btree_leaf = 4,  // B-tree leaf node (NOT for document storage)
};

pub const PageHeader = extern struct {
    page_no: u32 align(4),   //  0 this page's own number
    next_page: u32,          //  4 next page in free list or overflow chain
    used_bytes: u16,         //  8 bytes used in usable area
    doc_count: u16,          // 10 number of documents on this page
    page_type: u8,           // 12
    flags: u8,               // 13
    _pad: [18]u8,            // 14-31

    pub const size = @sizeOf(PageHeader);
    comptime { std.debug.assert(size == 32); }
};

// ─── PageFile ────────────────────────────────────────────────────────────

pub const PageFile = struct {
    mm: mmap.MmapFile,
    free_head: std.atomic.Value(u32),  // head of free-list (0 = empty)
    next_alloc: std.atomic.Value(u32), // next never-allocated page
    mu: std.Thread.Mutex,
    leaf_mu: std.Thread.Mutex,         // protects leafAppend read-modify-write

    pub fn open(path: []const u8) !PageFile {
        var path_buf: [std.fs.max_path_bytes + 1]u8 = undefined;
        const path_z = try std.fmt.bufPrintZ(&path_buf, "{s}", .{path});
        const mm = try mmap.MmapFile.open(path_z, PAGE_SIZE * 16);
        const page_count: u32 = @intCast(mm.dataLen() / PAGE_SIZE);
        return PageFile{
            .mm         = mm,
            .free_head  = std.atomic.Value(u32).init(0),
            .next_alloc = std.atomic.Value(u32).init(page_count),
            .mu         = .{},
            .leaf_mu    = .{},
        };
    }

    pub fn close(self: *PageFile) void {
        self.mm.close();
    }

    pub fn sync(self: *PageFile) !void {
        try self.mm.syncSync();
    }

    /// Allocate a fresh page. Returns the page number.
    pub fn allocPage(self: *PageFile, ptype: PageType) !u32 {
        self.mu.lock();
        defer self.mu.unlock();

        // Try to reclaim from free list first.
        const fhead = self.free_head.load(.acquire);
        if (fhead != 0) {
            const ph = self.pageHeader(fhead);
            self.free_head.store(ph.next_page, .release);
            ph.* = std.mem.zeroes(PageHeader);
            ph.page_no = fhead;
            ph.page_type = @intFromEnum(ptype);
            return fhead;
        }

        // Extend the file.
        const pno = self.next_alloc.fetchAdd(1, .seq_cst);
        const needed = (@as(usize, pno) + 1) * PAGE_SIZE;
        if (needed > self.mm.capacity.load(.acquire)) try self.mm.grow(needed);

        const ph = self.pageHeader(pno);
        ph.* = std.mem.zeroes(PageHeader);
        ph.page_no = pno;
        ph.page_type = @intFromEnum(ptype);
        return pno;
    }

    /// Return a page to the free list.
    pub fn freePage(self: *PageFile, pno: u32) void {
        if (pno == 0) return;
        self.mu.lock();
        defer self.mu.unlock();
        const ph = self.pageHeader(pno);
        ph.* = std.mem.zeroes(PageHeader);
        ph.page_type = @intFromEnum(PageType.free);
        ph.next_page = self.free_head.load(.monotonic);
        self.free_head.store(pno, .release);
    }

    /// Access the PageHeader of page `pno`.
    pub fn pageHeader(self: *PageFile, pno: u32) *PageHeader {
        return self.mm.at(PageHeader, @as(usize, pno) * PAGE_SIZE);
    }

    /// Access the usable data area of page `pno`.
    pub fn pageData(self: *PageFile, pno: u32) []u8 {
        const off = @as(usize, pno) * PAGE_SIZE + PAGE_HEADER_SIZE;
        return self.mm.slice(u8, off, PAGE_USABLE);
    }

    /// Append bytes to a leaf page. Returns offset within the usable area,
    /// or null if there isn't enough space.
    pub fn leafAppend(self: *PageFile, pno: u32, data: []const u8) ?u16 {
        self.leaf_mu.lock();
        defer self.leaf_mu.unlock();

        const ph = self.pageHeader(pno);
        const used = ph.used_bytes;
        if (@as(usize, used) + data.len > PAGE_USABLE) return null;
        const dest = self.pageData(pno);
        @memcpy(dest[used..][0..data.len], data);
        ph.used_bytes += @intCast(data.len);
        ph.doc_count += 1;
        return used;
    }

    /// Read `len` bytes from page `pno` at usable-area offset `off`.
    /// Returns null if the read would extend past the page boundary.
    pub fn leafRead(self: *PageFile, pno: u32, off: u16, len: usize) ?[]const u8 {
        if (@as(usize, off) + len > PAGE_USABLE) return null;
        const data = self.pageData(pno);
        return data[off..][0..len];
    }

    /// Iterate all documents on a leaf page, calling `cb` for each raw record slice.
    pub fn leafIter(
        self: *PageFile,
        pno: u32,
        ctx: anytype,
        comptime cb: fn (@TypeOf(ctx), []const u8) bool,
    ) void {
        const ph = self.pageHeader(pno);
        if (@as(PageType, @enumFromInt(ph.page_type)) != .leaf) return;
        const data = self.pageData(pno);
        var pos: usize = 0;
        while (pos + 32 <= ph.used_bytes) {
            const rem = data[pos..ph.used_bytes];
            if (!cb(ctx, rem)) return;
            // Advance by DocHeader + key_len + val_len.
            if (rem.len < 32) break;
            const kl: usize = @as(u16, @bitCast([2]u8{ rem[20], rem[21] }));
            const vl: usize = @as(u32, @bitCast([4]u8{ rem[16], rem[17], rem[18], rem[19] }));
            pos += 32 + kl + vl;
        }
    }
};
