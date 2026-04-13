//! Memory-mapped file — mmap wrapper for ZigGraph and TurboDB.
//!
//! Exposes the OS page cache directly as a typed slice.  All reads are
//! zero-copy; writes go through the page cache and are made durable
//! by msync(MS_SYNC) at checkpoint time.
//!
//! Stability guarantee: we pre-reserve a large virtual address range
//! (MAX_VA_SIZE) at open() time.  grow() extends the backing file and
//! maps new pages into the *same* VA range with MAP_FIXED, so the base
//! pointer never moves.  at() / slice() are therefore lock-free — no
//! pointer can dangle because the mapping base is stable for the
//! lifetime of the MmapFile.
const std   = @import("std");
const posix = std.posix;

pub const GROW_CHUNK: usize = 256 * 1024 * 1024; // 256 MiB
pub const PAGE_SIZE:  usize = 4096;

/// OS minimum page alignment — on Apple Silicon this is 16384, on x86-64
/// it is 4096.  All mmap pointers must satisfy this alignment.
const OS_PAGE_ALIGN: usize = std.heap.page_size_min;

/// 4 GiB virtual reservation.  Costs zero physical memory — pages are
/// only materialised when backed by ftruncate + MAP_FIXED.
pub const MAX_VA_SIZE: usize = 4 * 1024 * 1024 * 1024;

pub const MmapFile = struct {
    fd:       posix.fd_t,
    /// Stable base pointer — lives for the entire VA reservation lifetime.
    /// Never changes after open().
    ptr:      [*]align(OS_PAGE_ALIGN) u8,
    /// Logical data length.  Written by grow(), read by appendRecords() and
    /// external callers.  Uses atomic ops to avoid a data race with
    /// concurrent readers (no lock needed thanks to the stable VA range).
    len:      std.atomic.Value(usize),
    /// How many bytes of the VA range are currently file-backed and mapped
    /// PROT_READ|PROT_WRITE.  Atomic so concurrent at()/slice() callers
    /// can read without holding grow_mu.
    capacity: std.atomic.Value(usize),
    /// Serialises concurrent grow() calls.  NOT needed for readers.
    grow_mu:  std.Thread.Mutex,

    // ── Open / Close ──────────────────────────────────────────────────────────

    pub fn open(path: [:0]const u8, initial_size: usize) !MmapFile {
        const flags = posix.O{ .ACCMODE = .RDWR, .CREAT = true };
        const fd = try posix.open(path, flags, 0o644);
        errdefer posix.close(fd);

        const stat = try posix.fstat(fd);
        const existing: usize = @intCast(stat.size);
        const capacity = alignUp(
            @max(existing, @max(initial_size, GROW_CHUNK)),
            OS_PAGE_ALIGN,
        );

        // 1. Reserve the full virtual address range (PROT_NONE, anonymous).
        //    No physical memory is consumed — this just carves out VA space.
        const reserve = try posix.mmap(
            null, MAX_VA_SIZE,
            posix.PROT.NONE,
            .{ .TYPE = .PRIVATE, .ANONYMOUS = true },
            -1, 0,
        );
        const base_ptr: [*]align(OS_PAGE_ALIGN) u8 = @alignCast(reserve.ptr);

        // 2. Extend the backing file to `capacity` if needed.
        if (@as(usize, @intCast(stat.size)) < capacity)
            try posix.ftruncate(fd, @intCast(capacity));

        // 3. Map the file content over the start of the reserved range
        //    with MAP_FIXED so the kernel places it exactly at base_ptr.
        _ = try posix.mmap(
            base_ptr, capacity,
            posix.PROT.READ | posix.PROT.WRITE,
            .{ .TYPE = .SHARED, .FIXED = true },
            fd, 0,
        );

        return .{
            .fd       = fd,
            .ptr      = base_ptr,
            .len      = std.atomic.Value(usize).init(existing),
            .capacity = std.atomic.Value(usize).init(capacity),
            .grow_mu  = .{},
        };
    }

    pub fn close(self: *MmapFile) void {
        // Unmap the entire VA reservation in one call.
        posix.munmap(@alignCast(self.ptr[0..MAX_VA_SIZE]));
        posix.close(self.fd);
    }

    // ── Sync / Checkpoint ─────────────────────────────────────────────────────

    pub fn syncAsync(self: *MmapFile) void {
        const cap = self.capacity.load(.acquire);
        posix.msync(@alignCast(self.ptr[0..cap]), posix.MSF.ASYNC) catch {};
    }

    pub fn syncSync(self: *MmapFile) !void {
        const cap = self.capacity.load(.acquire);
        try posix.msync(@alignCast(self.ptr[0..cap]), posix.MSF.SYNC);
    }

    // ── Grow ──────────────────────────────────────────────────────────────────

    /// Extend the logical length.  If the file-backed region is too small,
    /// extends the file and maps the new pages into the pre-reserved VA
    /// range with MAP_FIXED.  The base pointer never changes.
    pub fn grow(self: *MmapFile, needed_len: usize) !void {
        if (needed_len <= self.capacity.load(.acquire)) {
            // Capacity is sufficient — just advance the logical length.
            self.len.store(needed_len, .release);
            return;
        }

        // Must extend capacity — serialise with other growers.
        self.grow_mu.lock();
        defer self.grow_mu.unlock();

        // Re-check after acquiring lock (another thread may have grown).
        const cur_cap = self.capacity.load(.acquire);
        if (needed_len <= cur_cap) {
            self.len.store(needed_len, .release);
            return;
        }

        const old_cap = cur_cap;
        const new_cap = alignUp(needed_len + GROW_CHUNK, OS_PAGE_ALIGN);

        std.debug.assert(new_cap <= MAX_VA_SIZE);

        // Extend the backing file.
        try posix.ftruncate(self.fd, @intCast(new_cap));

        // Map the NEW portion of the file into the pre-reserved VA range.
        // MAP_FIXED overwrites the PROT_NONE reservation for this sub-range.
        const delta = new_cap - old_cap;
        _ = try posix.mmap(
            @alignCast(self.ptr + old_cap), delta,
            posix.PROT.READ | posix.PROT.WRITE,
            .{ .TYPE = .SHARED, .FIXED = true },
            self.fd, @intCast(old_cap),
        );

        self.capacity.store(new_cap, .release);
        self.len.store(needed_len, .release);
    }

    // ── Typed access ──────────────────────────────────────────────────────────

    /// Return a pointer to the record at byte offset `off`.
    /// Lock-free: the base pointer is stable for the lifetime of the mapping.
    pub fn at(self: *MmapFile, comptime T: type, off: usize) *T {
        std.debug.assert(off + @sizeOf(T) <= self.capacity.load(.acquire));
        return @alignCast(@ptrCast(&self.ptr[off]));
    }

    /// Return a slice of T starting at byte offset `off`, `count` elements.
    /// Lock-free: the base pointer is stable for the lifetime of the mapping.
    pub fn slice(self: *MmapFile, comptime T: type, off: usize, count: usize) []T {
        std.debug.assert(off + count * @sizeOf(T) <= self.capacity.load(.acquire));
        return @as([*]T, @alignCast(@ptrCast(&self.ptr[off])))[0..count];
    }

    /// Append `count` new T-records after existing data; returns their base offset.
    pub fn appendRecords(self: *MmapFile, comptime T: type, count: usize) !usize {
        const off  = alignUp(self.len.load(.acquire), @alignOf(T));
        const need = off + @sizeOf(T) * count;
        try self.grow(need);
        return off;
    }

    /// Read the logical data length (atomic, safe from any thread).
    pub fn dataLen(self: *const MmapFile) usize {
        return self.len.load(.acquire);
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    inline fn alignUp(v: usize, a: usize) usize {
        return (v + a - 1) & ~(a - 1);
    }
};
