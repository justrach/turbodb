//! Memory-mapped file — mmap wrapper for ZigGraph and TurboDB.
//!
//! Exposes the OS page cache directly as a typed slice.  All reads are
//! zero-copy; writes go through the page cache and are made durable
//! by msync(MS_SYNC) at checkpoint time.
//!
//! Growth strategy: grow in GROW_CHUNK increments (256 MiB) to amortise
//! ftruncate + remap syscalls.  On macOS (no mremap) we munmap and remap.
const std   = @import("std");
const posix = std.posix;

pub const GROW_CHUNK: usize = 256 * 1024 * 1024; // 256 MiB
pub const PAGE_SIZE:  usize = 4096;

pub const MmapFile = struct {
    fd:       posix.fd_t,
    ptr:      [*]align(PAGE_SIZE) u8,
    len:      usize,        // logical data length
    capacity: usize,        // mapped (file) length; >= len, multiple of PAGE_SIZE
    /// Protects ptr/capacity against concurrent grow+read.
    /// Writers (grow) take exclusive; readers (at/slice) take shared.
    rw_lock:  std.Thread.RwLock,

    // ── Open / Close ──────────────────────────────────────────────────────────

    pub fn open(path: [:0]const u8, initial_size: usize) !MmapFile {
        const flags = posix.O{ .ACCMODE = .RDWR, .CREAT = true };
        const fd = try posix.open(path, flags, 0o644);
        errdefer posix.close(fd);

        const stat = try posix.fstat(fd);
        const existing: usize = @intCast(stat.size);
        const capacity = alignUp(
            @max(existing, @max(initial_size, GROW_CHUNK)),
            PAGE_SIZE,
        );

        if (@as(usize, @intCast(stat.size)) < capacity)
            try posix.ftruncate(fd, @intCast(capacity));

        const ptr = try posix.mmap(
            null, capacity,
            posix.PROT.READ | posix.PROT.WRITE,
            .{ .TYPE = .SHARED },
            fd, 0,
        );

        return .{
            .fd       = fd,
            .ptr      = ptr.ptr,
            .len      = existing,
            .capacity = capacity,
            .rw_lock  = .{},
        };
    }

    pub fn close(self: *MmapFile) void {
        posix.munmap(@alignCast(self.ptr[0..self.capacity]));
        posix.close(self.fd);
    }

    // ── Sync / Checkpoint ─────────────────────────────────────────────────────

    pub fn syncAsync(self: MmapFile) void {
        posix.msync(@alignCast(self.ptr[0..self.capacity]), posix.MSF.ASYNC) catch {};
    }

    pub fn syncSync(self: MmapFile) !void {
        try posix.msync(@alignCast(self.ptr[0..self.capacity]), posix.MSF.SYNC);
    }

    // ── Grow ──────────────────────────────────────────────────────────────────

    /// Extend the logical length.  Remaps if needed (macOS: munmap + mmap).
    /// Takes an exclusive lock so concurrent readers cannot hold stale pointers.
    pub fn grow(self: *MmapFile, needed_len: usize) !void {
        if (needed_len <= self.capacity) {
            self.len = needed_len;
            return;
        }
        // Must remap — take exclusive lock to block readers during munmap/mmap.
        self.rw_lock.lock();
        defer self.rw_lock.unlock();
        // Re-check after acquiring lock (another thread may have grown).
        if (needed_len <= self.capacity) {
            self.len = needed_len;
            return;
        }
        const new_cap = alignUp(needed_len + GROW_CHUNK, PAGE_SIZE);
        // Extend file
        try posix.ftruncate(self.fd, @intCast(new_cap));
        // Remap (macOS has no mremap; unmap then remap)
        posix.munmap(@alignCast(self.ptr[0..self.capacity]));
        const ptr = try posix.mmap(
            null, new_cap,
            posix.PROT.READ | posix.PROT.WRITE,
            .{ .TYPE = .SHARED },
            self.fd, 0,
        );
        self.ptr      = ptr.ptr;
        self.capacity = new_cap;
        self.len      = needed_len;
    }

    // ── Typed access ──────────────────────────────────────────────────────────

    /// Return a pointer to the record at byte offset `off`.
    /// Takes a shared lock so grow() cannot remap underneath us.
    pub fn at(self: *MmapFile, comptime T: type, off: usize) *T {
        self.rw_lock.lockShared();
        defer self.rw_lock.unlockShared();
        std.debug.assert(off + @sizeOf(T) <= self.capacity);
        return @alignCast(@ptrCast(&self.ptr[off]));
    }

    /// Return a slice of T starting at byte offset `off`, `count` elements.
    /// Takes a shared lock so grow() cannot remap underneath us.
    pub fn slice(self: *MmapFile, comptime T: type, off: usize, count: usize) []T {
        self.rw_lock.lockShared();
        defer self.rw_lock.unlockShared();
        std.debug.assert(off + count * @sizeOf(T) <= self.capacity);
        return @as([*]T, @alignCast(@ptrCast(&self.ptr[off])))[0..count];
    }

    /// Append `count` new T-records after existing data; returns their base offset.
    pub fn appendRecords(self: *MmapFile, comptime T: type, count: usize) !usize {
        const off  = alignUp(self.len, @alignOf(T));
        const need = off + @sizeOf(T) * count;
        try self.grow(need);
        return off;
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    inline fn alignUp(v: usize, a: usize) usize {
        return (v + a - 1) & ~(a - 1);
    }
};
