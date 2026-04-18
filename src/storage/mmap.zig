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
const runtime = @import("runtime");

pub const GROW_CHUNK: usize = 256 * 1024 * 1024; // 256 MiB
pub const PAGE_SIZE:  usize = 4096;

pub const MmapFile = struct {
    fd:       posix.fd_t,
    ptr:      [*]align(PAGE_SIZE) u8,
    len:      usize,        // logical data length
    capacity: usize,        // mapped (file) length; >= len, multiple of PAGE_SIZE
    /// Protects ptr/capacity against concurrent grow+read.
    /// Writers (grow) take exclusive; readers (at/slice) take shared.
    rw_lock:  std.Io.RwLock,

    // ── Open / Close ──────────────────────────────────────────────────────────

    pub fn open(path: [:0]const u8, initial_size: usize) !MmapFile {
        const flags = posix.O{ .ACCMODE = .RDWR, .CREAT = true };
        const fd = try posix.openatZ(posix.AT.FDCWD, path, flags, 0o644);
        errdefer _ = std.c.close(fd);

        const file_size = try @import("compat").fs.fileSize(fd);
        const existing: usize = @intCast(file_size);
        const capacity = alignUp(
            @max(existing, @max(initial_size, GROW_CHUNK)),
            PAGE_SIZE,
        );

        if (file_size < capacity)
            if (std.c.ftruncate(fd, @intCast(capacity)) != 0) return error.FtruncateFailed;

        const ptr = try posix.mmap(
            null, capacity,
            .{ .READ = true, .WRITE = true },
            .{ .TYPE = .SHARED },
            fd, 0,
        );

        return .{
            .fd       = fd,
            .ptr      = ptr.ptr,
            .len      = existing,
            .capacity = capacity,
            .rw_lock  = .init,
        };
    }

    pub fn close(self: *MmapFile) void {
        posix.munmap(@alignCast(self.ptr[0..self.capacity]));
        _ = std.c.close(self.fd);
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
        self.rw_lock.lockUncancelable(runtime.io);
        defer self.rw_lock.unlock(runtime.io);
        // Re-check after acquiring lock (another thread may have grown).
        if (needed_len <= self.capacity) {
            self.len = needed_len;
            return;
        }
        const new_cap = alignUp(needed_len + GROW_CHUNK, PAGE_SIZE);
        // Extend file
        if (std.c.ftruncate(self.fd, @intCast(new_cap)) != 0) return error.FtruncateFailed;
        // Remap (macOS has no mremap; unmap then remap)
        posix.munmap(@alignCast(self.ptr[0..self.capacity]));
        const ptr = try posix.mmap(
            null, new_cap,
            .{ .READ = true, .WRITE = true },
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
        self.rw_lock.lockSharedUncancelable(runtime.io);
        defer self.rw_lock.unlockShared(runtime.io);
        return @alignCast(@ptrCast(&self.ptr[off]));
    }

    /// Return a slice of T starting at byte offset `off`, `count` elements.
    /// Takes a shared lock so grow() cannot remap underneath us.
    pub fn slice(self: *MmapFile, comptime T: type, off: usize, count: usize) []T {
        self.rw_lock.lockSharedUncancelable(runtime.io);
        defer self.rw_lock.unlockShared(runtime.io);
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
