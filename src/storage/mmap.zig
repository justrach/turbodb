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

    // ── Open / Close ──────────────────────────────────────────────────────────

    pub fn open(path: [:0]const u8, initial_size: usize) !MmapFile {
        const flags = posix.O{ .ACCMODE = .RDWR, .CREAT = true };
        const fd = try posix.openatZ(posix.AT.FDCWD, path, flags, 0o644);
        errdefer _ = std.c.close(fd);

        var stat_buf: std.c.Stat = undefined;
        if (std.c.fstat(fd, &stat_buf) != 0) return error.FstatFailed;
        const existing: usize = @intCast(stat_buf.size);
        const capacity = alignUp(
            @max(existing, @max(initial_size, GROW_CHUNK)),
            PAGE_SIZE,
        );

        if (@as(usize, @intCast(stat_buf.size)) < capacity)
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
    pub fn grow(self: *MmapFile, needed_len: usize) !void {
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
    pub fn at(self: MmapFile, comptime T: type, off: usize) *T {
        return @alignCast(@ptrCast(&self.ptr[off]));
    }

    /// Return a slice of T starting at byte offset `off`, `count` elements.
    pub fn slice(self: MmapFile, comptime T: type, off: usize, count: usize) []T {
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
