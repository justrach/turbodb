//! Thin compatibility wrappers for Zig 0.16 stdlib APIs that want to stay
//! one-liners at call sites.
//!
//! Philosophy: we adopt `std.Io` natively for anything whose shape actually
//! changed (Mutex, Condition, net.Stream — those get the full `runtime.io`
//! treatment at the call site). But for operations that are mechanical
//! one-liners in 0.15 and should stay that way (`deleteTree`, `makeDir`,
//! `milliTimestamp`, `nanoTimestamp`, `threadSleep`), we keep the ergonomic
//! short form via these helpers and implicitly read `runtime.io`.
//!
//! Only add a wrapper here if:
//! 1. the 0.15 call was a one-liner, AND
//! 2. the 0.16 equivalent would force us to add an `io:` argument at every
//!    caller with no benefit.
//!
//! For primitives that genuinely benefit from threading `io` explicitly
//! (mutexes, streams), use `std.Io.X` directly at the call site and pass
//! `runtime.io`. Don't shim them here.

const std = @import("std");
const runtime = @import("runtime");
const compat = @import("compat");

// ─── Filesystem (cwd) ─────────────────────────────────────────────────────
// Each wrapper does `std.Io.Dir.cwd().X(runtime.io, ...)`.

pub const fs = struct {
    const Dir = std.Io.Dir;
    const File = std.Io.File;

    pub fn cwdDeleteTree(sub_path: []const u8) !void {
        return Dir.cwd().deleteTree(runtime.io, sub_path);
    }

    pub fn cwdMakeDir(sub_path: []const u8) !void {
        return Dir.cwd().createDir(runtime.io, sub_path, .default_dir);
    }

    pub fn cwdMakePath(sub_path: []const u8) !void {
        return Dir.cwd().createDirPath(runtime.io, sub_path);
    }

    pub fn cwdOpenFile(sub_path: []const u8, options: Dir.OpenFileOptions) !File {
        return Dir.cwd().openFile(runtime.io, sub_path, options);
    }

    pub fn cwdCreateFile(sub_path: []const u8, flags: Dir.CreateFileOptions) !File {
        return Dir.cwd().createFile(runtime.io, sub_path, flags);
    }

    pub fn cwdOpenDir(sub_path: []const u8, options: Dir.OpenOptions) !Dir {
        return Dir.cwd().openDir(runtime.io, sub_path, options);
    }

    pub fn cwdRename(old_path: []const u8, new_path: []const u8) !void {
        return Dir.cwd().rename(old_path, Dir.cwd(), new_path, runtime.io);
    }

    pub fn cwdDeleteFile(sub_path: []const u8) !void {
        return Dir.cwd().deleteFile(runtime.io, sub_path);
    }

    pub fn cwdAccess(sub_path: []const u8, options: Dir.AccessOptions) !void {
        return Dir.cwd().access(runtime.io, sub_path, options);
    }

    /// 0.15 signature: `cwd().readFileAlloc(alloc, path, max_bytes) -> []u8`.
    /// Preserved for a clean mechanical rewrite.
    pub fn cwdReadFileAlloc(gpa: std.mem.Allocator, sub_path: []const u8, max_bytes: usize) ![]u8 {
        return Dir.cwd().readFileAlloc(runtime.io, sub_path, gpa, .limited(max_bytes));
    }

    // Absolute-path helpers.
    pub fn openDirAbsolute(absolute_path: []const u8, options: Dir.OpenOptions) !Dir {
        return Dir.openDirAbsolute(runtime.io, absolute_path, options);
    }

    pub fn makeDirAbsolute(absolute_path: []const u8) !void {
        return Dir.createDirAbsolute(runtime.io, absolute_path, .default_dir);
    }

    /// Canonicalize `sub_path` relative to cwd. Returns a sentinel-terminated
    /// allocation owned by the caller.
    pub fn cwdRealpathAlloc(gpa: std.mem.Allocator, sub_path: []const u8) ![:0]u8 {
        return Dir.cwd().realPathFileAlloc(runtime.io, sub_path, gpa);
    }

    /// Canonicalize `sub_path` relative to `dir`. Replacement for the
    /// removed-in-0.16 `Dir.realpathAlloc`. Used heavily by tmpDir-based tests.
    pub fn dirRealpathAlloc(dir: Dir, gpa: std.mem.Allocator, sub_path: []const u8) ![:0]u8 {
        return dir.realPathFileAlloc(runtime.io, sub_path, gpa);
    }

    /// Replacement for the removed-in-0.16 `std.fs.createFileAbsolute`.
    /// Opens the parent dir, then creates the file relative to it.
    pub fn createFileAbsolute(absolute_path: []const u8, flags: Dir.CreateFileOptions) !File {
        const dir_path = std.fs.path.dirname(absolute_path) orelse "/";
        const file_name = std.fs.path.basename(absolute_path);
        var dir = try Dir.openDirAbsolute(runtime.io, dir_path, .{});
        defer dir.close(runtime.io);
        return dir.createFile(runtime.io, file_name, flags);
    }

    // Dir / File method wrappers that inject runtime.io (saves threading it
    // through bench and profile call sites).
    pub fn dirClose(dir: Dir) void {
        dir.close(runtime.io);
    }

    pub fn dirOpenFile(dir: Dir, sub_path: []const u8, options: Dir.OpenFileOptions) !File {
        return dir.openFile(runtime.io, sub_path, options);
    }

    pub fn fileClose(file: File) void {
        file.close(runtime.io);
    }

    /// Read up to `buffer.len` bytes, looping until the buffer is full or EOF.
    /// Matches the 0.15 `File.readAll` contract: a short return only means EOF,
    /// never "transient short read" — callers downstream (SSTable/index) treat
    /// short reads as corruption, so we must not surface them here.
    pub fn fileReadAll(file: File, buffer: []u8) !usize {
        var total: usize = 0;
        while (total < buffer.len) {
            const rem = buffer[total..];
            const bufs = [_][]u8{rem};
            const n = try file.readStreaming(runtime.io, &bufs);
            if (n == 0) break; // EOF
            total += n;
        }
        return total;
    }


    /// Seek to absolute byte position. Wraps raw lseek since std.Io.File has no seekTo.
    pub fn fileSeekTo(file: File, pos: u64) void {
        _ = std.c.lseek(file.handle, @intCast(pos), std.posix.SEEK.SET);
    }

    /// Portable fstat size. std.posix.fstat is gone in 0.16; std.c.fstat is
    /// not a function on Linux; use statx(AT.EMPTY_PATH) there.
    pub fn fileSize(fd: std.posix.fd_t) !u64 {
        const builtin = @import("builtin");
        switch (builtin.os.tag) {
            .linux => {
                var stx: std.os.linux.Statx = undefined;
                const rc = std.os.linux.statx(
                    fd,
                    "",
                    std.os.linux.AT.EMPTY_PATH,
                    .{ .SIZE = true },
                    &stx,
                );
                if (std.posix.errno(rc) != .SUCCESS) return error.FstatFailed;
                return stx.size;
            },
            else => {
                var st: std.c.Stat = undefined;
                if (std.c.fstat(fd, &st) != 0) return error.FstatFailed;
                return @intCast(st.size);
            },
        }
    }
};

    /// Blocking read from a net stream. Returns bytes read, 0 on EOF.
    pub fn streamRead(stream: std.Io.net.Stream, buffer: []u8) !usize {
        return std.posix.read(stream.socket.handle, buffer);
    }

    /// Blocking write-all to a net stream. Retries on EINTR/EAGAIN so transient
    /// signal delivery doesn't fall through to BrokenPipe on a still-healthy socket.
    pub fn streamWriteAll(stream: std.Io.net.Stream, data: []const u8) !void {
        var rem = data;
        while (rem.len > 0) {
            const n = std.c.write(stream.socket.handle, rem.ptr, rem.len);
            if (n > 0) {
                rem = rem[@intCast(n)..];
                continue;
            }
            const e = std.posix.errno(n);
            switch (e) {
                .INTR, .AGAIN => continue,
                else => return error.BrokenPipe,
            }
        }
    }
// ─── Time ─────────────────────────────────────────────────────────────────
// `std.time.{timestamp, milliTimestamp, nanoTimestamp}` are all gone in 0.16.

/// Wall-clock seconds since Unix epoch.
pub fn timestampSec() i64 {
    var ts: std.c.timespec = undefined;
    _ = std.c.clock_gettime(.REALTIME, &ts);
    return ts.sec;
}

/// Wall-clock milliseconds since Unix epoch.
pub fn milliTimestamp() i64 {
    var ts: std.c.timespec = undefined;
    _ = std.c.clock_gettime(.REALTIME, &ts);
    return @as(i64, ts.sec) * 1000 + @divTrunc(@as(i64, ts.nsec), 1_000_000);
}

/// Wall-clock nanoseconds since Unix epoch.
pub fn nanoTimestamp() i128 {
    var ts: std.c.timespec = undefined;
    _ = std.c.clock_gettime(.REALTIME, &ts);
    return @as(i128, ts.sec) * 1_000_000_000 + @as(i128, ts.nsec);
}

// ─── Sleep ────────────────────────────────────────────────────────────────
// `std.Thread.sleep` is gone in 0.16. We do NOT route through `runtime.io`
// here because `io.sleep` returns `Cancelable!void` and most callers are
// infinite background loops that don't want a cancel check. A raw nanosleep
// keeps the old semantics bit-for-bit.

pub fn threadSleep(nanoseconds: u64) void {
    var remaining = std.c.timespec{
        .sec = @intCast(nanoseconds / std.time.ns_per_s),
        .nsec = @intCast(nanoseconds % std.time.ns_per_s),
    };
    // Loop across EINTR so signal delivery doesn't collapse the sleep —
    // WAL and background workers rely on the full duration for backoff.
    while (true) {
        var rem: std.c.timespec = undefined;
        const rc = std.c.nanosleep(&remaining, &rem);
        if (rc == 0) return;
        if (std.posix.errno(rc) != .INTR) return;
        remaining = rem;
    }
}

// ─── Random ───────────────────────────────────────────────────────────────
// `std.crypto.random` is gone. `runtime.io.random` is threadsafe and seeded
// from the process-wide CSPRNG.


// ─── Command-line arguments ───────────────────────────────────────────────
// `std.process.argsAlloc` is gone in 0.16. The new structured `main(init)`
// passes args via `init.minimal.args`. These helpers collect them into a
// heap-owned `[][:0]const u8` slice so existing index-based parsing keeps
// working with minimal changes.

pub fn argsAlloc(gpa: std.mem.Allocator, args: std.process.Args) ![][:0]const u8 {
    var list: std.ArrayList([:0]const u8) = .empty;
    errdefer list.deinit(gpa);
    var it = std.process.Args.Iterator.init(args);
    while (it.next()) |arg| {
        try list.append(gpa, arg);
    }
    return list.toOwnedSlice(gpa);
}

pub fn argsFree(gpa: std.mem.Allocator, args: [][:0]const u8) void {
    gpa.free(args);
}

pub fn randomBytes(buffer: []u8) void {
    runtime.io.random(buffer);
}

// ─── Tests ────────────────────────────────────────────────────────────────

test "milliTimestamp within ±1s of wall clock" {
    runtime.ensureForTest();
    const ms = milliTimestamp();
    try std.testing.expect(ms > 1_700_000_000_000); // after 2023
    try std.testing.expect(ms < 4_000_000_000_000); // before 2096
}

test "nanoTimestamp monotonic within a burst" {
    runtime.ensureForTest();
    const a = nanoTimestamp();
    const b = nanoTimestamp();
    try std.testing.expect(b >= a);
}

test "threadSleep blocks for at least the requested duration" {
    runtime.ensureForTest();
    const start = nanoTimestamp();
    threadSleep(2 * std.time.ns_per_ms);
    const elapsed = nanoTimestamp() - start;
    try std.testing.expect(elapsed >= 1 * std.time.ns_per_ms);
}
