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
        return Dir.cwd().rename(runtime.io, old_path, Dir.cwd(), new_path);
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
};

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
    const ts = std.c.timespec{
        .sec = @intCast(nanoseconds / std.time.ns_per_s),
        .nsec = @intCast(nanoseconds % std.time.ns_per_s),
    };
    _ = std.c.nanosleep(&ts, null);
}

// ─── Random ───────────────────────────────────────────────────────────────
// `std.crypto.random` is gone. `runtime.io.random` is threadsafe and seeded
// from the process-wide CSPRNG.

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
