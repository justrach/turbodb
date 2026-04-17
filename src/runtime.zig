//! Process-wide `std.Io` instance.
//!
//! Zig 0.16 routes filesystem, networking, time, random, and concurrency
//! primitives through an `Io` vtable. The application is expected to pick one
//! backend at startup and pass it (or a pointer to it) to every component that
//! needs it.
//!
//! TurboDB is a long-lived, threaded daemon. We instantiate one
//! `std.Io.Threaded` in `init(gpa)` and publish it as `runtime.io`. Every
//! module that needs an `Io` imports this file and reads `runtime.io`.
//!
//! Swapping to an evented backend (io_uring on Linux, Dispatch on macOS,
//! Kqueue on BSD) later is a one-line change in `init()`:
//! ```zig
//! evented = std.Io.Evented.init(gpa, .{});
//! io = evented.io();
//! ```
//! All mutexes, conditions, `std.Io.net.Stream` reads/writes, and
//! `std.Io.Dir`/`File` ops that route through `runtime.io` automatically start
//! using fibers + io_uring batching at that point. No handler rewrites needed.

const std = @import("std");

pub var threaded: std.Io.Threaded = undefined;
pub var io: std.Io = undefined;
var initialized: bool = false;

/// Must be called once at process startup, before any other module touches
/// `runtime.io`. Idempotent.
pub fn init(gpa: std.mem.Allocator) void {
    if (initialized) return;
    threaded = std.Io.Threaded.init(gpa, .{});
    io = threaded.io();
    initialized = true;
}

pub fn deinit() void {
    if (!initialized) return;
    threaded.deinit();
    initialized = false;
}

/// Test-only: fall back to `global_single_threaded` if `init()` wasn't called.
/// This lets unit tests that don't bootstrap the full runtime still exercise
/// compat helpers. Never call this in production code — prefer `init()`.
pub fn ensureForTest() void {
    if (initialized) return;
    io = std.Io.Threaded.global_single_threaded.io();
    initialized = true;
}
