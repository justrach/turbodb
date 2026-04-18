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

/// Lockless init state machine. Uninit → Initing → Ready.
/// Losers of the CAS spin on `state` until it reaches Ready, then read `io`
/// (which is published with a release store by the winner).
const State = enum(u8) { uninit = 0, initing = 1, ready = 2 };
var state: std.atomic.Value(u8) = std.atomic.Value(u8).init(@intFromEnum(State.uninit));

/// Returns true if the caller won the CAS and should perform init; false if
/// it lost or init already finished (in which case it has already spin-waited
/// until the winner published the final state).
fn claimOrWait() bool {
    if (state.load(.acquire) == @intFromEnum(State.ready)) return false;
    if (state.cmpxchgStrong(
        @intFromEnum(State.uninit),
        @intFromEnum(State.initing),
        .acquire,
        .acquire,
    )) |_| {
        while (state.load(.acquire) != @intFromEnum(State.ready)) std.atomic.spinLoopHint();
        return false;
    }
    return true;
}

/// Must be called once at process startup, before any other module touches
/// `runtime.io`. Idempotent and thread-safe — concurrent FFI callers can race
/// into `turbodb_open`, but only the first one through the gate initializes.
pub fn init(gpa: std.mem.Allocator) void {
    if (!claimOrWait()) return;
    threaded = std.Io.Threaded.init(gpa, .{});
    io = threaded.io();
    state.store(@intFromEnum(State.ready), .release);
}

/// Publish an externally-owned Io (e.g. `std.process.Init.io` from the new
/// structured main) as the process-wide runtime. Preferred over `init()`
/// because the stdlib start code already instantiates Io based on build
/// options; we just piggyback on it.
pub fn setIo(external: std.Io) void {
    if (!claimOrWait()) return;
    io = external;
    state.store(@intFromEnum(State.ready), .release);
}

pub fn deinit() void {
    if (state.load(.acquire) != @intFromEnum(State.ready)) return;
    threaded.deinit();
    state.store(@intFromEnum(State.uninit), .release);
}

/// Test-only: fall back to `global_single_threaded` if `init()` wasn't called.
/// This lets unit tests that don't bootstrap the full runtime still exercise
/// compat helpers. Never call this in production code — prefer `init()`.
pub fn ensureForTest() void {
    if (!claimOrWait()) return;
    io = std.Io.Threaded.global_single_threaded.io();
    state.store(@intFromEnum(State.ready), .release);
}
