/// TurboDB async I/O engine — event-driven connection handling for 10K+ connections.
///
/// Backends:
///   - Linux:    io_uring via std.os.linux.IoUring
///   - macOS:    kqueue + kevent via std.posix
///
/// Architecture:
///   Single I/O thread (accept + dispatch) → N worker threads via lock-free MPSC ring.
///   Connection pool pre-allocates fixed slots; each slot tracks its own state machine.

const std = @import("std");
const builtin = @import("builtin");
const posix = std.posix;
const Allocator = std.mem.Allocator;

// ─── Core types ───────────────────────────────────────────────────────────────

pub const CompletionTag = enum(u8) { accept, read, write, close };

pub const Completion = struct {
    fd: posix.fd_t,
    tag: CompletionTag,
    result: i32, // bytes transferred or negated errno
    user_data: usize, // connection index
};

pub const ConnState = enum { idle, reading, processing, writing, closing };

pub const Connection = struct {
    fd: posix.fd_t,
    state: ConnState,
    read_buf: [65536]u8,
    write_buf: [131072]u8,
    read_len: usize,
    write_len: usize,

    pub fn reset(self: *Connection) void {
        self.fd = -1;
        self.state = .idle;
        self.read_len = 0;
        self.write_len = 0;
    }
};

// ─── ConnectionPool ───────────────────────────────────────────────────────────

pub const ConnectionPool = struct {
    conns: []Connection,
    free_list: std.ArrayList(usize),
    alloc: Allocator,

    pub fn init(alloc: Allocator, max_conns: usize) !ConnectionPool {
        const conns = try alloc.alloc(Connection, max_conns);
        var free = try std.ArrayList(usize).initCapacity(alloc, max_conns);

        for (0..max_conns) |i| {
            conns[i] = .{
                .fd = -1,
                .state = .idle,
                .read_buf = undefined,
                .write_buf = undefined,
                .read_len = 0,
                .write_len = 0,
            };
            free.appendAssumeCapacity(i);
        }

        return .{ .conns = conns, .free_list = free, .alloc = alloc };
    }

    pub fn deinit(self: *ConnectionPool) void {
        self.free_list.deinit(self.alloc);
        self.alloc.free(self.conns);
    }

    /// Acquire a free connection slot. Returns null if pool is exhausted.
    pub fn acquire(self: *ConnectionPool) ?usize {
        return self.free_list.pop();
    }

    /// Release a connection slot back into the pool.
    pub fn release(self: *ConnectionPool, idx: usize) void {
        self.conns[idx].reset();
        self.free_list.append(self.alloc, idx) catch {}; // capacity was pre-reserved
    }
};

// ─── IoEngine — compile-time backend selection ────────────────────────────────

pub const IoEngine = struct {
    backend: Backend,
    alloc: Allocator,

    const Backend = switch (builtin.os.tag) {
        .linux => LinuxBackend,
        .macos, .freebsd, .netbsd, .openbsd => KqueueBackend,
        else => @compileError("unsupported OS for IoEngine"),
    };

    pub fn init(alloc: Allocator) !IoEngine {
        return .{
            .backend = try Backend.init(alloc),
            .alloc = alloc,
        };
    }

    pub fn deinit(self: *IoEngine) void {
        self.backend.deinit();
    }

    pub fn submitAccept(self: *IoEngine, listen_fd: posix.fd_t, user_data: usize) !void {
        try self.backend.submitAccept(listen_fd, user_data);
    }

    pub fn submitRead(self: *IoEngine, fd: posix.fd_t, buf: []u8, user_data: usize) !void {
        try self.backend.submitRead(fd, buf, user_data);
    }

    pub fn submitWrite(self: *IoEngine, fd: posix.fd_t, data: []const u8, user_data: usize) !void {
        try self.backend.submitWrite(fd, data, user_data);
    }

    pub fn submitClose(self: *IoEngine, fd: posix.fd_t, user_data: usize) !void {
        try self.backend.submitClose(fd, user_data);
    }

    /// Poll for completed I/O events. Returns the number of completions filled.
    pub fn poll(self: *IoEngine, completions: []Completion) !usize {
        return try self.backend.poll(completions);
    }
};

// ─── kqueue backend (macOS / BSD) ─────────────────────────────────────────────

const KqueueBackend = struct {
    kq: posix.fd_t,
    alloc: Allocator,
    /// Pending changelist to submit on next poll.
    changelist: std.ArrayListUnmanaged(posix.Kevent),
    /// Maps fd → user_data for accept sockets.
    pending_accepts: std.AutoHashMapUnmanaged(posix.fd_t, usize),
    pending_closes: std.ArrayListUnmanaged(PendingClose),

    const PendingClose = struct { fd: posix.fd_t, user_data: usize };

    fn init(alloc: Allocator) !KqueueBackend {
        const kq = std.c.kqueue();
        if (kq < 0) return error.KqueueInitFailed;
        return .{
            .kq = kq,
            .alloc = alloc,
            .changelist = .empty,
            .pending_accepts = .empty,
            .pending_closes = .empty,
        };
    }

    fn deinit(self: *KqueueBackend) void {
        _ = std.c.close(self.kq);
        self.changelist.deinit(self.alloc);
        self.pending_accepts.deinit(self.alloc);
        self.pending_closes.deinit(self.alloc);
    }

    fn submitAccept(self: *KqueueBackend, listen_fd: posix.fd_t, user_data: usize) !void {
        // Register EVFILT_READ on the listen socket — readable means a pending connection.
        try self.pending_accepts.put(self.alloc, listen_fd, user_data);
        try self.changelist.append(self.alloc, .{
            .ident = @intCast(listen_fd),
            .filter = std.posix.system.EVFILT.READ,
            .flags = std.posix.system.EV.ADD | std.posix.system.EV.ENABLE | std.posix.system.EV.CLEAR,
            .fflags = 0,
            .data = 0,
            .udata = user_data,
        });
    }

    fn submitRead(self: *KqueueBackend, fd: posix.fd_t, _: []u8, user_data: usize) !void {
        // Register edge-triggered read interest.
        try self.changelist.append(self.alloc, .{
            .ident = @intCast(fd),
            .filter = std.posix.system.EVFILT.READ,
            .flags = std.posix.system.EV.ADD | std.posix.system.EV.ENABLE | std.posix.system.EV.CLEAR,
            .fflags = 0,
            .data = 0,
            .udata = user_data,
        });
    }

    fn submitWrite(self: *KqueueBackend, fd: posix.fd_t, _: []const u8, user_data: usize) !void {
        // Register edge-triggered write interest.
        try self.changelist.append(self.alloc, .{
            .ident = @intCast(fd),
            .filter = std.posix.system.EVFILT.WRITE,
            .flags = std.posix.system.EV.ADD | std.posix.system.EV.ENABLE | std.posix.system.EV.ONESHOT,
            .fflags = 0,
            .data = 0,
            .udata = user_data,
        });
    }

    fn submitClose(self: *KqueueBackend, fd: posix.fd_t, user_data: usize) !void {
        // Remove all filters, then close fd. Synthesise a completion on next poll.
        try self.changelist.append(self.alloc, .{
            .ident = @intCast(fd),
            .filter = std.posix.system.EVFILT.READ,
            .flags = std.posix.system.EV.DELETE,
            .fflags = 0,
            .data = 0,
            .udata = user_data,
        });
        try self.pending_closes.append(self.alloc, .{ .fd = fd, .user_data = user_data });
    }

    fn poll(self: *KqueueBackend, completions: []Completion) !usize {
        // First, drain any pending close operations and synthesise completions.
        var count: usize = 0;

        for (self.pending_closes.items) |pc| {
            _ = std.c.close(pc.fd);
            if (count < completions.len) {
                completions[count] = .{
                    .fd = pc.fd,
                    .tag = .close,
                    .result = 0,
                    .user_data = pc.user_data,
                };
                count += 1;
            }
        }
        self.pending_closes.clearRetainingCapacity();

        if (count >= completions.len) return count;

        // Submit changelist and wait for events.
        const remaining = completions.len - count;
        var events: [256]posix.Kevent = undefined;
        const max_events = @min(remaining, events.len);

        const changes = self.changelist.items;
        const timeout = posix.timespec{ .sec = 0, .nsec = 100_000_000 }; // 100ms

        const n = posix.kevent(self.kq, changes, events[0..max_events], &timeout) catch |e| switch (e) {
            error.AccessDenied, error.SystemResources => return count,
            else => return e,
        };
        self.changelist.clearRetainingCapacity();

        for (events[0..n]) |ev| {
            if (count >= completions.len) break;

            const fd: posix.fd_t = @intCast(ev.ident);
            const user_data = ev.udata;

            // Check if this is a listen socket (accept).
            if (self.pending_accepts.get(fd)) |_| {
                // Accept the new connection.
                const accepted = posix.accept(fd, null, null, posix.SOCK.NONBLOCK | posix.SOCK.CLOEXEC) catch |e| {
                    completions[count] = .{
                        .fd = fd,
                        .tag = .accept,
                        .result = -@as(i32, @intCast(@intFromEnum(e))),
                        .user_data = user_data,
                    };
                    count += 1;
                    continue;
                };
                completions[count] = .{
                    .fd = accepted,
                    .tag = .accept,
                    .result = @intCast(accepted),
                    .user_data = user_data,
                };
                count += 1;
                continue;
            }

            if (ev.filter == std.posix.system.EVFILT.READ) {
                completions[count] = .{
                    .fd = fd,
                    .tag = .read,
                    .result = @intCast(ev.data), // bytes available
                    .user_data = user_data,
                };
                count += 1;
            } else if (ev.filter == std.posix.system.EVFILT.WRITE) {
                completions[count] = .{
                    .fd = fd,
                    .tag = .write,
                    .result = @intCast(ev.data), // writable bytes
                    .user_data = user_data,
                };
                count += 1;
            }
        }

        return count;
    }
};

// ─── io_uring backend (Linux) ─────────────────────────────────────────────────

const LinuxBackend = if (builtin.os.tag == .linux) struct {
    ring: std.os.linux.IoUring,

    fn init(_: Allocator) !LinuxBackend {
        return .{
            .ring = try std.os.linux.IoUring.init(256, 0),
        };
    }

    fn deinit(self: *LinuxBackend) void {
        self.ring.deinit();
    }

    fn submitAccept(self: *LinuxBackend, listen_fd: posix.fd_t, user_data: usize) !void {
        _ = self.ring.accept(packUserData(.accept, user_data), listen_fd, null, null, posix.SOCK.NONBLOCK | posix.SOCK.CLOEXEC) catch |e| return mapSqeError(e);
        _ = try self.ring.submit();
    }

    fn submitRead(self: *LinuxBackend, fd: posix.fd_t, buf: []u8, user_data: usize) !void {
        _ = self.ring.recv(packUserData(.read, user_data), fd, .{ .base = buf.ptr, .len = buf.len }, 0) catch |e| return mapSqeError(e);
        _ = try self.ring.submit();
    }

    fn submitWrite(self: *LinuxBackend, fd: posix.fd_t, data: []const u8, user_data: usize) !void {
        _ = self.ring.send(packUserData(.write, user_data), fd, .{ .base = data.ptr, .len = data.len }, 0) catch |e| return mapSqeError(e);
        _ = try self.ring.submit();
    }

    fn submitClose(self: *LinuxBackend, fd: posix.fd_t, user_data: usize) !void {
        _ = self.ring.close(packUserData(.close, user_data), fd) catch |e| return mapSqeError(e);
        _ = try self.ring.submit();
    }

    fn poll(self: *LinuxBackend, completions: []Completion) !usize {
        const cqes_max: usize = @min(completions.len, 256);

        // Wait for at least 1 completion.
        _ = self.ring.submit_and_wait(1) catch |e| switch (e) {
            error.SignalInterrupt => return 0,
            else => return e,
        };

        var count: usize = 0;
        while (count < cqes_max) {
            const cqe = self.ring.cq.head() orelse break;
            const raw = cqe.user_data;
            const tag = unpackTag(raw);
            const user_data = unpackUserData(raw);

            completions[count] = .{
                .fd = if (tag == .accept and cqe.res >= 0) @intCast(cqe.res) else -1,
                .tag = tag,
                .result = cqe.res,
                .user_data = user_data,
            };
            count += 1;
            self.ring.cq.advance(1);
        }

        return count;
    }

    /// Pack tag + user_data into a u64 for the SQE user_data field.
    fn packUserData(tag: CompletionTag, ud: usize) u64 {
        return (@as(u64, @intFromEnum(tag)) << 56) | @as(u64, @intCast(ud));
    }

    fn unpackTag(raw: u64) CompletionTag {
        return @enumFromInt(@as(u8, @truncate(raw >> 56)));
    }

    fn unpackUserData(raw: u64) usize {
        return @intCast(raw & 0x00FFFFFFFFFFFFFF);
    }

    fn mapSqeError(e: anytype) error{SystemResources} {
        _ = e;
        return error.SystemResources;
    }
} else struct {
    // Stub — never instantiated on non-Linux.
    fn init(_: Allocator) !@This() {
        unreachable;
    }
    fn deinit(_: *@This()) void {}
    fn submitAccept(_: *@This(), _: posix.fd_t, _: usize) !void {}
    fn submitRead(_: *@This(), _: posix.fd_t, _: []u8, _: usize) !void {}
    fn submitWrite(_: *@This(), _: posix.fd_t, _: []const u8, _: usize) !void {}
    fn submitClose(_: *@This(), _: posix.fd_t, _: usize) !void {}
    fn poll(_: *@This(), _: []Completion) !usize {
        return 0;
    }
};

// ─── Lock-free MPSC ring buffer (I/O thread → worker threads) ─────────────────

pub fn MpscRing(comptime T: type, comptime cap: usize) type {
    return struct {
        const Self = @This();

        buf: [cap]T,
        head: std.atomic.Value(usize), // consumer reads from here
        tail: std.atomic.Value(usize), // producers write here (CAS)

        pub fn init() Self {
            return .{
                .buf = undefined,
                .head = std.atomic.Value(usize).init(0),
                .tail = std.atomic.Value(usize).init(0),
            };
        }

        /// Try to enqueue an item. Returns false if full.
        pub fn push(self: *Self, item: T) bool {
            while (true) {
                const t = self.tail.load(.acquire);
                const h = self.head.load(.acquire);
                const next = (t + 1) % cap;
                if (next == h) return false; // full
                if (self.tail.cmpxchgWeak(t, next, .release, .monotonic) == null) {
                    self.buf[t] = item;
                    return true;
                }
                // CAS failed, retry.
            }
        }

        /// Try to dequeue an item. Returns null if empty. Single-consumer only.
        pub fn pop(self: *Self) ?T {
            const h = self.head.load(.acquire);
            const t = self.tail.load(.acquire);
            if (h == t) return null; // empty
            const item = self.buf[h];
            self.head.store((h + 1) % cap, .release);
            return item;
        }
    };
}

// ─── Work item for the MPSC queue ─────────────────────────────────────────────

pub const WorkItem = struct {
    conn_idx: usize,
    request_len: usize,
};

// ─── EventLoop ────────────────────────────────────────────────────────────────

pub const EventLoop = struct {
    engine: IoEngine,
    pool: ConnectionPool,
    listen_fd: posix.fd_t,
    alloc: Allocator,
    running: std.atomic.Value(bool),
    work_queue: *MpscRing(WorkItem, 4096),
    workers: []std.Thread,
    worker_count: usize,

    pub fn init(alloc: Allocator, max_conns: usize) !EventLoop {
        const wq = try alloc.create(MpscRing(WorkItem, 4096));
        wq.* = MpscRing(WorkItem, 4096).init();

        const cpu_count = std.Thread.getCpuCount() catch 4;
        const n_workers = @max(1, cpu_count);

        return .{
            .engine = try IoEngine.init(alloc),
            .pool = try ConnectionPool.init(alloc, max_conns),
            .listen_fd = -1,
            .alloc = alloc,
            .running = std.atomic.Value(bool).init(false),
            .work_queue = wq,
            .workers = try alloc.alloc(std.Thread, n_workers),
            .worker_count = n_workers,
        };
    }

    pub fn deinit(self: *EventLoop) void {
        if (self.running.load(.acquire)) {
            self.running.store(false, .release);
        }
        if (self.listen_fd >= 0) {
            _ = std.c.close(self.listen_fd);
        }
        self.engine.deinit();
        self.pool.deinit();
        self.alloc.free(self.workers);
        self.alloc.destroy(self.work_queue);
    }

    /// Bind and listen on the given TCP port.
    pub fn bind(self: *EventLoop, port: u16) !void {
        const fd = std.c.socket(2, std.c.SOCK.STREAM, 0);
        if (fd < 0) return error.SocketError;
        errdefer _ = std.c.close(fd);

        // SO_REUSEADDR for fast restart.
        var opt_val: c_int = 1;
        _ = std.c.setsockopt(fd, std.c.SOL.SOCKET, std.c.SO.REUSEADDR, @ptrCast(&opt_val), @sizeOf(c_int));

        var addr: extern struct { family: u16, port_be: u16 align(1), addr_be: u32 align(1), zero: [8]u8 align(1) } = .{
            .family = 2,
            .port_be = std.mem.nativeToBig(u16, port),
            .addr_be = 0,
            .zero = std.mem.zeroes([8]u8),
        };
        if (std.c.bind(fd, @ptrCast(&addr), @sizeOf(@TypeOf(addr))) != 0) return error.BindError;
        if (std.c.listen(fd, 128) != 0) return error.ListenError;
        self.listen_fd = fd;
    }

    /// Run the event loop. The handler is called on worker threads:
    ///   handler(request_bytes, response_buffer) → response_length
    pub fn run(self: *EventLoop, handler: *const fn (request: []const u8, response: []u8) usize) !void {
        if (self.listen_fd < 0) return error.NotBound;

        self.running.store(true, .release);

        // Spawn worker threads.
        for (0..self.worker_count) |i| {
            self.workers[i] = try std.Thread.spawn(.{}, workerThread, .{ self, handler });
        }

        // Submit initial accept.
        try self.engine.submitAccept(self.listen_fd, std.math.maxInt(usize));

        // I/O dispatch loop.
        var completions: [256]Completion = undefined;
        while (self.running.load(.acquire)) {
            const n = self.engine.poll(&completions) catch |e| {
                if (!self.running.load(.acquire)) break;
                return e;
            };

            for (completions[0..n]) |comp| {
                switch (comp.tag) {
                    .accept => self.handleAccept(comp),
                    .read => self.handleRead(comp),
                    .write => self.handleWrite(comp),
                    .close => self.handleClose(comp),
                }
            }
        }
    }

    /// Stop the event loop gracefully.
    pub fn stop(self: *EventLoop) void {
        self.running.store(false, .release);
    }

    // ── Internal completion handlers ──

    fn handleAccept(self: *EventLoop, comp: Completion) void {
        // Always re-arm the accept.
        self.engine.submitAccept(self.listen_fd, std.math.maxInt(usize)) catch {};

        if (comp.result < 0) return;

        const idx = self.pool.acquire() orelse {
            // Pool exhausted — reject.
            _ = std.c.close(comp.fd);
            return;
        };

        self.pool.conns[idx].fd = comp.fd;
        self.pool.conns[idx].state = .reading;

        // Submit a read on the new connection.
        self.engine.submitRead(comp.fd, &self.pool.conns[idx].read_buf, idx) catch {
            self.pool.release(idx);
            _ = std.c.close(comp.fd);
        };
    }

    fn handleRead(self: *EventLoop, comp: Completion) void {
        if (comp.user_data >= self.pool.conns.len) return;
        const conn = &self.pool.conns[comp.user_data];

        if (comp.result <= 0) {
            // EOF or error — close.
            conn.state = .closing;
            self.engine.submitClose(conn.fd, comp.user_data) catch {
                _ = std.c.close(conn.fd);
                self.pool.release(comp.user_data);
            };
            return;
        }

        const bytes: usize = @intCast(comp.result);

        // Perform the actual read on kqueue (kqueue only signals readability).
        if (builtin.os.tag != .linux) {
            const n = posix.read(conn.fd, conn.read_buf[conn.read_len..]) catch {
                conn.state = .closing;
                self.engine.submitClose(conn.fd, comp.user_data) catch {};
                return;
            };
            if (n == 0) {
                conn.state = .closing;
                self.engine.submitClose(conn.fd, comp.user_data) catch {};
                return;
            }
            conn.read_len += n;
        } else {
            _ = bytes;
            conn.read_len += @intCast(comp.result);
        }

        conn.state = .processing;

        // Push to worker queue.
        if (!self.work_queue.push(.{ .conn_idx = comp.user_data, .request_len = conn.read_len })) {
            // Queue full — close connection to shed load.
            conn.state = .closing;
            self.engine.submitClose(conn.fd, comp.user_data) catch {};
        }
    }

    fn handleWrite(self: *EventLoop, comp: Completion) void {
        if (comp.user_data >= self.pool.conns.len) return;
        const conn = &self.pool.conns[comp.user_data];

        if (comp.result <= 0) {
            conn.state = .closing;
            self.engine.submitClose(conn.fd, comp.user_data) catch {};
            return;
        }

        // For kqueue, we need to do the actual write.
        if (builtin.os.tag != .linux) {
            _ = posix.write(conn.fd, conn.write_buf[0..conn.write_len]) catch {
                conn.state = .closing;
                self.engine.submitClose(conn.fd, comp.user_data) catch {};
                return;
            };
        }

        // Reset for next request — go back to reading.
        conn.read_len = 0;
        conn.write_len = 0;
        conn.state = .reading;
        self.engine.submitRead(conn.fd, &conn.read_buf, comp.user_data) catch {
            conn.state = .closing;
            self.engine.submitClose(conn.fd, comp.user_data) catch {};
        };
    }

    fn handleClose(self: *EventLoop, comp: Completion) void {
        if (comp.user_data >= self.pool.conns.len) return;
        self.pool.release(comp.user_data);
    }

    // ── Worker thread ──

    fn workerThread(self: *EventLoop, handler: *const fn (request: []const u8, response: []u8) usize) void {
        while (self.running.load(.acquire)) {
            const item = self.work_queue.pop() orelse {
                // No work — brief spin-then-yield.
                std.atomic.spinLoopHint();
                std.Thread.yield() catch {};
                continue;
            };

            const conn = &self.pool.conns[item.conn_idx];
            const request = conn.read_buf[0..item.request_len];

            // Call the application handler.
            const resp_len = handler(request, &conn.write_buf);
            conn.write_len = resp_len;

            if (resp_len == 0) {
                // No response — close.
                conn.state = .closing;
                self.engine.submitClose(conn.fd, item.conn_idx) catch {};
                continue;
            }

            conn.state = .writing;
            self.engine.submitWrite(conn.fd, conn.write_buf[0..resp_len], item.conn_idx) catch {
                conn.state = .closing;
                self.engine.submitClose(conn.fd, item.conn_idx) catch {};
            };
        }
    }
};

// ─── Tests ────────────────────────────────────────────────────────────────────

test "IoEngine init/deinit" {
    var engine = try IoEngine.init(std.testing.allocator);
    defer engine.deinit();
    // If we got here without error, backend initialised correctly.
}

test "ConnectionPool acquire/release" {
    var pool = try ConnectionPool.init(std.testing.allocator, 4);
    defer pool.deinit();

    // Acquire all 4 slots.
    const a = pool.acquire().?;
    const b = pool.acquire().?;
    const c = pool.acquire().?;
    const d = pool.acquire().?;

    // All different indices.
    try std.testing.expect(a != b and b != c and c != d);

    // Pool should be exhausted.
    try std.testing.expect(pool.acquire() == null);

    // Release one and re-acquire.
    pool.release(b);
    const e = pool.acquire().?;
    try std.testing.expectEqual(b, e);

    // Exhausted again.
    try std.testing.expect(pool.acquire() == null);

    pool.release(a);
    pool.release(c);
    pool.release(d);
    pool.release(e);
}

test "ConnectionPool reset clears state" {
    var pool = try ConnectionPool.init(std.testing.allocator, 2);
    defer pool.deinit();

    const idx = pool.acquire().?;
    pool.conns[idx].fd = 42;
    pool.conns[idx].state = .writing;
    pool.conns[idx].read_len = 100;

    pool.release(idx);

    const idx2 = pool.acquire().?;
    try std.testing.expectEqual(idx, idx2);
    try std.testing.expectEqual(@as(posix.fd_t, -1), pool.conns[idx2].fd);
    try std.testing.expectEqual(ConnState.idle, pool.conns[idx2].state);
    try std.testing.expectEqual(@as(usize, 0), pool.conns[idx2].read_len);
}

test "EventLoop bind" {
    var loop = try EventLoop.init(std.testing.allocator, 64);
    defer loop.deinit();

    // Bind to an ephemeral port.
    try loop.bind(0);
    try std.testing.expect(loop.listen_fd >= 0);
}

test "MpscRing push/pop" {
    var ring = MpscRing(u32, 4).init();

    try std.testing.expect(ring.pop() == null); // empty

    try std.testing.expect(ring.push(10));
    try std.testing.expect(ring.push(20));
    try std.testing.expect(ring.push(30));
    try std.testing.expect(!ring.push(40)); // full (cap-1 usable slots)

    try std.testing.expectEqual(@as(u32, 10), ring.pop().?);
    try std.testing.expectEqual(@as(u32, 20), ring.pop().?);
    try std.testing.expectEqual(@as(u32, 30), ring.pop().?);
    try std.testing.expect(ring.pop() == null);
}
