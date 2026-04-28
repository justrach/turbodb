/// Zig 0.16 compatibility layer for TurboDB
///
/// Provides backward-compatible APIs for code written against Zig 0.15's
/// std.fs, std.Thread, std.io, and std.time. Uses POSIX/C functions directly
/// to avoid threading std.Io through the entire codebase.
const std = @import("std");
const c = std.c;
const posix = std.posix;

// ═══════════════════════════════════════════════════════════════════════════════
// File — drop-in replacement for std.fs.File (0.15)
// ═══════════════════════════════════════════════════════════════════════════════

pub const File = struct {
    handle: posix.fd_t,

    pub const Reader = struct {
        file: File,

        pub fn readAll(self: Reader, buf: []u8) !usize {
            return self.file.readAll(buf);
        }

        pub fn read(self: Reader, buf: []u8) !usize {
            return self.file.read(buf);
        }
    };

    pub const Writer = struct {
        file: File,

        pub fn writeAll(self: Writer, data: []const u8) !void {
            return self.file.writeAll(data);
        }

        pub fn write(self: Writer, data: []const u8) !usize {
            return self.file.writePartial(data);
        }

        pub fn print(self: Writer, comptime fmt_str: []const u8, args: anytype) !void {
            var buf: [4096]u8 = undefined;
            const result = std.fmt.bufPrint(&buf, fmt_str, args) catch return error.NoSpaceLeft;
            try self.writeAll(result);
        }
    };

    pub const Stat = struct {
        size: u64,
    };

    pub fn close(self: File) void {
        _ = c.close(self.handle);
    }

    pub fn writeAll(self: File, data: []const u8) !void {
        var written: usize = 0;
        while (written < data.len) {
            const n = c.write(self.handle, data[written..].ptr, data[written..].len);
            if (n < 0) return error.WriteError;
            if (n == 0) return error.WriteError;
            written += @intCast(n);
        }
    }

    pub fn readAll(self: File, buf: []u8) !usize {
        var total: usize = 0;
        while (total < buf.len) {
            const n = c.read(self.handle, buf[total..].ptr, buf[total..].len);
            if (n < 0) return error.ReadError;
            if (n == 0) break;
            total += @intCast(n);
        }
        return total;
    }

    pub fn read(self: File, buf: []u8) !usize {
        const n = c.read(self.handle, buf.ptr, buf.len);
        if (n < 0) return error.ReadError;
        return @intCast(n);
    }

    pub fn write(self: File, data: []const u8) !usize {
        const n = c.write(self.handle, data.ptr, data.len);
        if (n < 0) return error.WriteError;
        return @intCast(n);
    }

    pub fn stat(self: File) !Stat {
        const current = c.lseek(self.handle, 0, c.SEEK.CUR);
        if (current < 0) return error.StatError;
        const end = c.lseek(self.handle, 0, c.SEEK.END);
        if (end < 0) return error.StatError;
        _ = c.lseek(self.handle, current, c.SEEK.SET);
        return .{ .size = @intCast(end) };
    }

    pub fn seekTo(self: File, pos: u64) !void {
        _ = c.lseek(self.handle, @intCast(pos), c.SEEK.SET);
    }

    pub fn seekBy(self: File, delta: i64) !void {
        _ = c.lseek(self.handle, @intCast(delta), c.SEEK.CUR);
    }

    pub fn getPos(self: File) !u64 {
        const pos = c.lseek(self.handle, 0, c.SEEK.CUR);
        if (pos < 0) return error.SeekError;
        return @intCast(pos);
    }

    pub fn sync(self: File) !void {
        if (c.fsync(self.handle) != 0) return error.SyncError;
    }

    pub fn pread(self: File, buf: []u8, offset: u64) !usize {
        const n = c.pread(self.handle, buf.ptr, buf.len, @intCast(offset));
        if (n < 0) return error.ReadError;
        return @intCast(n);
    }
    pub fn setEndPos(self: File, pos: u64) !void {
        if (c.ftruncate(self.handle, @intCast(pos)) != 0) return error.TruncateError;
    }

    pub fn reader(self: File) Reader {
        return .{ .file = self };
    }

    pub fn writer(self: File) Writer {
        return .{ .file = self };
    }
};

// ═══════════════════════════════════════════════════════════════════════════════
// Dir / cwd() — drop-in for std.fs.cwd()
// ═══════════════════════════════════════════════════════════════════════════════

pub fn cwd() Dir {
    return .{};
}

pub const Dir = struct {
    pub fn makeDir(_: Dir, sub_path: []const u8) !void {
        var buf: [4096]u8 = undefined;
        const z = sliceToZ(&buf, sub_path) orelse return error.NameTooLong;
        const rc = c.mkdir(z, @as(c.mode_t, 0o755));
        if (rc != 0) {
            const e = std.posix.errno(rc);
            return switch (e) {
                .EXIST => error.PathAlreadyExists,
                else => error.MkdirError,
            };
        }
    }

    pub fn makePath(_: Dir, sub_path: []const u8) !void {
        var buf: [4096]u8 = undefined;
        if (sub_path.len >= buf.len) return error.NameTooLong;
        @memcpy(buf[0..sub_path.len], sub_path);
        buf[sub_path.len] = 0;

        var i: usize = 1;
        while (i <= sub_path.len) : (i += 1) {
            if (i == sub_path.len or buf[i] == '/') {
                const saved = buf[i];
                buf[i] = 0;
                _ = c.mkdir(@ptrCast(&buf), @as(c.mode_t, 0o755));
                buf[i] = saved;
            }
        }
    }

    pub fn deleteTree(_: Dir, sub_path: []const u8) !void {
        var cmd_buf: [4200]u8 = undefined;
        const cmd = std.fmt.bufPrint(&cmd_buf, "rm -rf '{s}'\x00", .{sub_path}) catch return error.NameTooLong;
        _ = system(@ptrCast(cmd.ptr));
    }

    pub fn deleteFile(_: Dir, sub_path: []const u8) !void {
        var buf: [4096]u8 = undefined;
        const z = sliceToZ(&buf, sub_path) orelse return error.NameTooLong;
        if (c.unlink(z) != 0) return error.DeleteError;
    }

    pub fn createFile(_: Dir, sub_path: []const u8, opts: anytype) !File {
        _ = opts;
        var buf: [4096]u8 = undefined;
        const z = sliceToZ(&buf, sub_path) orelse return error.NameTooLong;
        const fd = c.open(z, .{ .ACCMODE = .WRONLY, .CREAT = true, .TRUNC = true }, @as(c.mode_t, 0o644));
        if (fd < 0) return error.CreateFileError;
        return .{ .handle = fd };
    }

    pub fn openFile(_: Dir, sub_path: []const u8, opts: anytype) !File {
        _ = opts;
        var buf: [4096]u8 = undefined;
        const z = sliceToZ(&buf, sub_path) orelse return error.NameTooLong;
        const fd = c.open(z, .{ .ACCMODE = .RDWR }, @as(c.mode_t, 0));
        if (fd < 0) {
            const fd2 = c.open(z, .{ .ACCMODE = .RDONLY }, @as(c.mode_t, 0));
            if (fd2 < 0) return error.FileNotFound;
            return .{ .handle = fd2 };
        }
        return .{ .handle = fd };
    }

    pub fn openDir(_: Dir, sub_path: []const u8, opts: anytype) !IterableDir {
        _ = opts;
        var buf: [4096]u8 = undefined;
        const z = sliceToZ(&buf, sub_path) orelse return error.NameTooLong;
        const dp = c.opendir(z);
        if (dp == null) return error.FileNotFound;
        return .{ .dir_stream = dp.?, .path = sub_path };
    }

    pub fn rename(_: Dir, old: []const u8, new_path: []const u8) !void {
        var buf1: [4096]u8 = undefined;
        var buf2: [4096]u8 = undefined;
        const z1 = sliceToZ(&buf1, old) orelse return error.NameTooLong;
        const z2 = sliceToZ(&buf2, new_path) orelse return error.NameTooLong;
        if (c.rename(z1, z2) != 0) return error.RenameError;
    }

    pub fn readFileAlloc(_: Dir, allocator: std.mem.Allocator, sub_path: []const u8, max_size: usize) ![]u8 {
        var buf: [4096]u8 = undefined;
        const z = sliceToZ(&buf, sub_path) orelse return error.NameTooLong;
        const fd = c.open(z, .{ .ACCMODE = .RDONLY }, @as(c.mode_t, 0));
        if (fd < 0) return error.FileNotFound;
        defer _ = c.close(fd);

        const end = c.lseek(fd, 0, c.SEEK.END);
        if (end < 0) return error.StatError;
        _ = c.lseek(fd, 0, c.SEEK.SET);
        const size: usize = @intCast(end);
        if (size > max_size) return error.FileTooBig;

        const data = try allocator.alloc(u8, size);
        errdefer allocator.free(data);

        var total: usize = 0;
        while (total < size) {
            const n = c.read(fd, data[total..].ptr, data[total..].len);
            if (n < 0) return error.ReadError;
            if (n == 0) break;
            total += @intCast(n);
        }
        return data[0..total];
    }

    pub fn access(_: Dir, sub_path: []const u8, opts: anytype) !void {
        _ = opts;
        var buf: [4096]u8 = undefined;
        const z = sliceToZ(&buf, sub_path) orelse return error.NameTooLong;
        if (c.access(z, c.F_OK) != 0) return error.FileNotFound;
    }
};

pub fn openDirAbsolute(path: []const u8, opts: anytype) !IterableDir {
    _ = opts;
    var buf: [4096]u8 = undefined;
    const z = sliceToZ(&buf, path) orelse return error.NameTooLong;
    const dp = c.opendir(z);
    if (dp == null) return error.FileNotFound;
    return .{ .dir_stream = dp.?, .path = path };
}

pub fn createFileAbsolute(path: []const u8, opts: anytype) !File {
    return cwd().createFile(path, opts);
}

pub fn openFileAbsolute(path: []const u8, opts: anytype) !File {
    return cwd().openFile(path, opts);
}

pub const IterableDir = struct {
    dir_stream: *c.DIR,
    path: []const u8,

    pub fn close(self: *IterableDir) void {
        _ = c.closedir(self.dir_stream);
    }

    pub fn iterate(self: *IterableDir) Iterator {
        return .{ .dir_stream = self.dir_stream };
    }

    pub const Iterator = struct {
        dir_stream: *c.DIR,

        pub const Entry = struct {
            name: []const u8,
            kind: Kind,
        };

        pub const Kind = enum { file, directory, sym_link, unknown };

        pub fn next(self: *Iterator) !?Entry {
            while (true) {
                const entry = c.readdir(self.dir_stream);
                if (entry == null) return null;
                const name_ptr: [*:0]const u8 = @ptrCast(&entry.?.name);
                const name = std.mem.sliceTo(name_ptr, 0);
                if (std.mem.eql(u8, name, ".") or std.mem.eql(u8, name, "..")) continue;
                const kind: Kind = switch (entry.?.type) {
                    c.DT.REG => .file,
                    c.DT.DIR => .directory,
                    c.DT.LNK => .sym_link,
                    else => .unknown,
                };
                return .{ .name = name, .kind = kind };
            }
        }
    };
};

// ═══════════════════════════════════════════════════════════════════════════════
// Mutex — drop-in for std.Thread.Mutex
// ═══════════════════════════════════════════════════════════════════════════════

pub const Mutex = struct {
    inner: c.pthread_mutex_t = c.PTHREAD_MUTEX_INITIALIZER,

    pub fn lock(m: *Mutex) void {
        _ = c.pthread_mutex_lock(&m.inner);
    }

    pub fn unlock(m: *Mutex) void {
        _ = c.pthread_mutex_unlock(&m.inner);
    }

    pub fn tryLock(m: *Mutex) bool {
        return c.pthread_mutex_trylock(&m.inner) == 0;
    }
};

// ═══════════════════════════════════════════════════════════════════════════════
// RwLock — drop-in for std.Thread.RwLock
// ═══════════════════════════════════════════════════════════════════════════════

pub const RwLock = struct {
    inner: c.pthread_rwlock_t = std.mem.zeroes(c.pthread_rwlock_t),

    pub fn lockShared(rw: *RwLock) void {
        _ = c.pthread_rwlock_rdlock(&rw.inner);
    }

    pub fn unlockShared(rw: *RwLock) void {
        _ = c.pthread_rwlock_unlock(&rw.inner);
    }

    pub fn lock(rw: *RwLock) void {
        _ = c.pthread_rwlock_wrlock(&rw.inner);
    }

    pub fn unlock(rw: *RwLock) void {
        _ = c.pthread_rwlock_unlock(&rw.inner);
    }
};

// ═══════════════════════════════════════════════════════════════════════════════
// Condition — drop-in for std.Thread.Condition
// ═══════════════════════════════════════════════════════════════════════════════

pub const Condition = struct {
    inner: c.pthread_cond_t = c.PTHREAD_COND_INITIALIZER,

    pub fn wait(cond_var: *Condition, mutex: *Mutex) void {
        _ = c.pthread_cond_wait(&cond_var.inner, &mutex.inner);
    }

    pub fn signal(cond_var: *Condition) void {
        _ = c.pthread_cond_signal(&cond_var.inner);
    }

    pub fn broadcast(cond_var: *Condition) void {
        _ = c.pthread_cond_broadcast(&cond_var.inner);
    }
};

// ═══════════════════════════════════════════════════════════════════════════════
// sleep — drop-in for std.Thread.sleep
// ═══════════════════════════════════════════════════════════════════════════════

pub fn sleep(ns: u64) void {
    const ts = c.timespec{
        .sec = @intCast(ns / std.time.ns_per_s),
        .nsec = @intCast(ns % std.time.ns_per_s),
    };
    _ = c.nanosleep(&ts, null);
}

// ═══════════════════════════════════════════════════════════════════════════════
// Time — drop-in for std.time.milliTimestamp / nanoTimestamp / timestamp
// ═══════════════════════════════════════════════════════════════════════════════

pub fn milliTimestamp() i64 {
    var ts: c.timespec = undefined;
    _ = c.clock_gettime(.REALTIME, &ts);
    return @as(i64, ts.sec) * 1000 + @divTrunc(@as(i64, ts.nsec), 1_000_000);
}

pub fn nanoTimestamp() i128 {
    var ts: c.timespec = undefined;
    _ = c.clock_gettime(.REALTIME, &ts);
    return @as(i128, ts.sec) * std.time.ns_per_s + ts.nsec;
}

pub fn timestamp() i64 {
    var ts: c.timespec = undefined;
    _ = c.clock_gettime(.REALTIME, &ts);
    return ts.sec;
}

// ═══════════════════════════════════════════════════════════════════════════════
// FixedBufferStream — drop-in for std.io.fixedBufferStream
// ═══════════════════════════════════════════════════════════════════════════════

pub fn fixedBufferStream(buf: []u8) FixedBufferStream {
    return .{ .buffer = buf, .pos = 0 };
}

pub const FixedBufferStream = struct {
    buffer: []u8,
    pos: usize,

    pub fn writer(self: *FixedBufferStream) FbsWriter {
        return .{ .context = self };
    }

    pub fn reader(self: *FixedBufferStream) FbsReader {
        return .{ .context = self };
    }

    pub fn getWritten(self: *const FixedBufferStream) []const u8 {
        return self.buffer[0..self.pos];
    }

    pub fn reset(self: *FixedBufferStream) void {
        self.pos = 0;
    }

    pub const FbsWriter = struct {
        context: *FixedBufferStream,

        pub fn writeAll(self: FbsWriter, data: []const u8) !void {
            const fbs = self.context;
            if (fbs.pos + data.len > fbs.buffer.len) return error.NoSpaceLeft;
            @memcpy(fbs.buffer[fbs.pos..][0..data.len], data);
            fbs.pos += data.len;
        }

        pub fn writeByte(self: FbsWriter, byte: u8) !void {
            const fbs = self.context;
            if (fbs.pos >= fbs.buffer.len) return error.NoSpaceLeft;
            fbs.buffer[fbs.pos] = byte;
            fbs.pos += 1;
        }

        pub fn writeInt(self: FbsWriter, comptime T: type, value: T, comptime endian: std.builtin.Endian) !void {
            const bytes = std.mem.toBytes(if (endian == .little) value else @byteSwap(value));
            try self.writeAll(&bytes);
        }

        pub fn write(self: FbsWriter, data: []const u8) !usize {
            const fbs = self.context;
            const avail = fbs.buffer.len - fbs.pos;
            const n = @min(data.len, avail);
            @memcpy(fbs.buffer[fbs.pos..][0..n], data[0..n]);
            fbs.pos += n;
            return n;
        }

        pub fn print(self: FbsWriter, comptime fmt_str: []const u8, args: anytype) !void {
            var tmp: [8192]u8 = undefined;
            const result = std.fmt.bufPrint(&tmp, fmt_str, args) catch return error.NoSpaceLeft;
            try self.writeAll(result);
        }
    };

    pub const FbsReader = struct {
        context: *FixedBufferStream,

        pub fn readAll(self: FbsReader, buf: []u8) !usize {
            const fbs = self.context;
            const avail = fbs.buffer.len - fbs.pos;
            const n = @min(buf.len, avail);
            @memcpy(buf[0..n], fbs.buffer[fbs.pos..][0..n]);
            fbs.pos += n;
            return n;
        }
    };
};

pub fn constFixedBufferStream(buf: []const u8) ConstFixedBufferStream {
    return .{ .buffer = buf, .pos = 0 };
}

pub const ConstFixedBufferStream = struct {
    buffer: []const u8,
    pos: usize,

    pub fn reader(self: *ConstFixedBufferStream) ConstFbsReader {
        return .{ .context = self };
    }

    pub const ConstFbsReader = struct {
        context: *ConstFixedBufferStream,

        pub fn readAll(self: ConstFbsReader, buf: []u8) !usize {
            const fbs = self.context;
            const avail = fbs.buffer.len - fbs.pos;
            const n = @min(buf.len, avail);
            @memcpy(buf[0..n], fbs.buffer[fbs.pos..][0..n]);
            fbs.pos += n;
            return n;
        }

        pub fn readByte(self: ConstFbsReader) !u8 {
            const fbs = self.context;
            if (fbs.pos >= fbs.buffer.len) return error.EndOfStream;
            const b = fbs.buffer[fbs.pos];
            fbs.pos += 1;
            return b;
        }

        pub fn readInt(self: ConstFbsReader, comptime T: type, comptime endian: std.builtin.Endian) !T {
            const bytes_needed = @sizeOf(T);
            const fbs = self.context;
            if (fbs.pos + bytes_needed > fbs.buffer.len) return error.EndOfStream;
            var bytes: [@sizeOf(T)]u8 = undefined;
            @memcpy(&bytes, fbs.buffer[fbs.pos..][0..bytes_needed]);
            fbs.pos += bytes_needed;
            const raw = std.mem.bytesToValue(T, &bytes);
            return if (endian == .little) raw else @byteSwap(raw);
        }
    };
};

// ═══════════════════════════════════════════════════════════════════════════════
// BufferedReader — drop-in for std.io.BufferedReader / std.io.bufferedReader
// ═══════════════════════════════════════════════════════════════════════════════

pub fn GenericBufferedReader(comptime buf_size: usize, comptime ReaderType: type) type {
    return struct {
        unbuffered: ReaderType,
        buf: [buf_size]u8 = undefined,
        start: usize = 0,
        end: usize = 0,

        const Self = @This();

        pub fn reader(self: *Self) InnerReader {
            return .{ .context = self };
        }

        pub const InnerReader = struct {
            context: *Self,

            pub fn readAll(self_inner: InnerReader, out: []u8) !usize {
                var total: usize = 0;
                while (total < out.len) {
                    const br = self_inner.context;
                    if (br.start < br.end) {
                        const buffered = br.end - br.start;
                        const n = @min(out.len - total, buffered);
                        @memcpy(out[total..][0..n], br.buf[br.start..][0..n]);
                        br.start += n;
                        total += n;
                        continue;
                    }
                    const n = try br.unbuffered.readAll(br.buf[0..]);
                    if (n == 0) break;
                    br.start = 0;
                    br.end = n;
                }
                return total;
            }
        };
    };
}

pub fn bufferedReader(underlying: anytype) GenericBufferedReader(4096, @TypeOf(underlying)) {
    return .{ .unbuffered = underlying };
}

// ═══════════════════════════════════════════════════════════════════════════════
// Crypto helper — random bytes without Io
// ═══════════════════════════════════════════════════════════════════════════════

extern "c" fn arc4random_buf(buf: *anyopaque, nbytes: usize) void;
extern "c" fn system(command: [*:0]const u8) c_int;

pub fn randomBytes(buf: []u8) void {
    arc4random_buf(buf.ptr, buf.len);
}

// ═══════════════════════════════════════════════════════════════════════════════
// fmt.format replacement — writes to any writer with .writeAll
// ═══════════════════════════════════════════════════════════════════════════════

pub fn format(w: anytype, comptime fmt_str: []const u8, args: anytype) !void {
    var tmp: [8192]u8 = undefined;
    const result = std.fmt.bufPrint(&tmp, fmt_str, args) catch return error.NoSpaceLeft;
    try w.writeAll(result);
}



// ═══════════════════════════════════════════════════════════════════════════════
// TCP Networking — drop-in for std.net (removed in 0.16)
// ═══════════════════════════════════════════════════════════════════════════════

pub const net = struct {
    pub const Address = struct {
        family: u8,
        port: u16,
        addr: u32,

        pub fn parseIp(ip: []const u8, port: u16) !Address {
            return .{ .family = 2, .port = port, .addr = try parseIpv4(ip) };
        }

        pub fn listen(self: Address, opts: anytype) !Server {
            const fd = c.socket(2, 1, 0); // AF_INET=2, SOCK_STREAM=1
            if (fd < 0) return error.SocketError;

            // SO_REUSEADDR
            if (opts.reuse_address) {
                var opt_val: c_int = 1;
                _ = c.setsockopt(fd, 0xFFFF, 0x0004, @ptrCast(&opt_val), @sizeOf(c_int)); // SOL_SOCKET, SO_REUSEADDR
            }

            // Bind
            var addr: extern struct { family: u16, port: u16 align(1), addr: u32 align(1), zero: [8]u8 align(1) } = .{
                .family = 2, // AF_INET
                .port = std.mem.nativeToBig(u16, self.port),
                .addr = std.mem.nativeToBig(u32, self.addr),
                .zero = std.mem.zeroes([8]u8),
            };
            if (c.bind(fd, @ptrCast(&addr), @sizeOf(@TypeOf(addr))) != 0) {
                _ = c.close(fd);
                return error.BindError;
            }

            // Listen
            const backlog: c_int = if (@hasField(@TypeOf(opts), "kernel_backlog")) @intCast(opts.kernel_backlog) else 128;
            if (c.listen(fd, backlog) != 0) {
                _ = c.close(fd);
                return error.ListenError;
            }

            return .{ .fd = fd };
        }

        pub fn initUnix(path: anytype) !Address {
            _ = path;
            return .{ .family = 1, .port = 0, .addr = 0 };
        }

        fn parseIpv4(ip: []const u8) !u32 {
            if (std.mem.eql(u8, ip, "localhost")) return 0x7f000001;
            var parts = std.mem.splitScalar(u8, ip, '.');
            var out: u32 = 0;
            var count: u8 = 0;
            while (parts.next()) |part| {
                if (count == 4 or part.len == 0) return error.InvalidIpAddress;
                const octet = try std.fmt.parseInt(u8, part, 10);
                out = (out << 8) | octet;
                count += 1;
            }
            if (count != 4) return error.InvalidIpAddress;
            return out;
        }
    };

    pub const Stream = struct {
        handle: posix.fd_t,

        pub fn read(self: Stream, buf: []u8) !usize {
            const n = c.read(self.handle, buf.ptr, buf.len);
            if (n < 0) return error.ReadError;
            return @intCast(n);
        }

        pub fn readAll(self: Stream, buf: []u8) !usize {
            var total: usize = 0;
            while (total < buf.len) {
                const n = try self.read(buf[total..]);
                if (n == 0) break;
                total += n;
            }
            return total;
        }

        pub fn write(self: Stream, data: []const u8) !usize {
            const n = c.write(self.handle, data.ptr, data.len);
            if (n < 0) return error.WriteError;
            return @intCast(n);
        }

        pub fn writeAll(self: Stream, data: []const u8) !void {
            var written: usize = 0;
            while (written < data.len) {
                const n = try self.write(data[written..]);
                if (n == 0) return error.WriteError;
                written += n;
            }
        }

        pub fn close(self: Stream) void {
            _ = c.close(self.handle);
        }
    };

    pub fn tcpConnectToAddress(address: Address) !Stream {
        const fd = c.socket(2, 1, 0);
        if (fd < 0) return error.SocketError;
        errdefer _ = c.close(fd);

        var sockaddr: extern struct { family: u16, port: u16 align(1), addr: u32 align(1), zero: [8]u8 align(1) } = .{
            .family = 2,
            .port = std.mem.nativeToBig(u16, address.port),
            .addr = std.mem.nativeToBig(u32, address.addr),
            .zero = std.mem.zeroes([8]u8),
        };
        if (c.connect(fd, @ptrCast(&sockaddr), @sizeOf(@TypeOf(sockaddr))) != 0) {
            return error.ConnectError;
        }
        return .{ .handle = fd };
    }

    pub const Server = struct {
        fd: posix.fd_t,

        pub const Connection = struct {
            stream: Stream,
            address: Address,
        };

        pub fn accept(self: Server) !Connection {
            const client_fd = c.accept(self.fd, null, null);
            if (client_fd < 0) return error.AcceptError;
            return .{
                .stream = .{ .handle = client_fd },
                .address = .{ .family = 2, .port = 0, .addr = 0 },
            };
        }

        pub fn deinit(self: *Server) void {
            _ = c.close(self.fd);
        }
    };
};

// ═══════════════════════════════════════════════════════════════════════════════
// ArrayListWriter — drop-in for ArrayList(u8).writer() (removed in 0.16)
// ═══════════════════════════════════════════════════════════════════════════════

pub const ArrayListWriter = struct {
    list: *std.ArrayList(u8),
    gpa: std.mem.Allocator,

    pub fn writeAll(self: ArrayListWriter, data: []const u8) !void {
        try self.list.appendSlice(self.gpa, data);
    }

    pub fn writeByte(self: ArrayListWriter, byte: u8) !void {
        try self.list.append(self.gpa, byte);
    }

    pub fn print(self: ArrayListWriter, comptime fmt_str: []const u8, args: anytype) !void {
        var tmp: [8192]u8 = undefined;
        const result = std.fmt.bufPrint(&tmp, fmt_str, args) catch return error.NoSpaceLeft;
        try self.writeAll(result);
    }
};

pub fn arrayListWriter(list: *std.ArrayList(u8), gpa: std.mem.Allocator) ArrayListWriter {
    return .{ .list = list, .gpa = gpa };
}

// ═══════════════════════════════════════════════════════════════════════════════
// Args — drop-in for std.process.argsAlloc (0.15)
// ═══════════════════════════════════════════════════════════════════════════════

pub fn argsAlloc(allocator: std.mem.Allocator, init: std.process.Init) ![][:0]const u8 {
    // Count args first
    var count: usize = 0;
    {
        var it = std.process.Args.Iterator.init(init.minimal.args);
        while (it.next()) |_| count += 1;
    }
    const args = try allocator.alloc([:0]const u8, count);
    var it = std.process.Args.Iterator.init(init.minimal.args);
    var i: usize = 0;
    while (it.next()) |arg| : (i += 1) {
        args[i] = arg;
    }
    return args;
}

pub fn argsFree(allocator: std.mem.Allocator, args: [][:0]const u8) void {
    allocator.free(args);
}

// ═══════════════════════════════════════════════════════════════════════════════
// Helpers
// ═══════════════════════════════════════════════════════════════════════════════

fn sliceToZ(buf: *[4096]u8, s: []const u8) ?[*:0]const u8 {
    if (s.len >= buf.len) return null;
    @memcpy(buf[0..s.len], s);
    buf[s.len] = 0;
    return @ptrCast(buf);
}
