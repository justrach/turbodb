const std = @import("std");
const compat = @import("compat");

extern "c" fn socket(domain: c_uint, sock_type: c_uint, protocol: c_uint) c_int;
extern "c" fn close(fd: std.c.fd_t) c_int;

const OUTER_HDR: usize = 5;
const WIRE2_HDR: usize = 16;
const FRAME_HDR: usize = OUTER_HDR + WIRE2_HDR;
const MAX_SERVER_RESPONSE: usize = 4 * 1024 * 1024;
const MAX_WIRE2_FRAME: usize = 64 * 1024 * 1024 + OUTER_HDR;

const OP_WIRE2: u8 = 0x20;
const WIRE2_VERSION: u8 = 2;
const WIRE2_OP_HELLO: u8 = 0x00;
const WIRE2_OP_GET: u8 = 0x02;
const WIRE2_OP_PUT: u8 = 0x03;
const WIRE2_OP_BATCH_GET: u8 = 0x06;
const WIRE2_OP_BULK_INSERT: u8 = 0x07;
const WIRE2_OP_AUTH: u8 = 0x0A;

const STATUS_OK: u8 = 0x00;
const STATUS_NOT_FOUND: u8 = 0x01;

const Config = struct {
    host: []const u8 = "127.0.0.1",
    host_owned: bool = false,
    port: u16 = 27017,
    collection: []const u8 = "wire2_perf",
    collection_owned: bool = false,
    rows: usize = 1_000_000,
    batch_size: usize = 10_000,
    get_rounds: usize = 100,
    get_batch_size: usize = 0,
    pipeline_window: usize = 4,
    auth_key: []const u8 = "",
    auth_key_owned: bool = false,
    output: []const u8 = "",
    output_owned: bool = false,

    fn deinit(self: *Config, allocator: std.mem.Allocator) void {
        if (self.host_owned) allocator.free(self.host);
        if (self.collection_owned) allocator.free(self.collection);
        if (self.auth_key_owned) allocator.free(self.auth_key);
        if (self.output_owned) allocator.free(self.output);
    }
};

const HelloInfo = struct {
    max_frame: u32 = 0,
    max_response: u32 = 0,
    features: u32 = 0,
};

const Wire2Response = struct {
    request_id: u64,
    op: u8,
    status: u8,
    body: []const u8,
};

const Wire2Stats = struct {
    inserted: u64 = 0,
    updated: u64 = 0,
    deleted: u64 = 0,
    missing: u64 = 0,
    errors: u64 = 0,
    bytes: u64 = 0,
};

const PhaseMetrics = struct {
    operations: u64 = 0,
    seconds: f64 = 0,
    per_second: f64 = 0,
    bytes: u64 = 0,
    batches: u64 = 0,
    inserted: u64 = 0,
    found: u64 = 0,
    missing: u64 = 0,
    errors: u64 = 0,
};

const BenchResult = struct {
    hello: HelloInfo,
    probe_seconds: f64,
    bulk: PhaseMetrics,
    batch_get: PhaseMetrics,
};

pub fn main(init: std.process.Init) !void {
    const allocator = std.heap.smp_allocator;
    const args = try compat.argsAlloc(allocator, init);
    defer compat.argsFree(allocator, args);

    var config = try parseArgs(args, allocator);
    defer config.deinit(allocator);
    validateConfig(config) catch |err| {
        usage();
        return err;
    };

    const result = try runBenchmark(allocator, config);
    try emitResult(config, result);
}

fn parseArgs(args: [][:0]const u8, allocator: std.mem.Allocator) !Config {
    var config: Config = .{};
    var i: usize = 1;
    while (i < args.len) : (i += 1) {
        const arg = args[i];
        if (std.mem.eql(u8, arg, "--host")) {
            i += 1;
            config.host = try dupArg(args, i, allocator);
            config.host_owned = true;
        } else if (std.mem.eql(u8, arg, "--port")) {
            i += 1;
            config.port = try parseArg(u16, args, i);
        } else if (std.mem.eql(u8, arg, "--collection")) {
            i += 1;
            config.collection = try dupArg(args, i, allocator);
            config.collection_owned = true;
        } else if (std.mem.eql(u8, arg, "--rows")) {
            i += 1;
            config.rows = try parseArg(usize, args, i);
        } else if (std.mem.eql(u8, arg, "--batch-size")) {
            i += 1;
            config.batch_size = try parseArg(usize, args, i);
        } else if (std.mem.eql(u8, arg, "--get-rounds")) {
            i += 1;
            config.get_rounds = try parseArg(usize, args, i);
        } else if (std.mem.eql(u8, arg, "--get-batch-size")) {
            i += 1;
            config.get_batch_size = try parseArg(usize, args, i);
        } else if (std.mem.eql(u8, arg, "--pipeline-window")) {
            i += 1;
            config.pipeline_window = try parseArg(usize, args, i);
        } else if (std.mem.eql(u8, arg, "--auth-key")) {
            i += 1;
            config.auth_key = try dupArg(args, i, allocator);
            config.auth_key_owned = true;
        } else if (std.mem.eql(u8, arg, "--output")) {
            i += 1;
            config.output = try dupArg(args, i, allocator);
            config.output_owned = true;
        } else if (std.mem.eql(u8, arg, "--help") or std.mem.eql(u8, arg, "-h")) {
            usage();
            std.process.exit(0);
        } else {
            return error.InvalidArgument;
        }
    }
    if (config.get_batch_size == 0) config.get_batch_size = config.batch_size;
    return config;
}

fn dupArg(args: [][:0]const u8, i: usize, allocator: std.mem.Allocator) ![]const u8 {
    if (i >= args.len) return error.InvalidArgument;
    return allocator.dupe(u8, args[i]);
}

fn parseArg(comptime T: type, args: [][:0]const u8, i: usize) !T {
    if (i >= args.len) return error.InvalidArgument;
    return std.fmt.parseInt(T, args[i], 10);
}

fn validateConfig(config: Config) !void {
    if (config.host.len == 0 or config.collection.len == 0) return error.InvalidArgument;
    if (config.rows == 0 or config.batch_size == 0 or config.get_rounds == 0 or config.get_batch_size == 0) return error.InvalidArgument;
    if (config.pipeline_window == 0) return error.InvalidArgument;
    if (config.batch_size > std.math.maxInt(u32) or config.get_batch_size > std.math.maxInt(u32)) return error.InvalidArgument;
    if (config.collection.len > std.math.maxInt(u16) or config.auth_key.len > std.math.maxInt(u16)) return error.InvalidArgument;
    if (config.get_batch_size > config.rows) return error.InvalidArgument;
}

fn usage() void {
    std.debug.print(
        \\usage: wire2-perf-client [options]
        \\  --host IP              TurboDB wire host (default 127.0.0.1)
        \\  --port PORT            TurboDB wire port (default 27017)
        \\  --collection NAME      Collection to benchmark (default wire2_perf)
        \\  --rows N               Rows to bulk insert (default 1000000)
        \\  --batch-size N         Rows per bulk insert request (default 10000)
        \\  --get-rounds N         Batch-get repetitions (default 100)
        \\  --get-batch-size N     Keys per batch-get request (default batch-size)
        \\  --pipeline-window N    Batch-get requests in flight (default 4)
        \\  --auth-key KEY         Optional wire2 auth key
        \\  --output PATH          Optional JSON output path
        \\
    , .{});
}

fn runBenchmark(allocator: std.mem.Allocator, config: Config) !BenchResult {
    const request_capacity = try requiredRequestCapacity(config);
    const request_buf = try allocator.alloc(u8, request_capacity);
    defer allocator.free(request_buf);
    const response_buf = try allocator.alloc(u8, MAX_SERVER_RESPONSE);
    defer allocator.free(response_buf);

    const fd = try connectTcp(config.host, config.port);
    defer _ = close(fd);

    var request_id: u64 = 1;
    const hello = try doHello(fd, request_buf, response_buf, &request_id);
    if (config.auth_key.len > 0) try doAuth(fd, request_buf, response_buf, &request_id, config.auth_key);

    const probe_start = compat.nanoTimestamp();
    try doProbe(fd, request_buf, response_buf, &request_id, config.collection);
    const probe_seconds = nsToSeconds(compat.nanoTimestamp() - probe_start);

    const bulk = try doBulkInsert(fd, request_buf, response_buf, &request_id, config);
    const batch_get = try doBatchGet(fd, request_buf, response_buf, &request_id, config);

    return .{
        .hello = hello,
        .probe_seconds = probe_seconds,
        .bulk = bulk,
        .batch_get = batch_get,
    };
}

fn requiredRequestCapacity(config: Config) !usize {
    const row_width: usize = 6 + 32 + 96;
    const key_width: usize = 2 + 32;
    const bulk_body = 2 + config.collection.len + 4 + config.batch_size * row_width;
    const get_body = 2 + config.collection.len + 4 + config.get_batch_size * key_width;
    const need = FRAME_HDR + @max(bulk_body, get_body);
    if (need > MAX_WIRE2_FRAME) return error.FrameTooLarge;
    return @max(need, 64 * 1024);
}

fn doHello(fd: std.c.fd_t, request_buf: []u8, response_buf: []u8, request_id: *u64) !HelloInfo {
    const resp = try callWire2(fd, request_buf, response_buf, request_id, WIRE2_OP_HELLO, 0);
    try expectStatus(resp, WIRE2_OP_HELLO, STATUS_OK);
    if (resp.body.len < 12) return error.BadResponse;
    return .{
        .max_frame = rdU32LE(resp.body[0..4]),
        .max_response = rdU32LE(resp.body[4..8]),
        .features = rdU32LE(resp.body[8..12]),
    };
}

fn doAuth(fd: std.c.fd_t, request_buf: []u8, response_buf: []u8, request_id: *u64, auth_key: []const u8) !void {
    if (auth_key.len > std.math.maxInt(u16)) return error.InvalidArgument;
    const body = requestBody(request_buf);
    if (body.len < 2 + auth_key.len) return error.FrameTooLarge;
    wrU16LE(body[0..2], @intCast(auth_key.len));
    @memcpy(body[2..][0..auth_key.len], auth_key);
    const resp = try callWire2(fd, request_buf, response_buf, request_id, WIRE2_OP_AUTH, 2 + auth_key.len);
    try expectStatus(resp, WIRE2_OP_AUTH, STATUS_OK);
}

fn doProbe(fd: std.c.fd_t, request_buf: []u8, response_buf: []u8, request_id: *u64, collection: []const u8) !void {
    const key = "probe-native";
    const value = "{\"probe\":\"wire2-zig\"}";
    const body_len = encodeKeyValue(requestBody(request_buf), collection, key, value) catch return error.FrameTooLarge;
    var resp = try callWire2(fd, request_buf, response_buf, request_id, WIRE2_OP_PUT, body_len);
    try expectStatus(resp, WIRE2_OP_PUT, STATUS_OK);

    const get_len = encodeKey(requestBody(request_buf), collection, key) catch return error.FrameTooLarge;
    resp = try callWire2(fd, request_buf, response_buf, request_id, WIRE2_OP_GET, get_len);
    try expectStatus(resp, WIRE2_OP_GET, STATUS_OK);
    if (resp.body.len < 13) return error.BadResponse;
    const value_len: usize = rdU32LE(resp.body[9..13]);
    if (resp.body.len != 13 + value_len) return error.BadResponse;
    if (!std.mem.eql(u8, resp.body[13..], value)) return error.BadResponse;
}

fn doBulkInsert(fd: std.c.fd_t, request_buf: []u8, response_buf: []u8, request_id: *u64, config: Config) !PhaseMetrics {
    var metrics: PhaseMetrics = .{};
    const started = compat.nanoTimestamp();

    var base: usize = 0;
    while (base < config.rows) {
        const count = @min(config.batch_size, config.rows - base);
        const body_len = try encodeBulkInsertBody(requestBody(request_buf), config.collection, base, count);
        const resp = try callWire2(fd, request_buf, response_buf, request_id, WIRE2_OP_BULK_INSERT, body_len);
        try expectStatus(resp, WIRE2_OP_BULK_INSERT, STATUS_OK);
        const stats = try parseStats(resp.body);
        metrics.inserted += stats.inserted;
        metrics.errors += stats.errors;
        metrics.bytes += stats.bytes;
        metrics.operations += count;
        metrics.batches += 1;
        if (stats.errors != 0) return error.ServerErrors;
        base += count;
    }

    metrics.seconds = nsToSeconds(compat.nanoTimestamp() - started);
    metrics.per_second = rate(metrics.operations, metrics.seconds);
    return metrics;
}

fn doBatchGet(fd: std.c.fd_t, request_buf: []u8, response_buf: []u8, request_id: *u64, config: Config) !PhaseMetrics {
    var metrics: PhaseMetrics = .{};
    const started = compat.nanoTimestamp();

    const first_request_id = request_id.*;
    var sent_rounds: usize = 0;
    var received_rounds: usize = 0;
    while (received_rounds < config.get_rounds) {
        while (sent_rounds < config.get_rounds and sent_rounds - received_rounds < config.pipeline_window) : (sent_rounds += 1) {
            const base = (sent_rounds * config.get_batch_size) % config.rows;
            const body_len = try encodeBatchGetBody(requestBody(request_buf), config.collection, base, config.get_batch_size, config.rows);
            const id = request_id.*;
            request_id.* += 1;
            try sendAll(fd, finishRequest(request_buf, id, WIRE2_OP_BATCH_GET, body_len));
        }

        const resp = try readWire2Response(fd, response_buf);
        const expected_id = first_request_id + @as(u64, @intCast(received_rounds));
        if (resp.request_id != expected_id or resp.op != WIRE2_OP_BATCH_GET) return error.BadResponse;
        try expectStatus(resp, WIRE2_OP_BATCH_GET, STATUS_OK);
        const counts = try parseBatchGet(resp.body);
        metrics.found += counts.found;
        metrics.missing += counts.missing;
        metrics.operations += config.get_batch_size;
        metrics.batches += 1;
        if (counts.missing != 0) return error.BadResponse;
        received_rounds += 1;
    }

    metrics.seconds = nsToSeconds(compat.nanoTimestamp() - started);
    metrics.per_second = rate(metrics.operations, metrics.seconds);
    return metrics;
}

fn callWire2(fd: std.c.fd_t, request_buf: []u8, response_buf: []u8, request_id: *u64, op: u8, body_len: usize) !Wire2Response {
    const id = request_id.*;
    request_id.* += 1;
    const frame = finishRequest(request_buf, id, op, body_len);
    try sendAll(fd, frame);
    const resp = try readWire2Response(fd, response_buf);
    if (resp.request_id != id or resp.op != op) return error.BadResponse;
    return resp;
}

fn requestBody(request_buf: []u8) []u8 {
    return request_buf[FRAME_HDR..];
}

fn finishRequest(request_buf: []u8, request_id: u64, op: u8, body_len: usize) []const u8 {
    const total_len = FRAME_HDR + body_len;
    wrU32BE(request_buf[0..4], @intCast(total_len));
    request_buf[4] = OP_WIRE2;
    request_buf[5] = WIRE2_VERSION;
    request_buf[6] = 0;
    wrU64LE(request_buf[7..15], request_id);
    request_buf[15] = op;
    request_buf[16] = 0;
    wrU32LE(request_buf[17..21], @intCast(body_len));
    return request_buf[0..total_len];
}

fn encodeBulkInsertBody(out: []u8, collection: []const u8, base: usize, count: usize) !usize {
    var pos = try encodeCollectionHeader(out, collection, count);
    for (0..count) |i| {
        const n = base + i;
        if (pos + 6 + 32 + 96 > out.len) return error.FrameTooLarge;
        const key = try std.fmt.bufPrint(out[pos + 6 ..][0..32], "k-{d:0>10}", .{n});
        const value = try std.fmt.bufPrint(out[pos + 6 + key.len ..][0..96], "{{\"n\":{d},\"v\":\"wire2\"}}", .{n});
        wrU16LE(out[pos..][0..2], @intCast(key.len));
        wrU32LE(out[pos + 2 ..][0..4], @intCast(value.len));
        pos += 6 + key.len + value.len;
    }
    return pos;
}

fn encodeBatchGetBody(out: []u8, collection: []const u8, base: usize, count: usize, rows: usize) !usize {
    var pos = try encodeCollectionHeader(out, collection, count);
    for (0..count) |i| {
        const n = (base + i) % rows;
        if (pos + 2 + 32 > out.len) return error.FrameTooLarge;
        const key = try std.fmt.bufPrint(out[pos + 2 ..][0..32], "k-{d:0>10}", .{n});
        wrU16LE(out[pos..][0..2], @intCast(key.len));
        pos += 2 + key.len;
    }
    return pos;
}

fn encodeCollectionHeader(out: []u8, collection: []const u8, count: usize) !usize {
    if (collection.len > std.math.maxInt(u16) or count > std.math.maxInt(u32)) return error.InvalidArgument;
    if (out.len < 2 + collection.len + 4) return error.FrameTooLarge;
    wrU16LE(out[0..2], @intCast(collection.len));
    @memcpy(out[2..][0..collection.len], collection);
    wrU32LE(out[2 + collection.len ..][0..4], @intCast(count));
    return 2 + collection.len + 4;
}

fn encodeKey(out: []u8, collection: []const u8, key: []const u8) !usize {
    if (collection.len > std.math.maxInt(u16) or key.len > std.math.maxInt(u16)) return error.InvalidArgument;
    const need = 2 + collection.len + 2 + key.len;
    if (out.len < need) return error.FrameTooLarge;
    wrU16LE(out[0..2], @intCast(collection.len));
    @memcpy(out[2..][0..collection.len], collection);
    const key_off = 2 + collection.len;
    wrU16LE(out[key_off..][0..2], @intCast(key.len));
    @memcpy(out[key_off + 2 ..][0..key.len], key);
    return need;
}

fn encodeKeyValue(out: []u8, collection: []const u8, key: []const u8, value: []const u8) !usize {
    if (collection.len > std.math.maxInt(u16) or key.len > std.math.maxInt(u16)) return error.InvalidArgument;
    if (value.len > std.math.maxInt(u32)) return error.InvalidArgument;
    const need = 2 + collection.len + 2 + key.len + 4 + value.len;
    if (out.len < need) return error.FrameTooLarge;
    wrU16LE(out[0..2], @intCast(collection.len));
    @memcpy(out[2..][0..collection.len], collection);
    const key_off = 2 + collection.len;
    wrU16LE(out[key_off..][0..2], @intCast(key.len));
    @memcpy(out[key_off + 2 ..][0..key.len], key);
    const val_off = key_off + 2 + key.len;
    wrU32LE(out[val_off..][0..4], @intCast(value.len));
    @memcpy(out[val_off + 4 ..][0..value.len], value);
    return need;
}

fn parseStats(body: []const u8) !Wire2Stats {
    if (body.len != 28) return error.BadResponse;
    return .{
        .inserted = rdU32LE(body[0..4]),
        .updated = rdU32LE(body[4..8]),
        .deleted = rdU32LE(body[8..12]),
        .missing = rdU32LE(body[12..16]),
        .errors = rdU32LE(body[16..20]),
        .bytes = rdU64LE(body[20..28]),
    };
}

fn parseBatchGet(body: []const u8) !struct { found: u64, missing: u64 } {
    if (body.len < 4) return error.BadResponse;
    const count = rdU32LE(body[0..4]);
    var pos: usize = 4;
    var found: u64 = 0;
    var missing: u64 = 0;
    for (0..count) |_| {
        if (pos + 14 > body.len) return error.BadResponse;
        const status = body[pos];
        const value_len: usize = rdU32LE(body[pos + 10 ..][0..4]);
        pos += 14;
        if (pos + value_len > body.len) return error.BadResponse;
        pos += value_len;
        if (status == STATUS_OK) {
            found += 1;
        } else if (status == STATUS_NOT_FOUND) {
            missing += 1;
        } else {
            return error.BadResponse;
        }
    }
    if (pos != body.len) return error.BadResponse;
    return .{ .found = found, .missing = missing };
}

fn expectStatus(resp: Wire2Response, op: u8, status: u8) !void {
    if (resp.op != op) return error.BadResponse;
    if (resp.status != status) return error.BadStatus;
}

fn readWire2Response(fd: std.c.fd_t, response_buf: []u8) !Wire2Response {
    if (response_buf.len < FRAME_HDR) return error.BadResponse;
    try recvExact(fd, response_buf[0..OUTER_HDR]);
    const frame_len: usize = rdU32BE(response_buf[0..4]);
    if (frame_len < FRAME_HDR or frame_len > response_buf.len) return error.BadResponse;
    if (response_buf[4] != OP_WIRE2) return error.BadResponse;
    try recvExact(fd, response_buf[OUTER_HDR..frame_len]);
    if (response_buf[5] != WIRE2_VERSION) return error.BadResponse;
    const body_len: usize = rdU32LE(response_buf[17..21]);
    if (FRAME_HDR + body_len != frame_len) return error.BadResponse;
    return .{
        .request_id = rdU64LE(response_buf[7..15]),
        .op = response_buf[15],
        .status = response_buf[16],
        .body = response_buf[FRAME_HDR..frame_len],
    };
}

fn connectTcp(host: []const u8, port: u16) !std.c.fd_t {
    const fd = socket(std.c.AF.INET, std.c.SOCK.STREAM, 0);
    switch (std.c.errno(fd)) {
        .SUCCESS => {},
        else => |err| return errnoError(err),
    }
    errdefer _ = close(fd);

    var addr: std.c.sockaddr.in = .{
        .port = std.mem.nativeToBig(u16, port),
        .addr = @bitCast(try parseIpv4(host)),
    };
    const rc = std.c.connect(fd, @ptrCast(&addr), @sizeOf(std.c.sockaddr.in));
    switch (std.c.errno(rc)) {
        .SUCCESS => {},
        else => |err| return errnoError(err),
    }
    return fd;
}

fn parseIpv4(ip: []const u8) ![4]u8 {
    var out: [4]u8 = undefined;
    var it = std.mem.splitScalar(u8, ip, '.');
    var i: usize = 0;
    while (it.next()) |part| {
        if (i >= out.len or part.len == 0) return error.InvalidIpAddress;
        out[i] = try std.fmt.parseInt(u8, part, 10);
        i += 1;
    }
    if (i != out.len) return error.InvalidIpAddress;
    return out;
}

fn recvExact(fd: std.c.fd_t, buf: []u8) !void {
    var got: usize = 0;
    while (got < buf.len) {
        const n = std.c.recv(fd, buf[got..].ptr, buf.len - got, 0);
        switch (std.c.errno(n)) {
            .SUCCESS => {
                if (n == 0) return error.ConnectionClosed;
                got += @intCast(n);
            },
            .INTR => continue,
            .AGAIN => continue,
            .CONNRESET => return error.ConnectionClosed,
            else => |err| return errnoError(err),
        }
    }
}

fn sendAll(fd: std.c.fd_t, bytes: []const u8) !void {
    var written: usize = 0;
    while (written < bytes.len) {
        const flags = if (@hasDecl(std.c.MSG, "NOSIGNAL")) std.c.MSG.NOSIGNAL else 0;
        const n = std.c.send(fd, bytes[written..].ptr, bytes.len - written, flags);
        switch (std.c.errno(n)) {
            .SUCCESS => {
                if (n == 0) return error.ConnectionClosed;
                written += @intCast(n);
            },
            .INTR => continue,
            .AGAIN => continue,
            else => |err| return errnoError(err),
        }
    }
}

fn emitResult(config: Config, result: BenchResult) !void {
    var buf: [8192]u8 = undefined;
    const json = try std.fmt.bufPrint(&buf,
        \\{{
        \\  "benchmark": "wire2_perf_client_zig",
        \\  "timestamp_ms": {d},
        \\  "config": {{
        \\    "host": "{s}",
        \\    "port": {d},
        \\    "collection": "{s}",
        \\    "rows": {d},
        \\    "batch_size": {d},
        \\    "get_rounds": {d},
        \\    "get_batch_size": {d},
        \\    "pipeline_window": {d},
        \\    "auth": {s}
        \\  }},
        \\  "hello": {{
        \\    "max_frame": {d},
        \\    "max_response": {d},
        \\    "features": {d}
        \\  }},
        \\  "probe": {{
        \\    "seconds": {d:.9}
        \\  }},
        \\  "bulk_insert": {{
        \\    "rows": {d},
        \\    "batches": {d},
        \\    "seconds": {d:.9},
        \\    "rows_per_second": {d:.3},
        \\    "inserted": {d},
        \\    "errors": {d},
        \\    "bytes": {d}
        \\  }},
        \\  "batch_get": {{
        \\    "items": {d},
        \\    "batches": {d},
        \\    "seconds": {d:.9},
        \\    "items_per_second": {d:.3},
        \\    "found": {d},
        \\    "missing": {d}
        \\  }}
        \\}}
    , .{
        compat.milliTimestamp(),
        config.host,
        config.port,
        config.collection,
        config.rows,
        config.batch_size,
        config.get_rounds,
        config.get_batch_size,
        config.pipeline_window,
        if (config.auth_key.len > 0) "true" else "false",
        result.hello.max_frame,
        result.hello.max_response,
        result.hello.features,
        result.probe_seconds,
        result.bulk.operations,
        result.bulk.batches,
        result.bulk.seconds,
        result.bulk.per_second,
        result.bulk.inserted,
        result.bulk.errors,
        result.bulk.bytes,
        result.batch_get.operations,
        result.batch_get.batches,
        result.batch_get.seconds,
        result.batch_get.per_second,
        result.batch_get.found,
        result.batch_get.missing,
    });

    std.debug.print("{s}\n", .{json});
    if (config.output.len > 0) {
        const file = try compat.cwd().createFile(config.output, .{});
        defer file.close();
        try file.writeAll(json);
        try file.writeAll("\n");
    }
}

fn rate(ops: u64, seconds: f64) f64 {
    if (seconds <= 0) return 0;
    return @as(f64, @floatFromInt(ops)) / seconds;
}

fn nsToSeconds(ns: i128) f64 {
    return @as(f64, @floatFromInt(ns)) / 1_000_000_000.0;
}

fn rdU32BE(b: []const u8) u32 {
    return (@as(u32, b[0]) << 24) | (@as(u32, b[1]) << 16) | (@as(u32, b[2]) << 8) | b[3];
}

fn rdU16LE(b: []const u8) u16 {
    return @as(u16, b[0]) | (@as(u16, b[1]) << 8);
}

fn rdU32LE(b: []const u8) u32 {
    return @as(u32, b[0]) | (@as(u32, b[1]) << 8) | (@as(u32, b[2]) << 16) | (@as(u32, b[3]) << 24);
}

fn rdU64LE(b: []const u8) u64 {
    return @as(u64, rdU32LE(b[0..4])) | (@as(u64, rdU32LE(b[4..8])) << 32);
}

fn wrU32BE(w: []u8, v: u32) void {
    w[0] = @intCast((v >> 24) & 0xff);
    w[1] = @intCast((v >> 16) & 0xff);
    w[2] = @intCast((v >> 8) & 0xff);
    w[3] = @intCast(v & 0xff);
}

fn wrU16LE(w: []u8, v: u16) void {
    w[0] = @intCast(v & 0xff);
    w[1] = @intCast((v >> 8) & 0xff);
}

fn wrU32LE(w: []u8, v: u32) void {
    w[0] = @intCast(v & 0xff);
    w[1] = @intCast((v >> 8) & 0xff);
    w[2] = @intCast((v >> 16) & 0xff);
    w[3] = @intCast((v >> 24) & 0xff);
}

fn wrU64LE(w: []u8, v: u64) void {
    wrU32LE(w[0..4], @intCast(v & 0xffffffff));
    wrU32LE(w[4..8], @intCast((v >> 32) & 0xffffffff));
}

fn errnoError(err: std.c.E) error{
    BadResponse,
    BadStatus,
    ConnectionClosed,
    ConnectFailed,
    FrameTooLarge,
    InvalidIpAddress,
    ServerErrors,
    Unexpected,
} {
    switch (err) {
        .CONNREFUSED, .HOSTUNREACH, .NETUNREACH, .TIMEDOUT => return error.ConnectFailed,
        .CONNRESET, .PIPE, .NOTCONN => return error.ConnectionClosed,
        else => return error.Unexpected,
    }
}
