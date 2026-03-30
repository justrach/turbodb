/// TurboDB — Calvin Integration Layer
///
/// Bridges the Calvin replication protocol to TurboDB's Collection API.
/// This is the glue that makes replication actually work:
///
///   Client write → Sequencer → Batch → Broadcast → CalvinExecutor → Collection
///
/// The `ReplicatedDB` wraps a Database (as opaque handle) and optionally
/// routes writes through the Calvin sequencer. Reads always go directly
/// to the local collection.
///
/// Design: Uses opaque pointers for Database/Collection to avoid cross-module
/// imports. The actual DB operations are routed through function pointers
/// set at init time by the caller (main.zig).
const std = @import("std");
const sequencer = @import("sequencer.zig");
const calvin = @import("calvin.zig");
const peer = @import("peer.zig");

const Transaction = sequencer.Transaction;
const Sequencer = sequencer.Sequencer;
const CalvinExecutor = calvin.CalvinExecutor;
const ReplicationManager = calvin.ReplicationManager;
const PeerSender = peer.PeerSender;
const PeerReceiver = peer.PeerReceiver;

/// Function pointers for database operations. Set by main.zig at init.
/// This avoids importing collection.zig from the replication directory.
pub const DBOps = struct {
    /// fn(db: *anyopaque, col_name: []const u8, key: []const u8, value: []const u8) -> ?u64
    insert_fn: ?*const fn (*anyopaque, []const u8, []const u8, []const u8) ?u64 = null,
    /// fn(db: *anyopaque, branch_name: []const u8, col_name: []const u8, key: []const u8, value: []const u8) -> ?u64
    insert_branch_fn: ?*const fn (*anyopaque, []const u8, []const u8, []const u8, []const u8) ?u64 = null,
    /// fn(db: *anyopaque, col_name: []const u8, key: []const u8, value: []const u8) -> bool
    update_fn: ?*const fn (*anyopaque, []const u8, []const u8, []const u8) bool = null,
    /// fn(db: *anyopaque, branch_name: []const u8, col_name: []const u8, key: []const u8, value: []const u8) -> bool
    update_branch_fn: ?*const fn (*anyopaque, []const u8, []const u8, []const u8, []const u8) bool = null,
    /// fn(db: *anyopaque, col_name: []const u8, key: []const u8) -> bool
    delete_fn: ?*const fn (*anyopaque, []const u8, []const u8) bool = null,
    /// fn(db: *anyopaque, branch_name: []const u8, col_name: []const u8, key: []const u8) -> bool
    delete_branch_fn: ?*const fn (*anyopaque, []const u8, []const u8, []const u8) bool = null,
};

/// FNV-1a hash (duplicated here to avoid cross-module import of doc.zig).
fn fnv1a(s: []const u8) u64 {
    var h: u64 = 14695981039346656037;
    for (s) |b| {
        h ^= @as(u64, b);
        h = h *% 1099511628211;
    }
    return h;
}

/// Replication configuration.
pub const ReplicationConfig = struct {
    enabled: bool = false,
    node_id: u16 = 0,
    is_leader: bool = false,
    repl_port: u16 = 27117,
    partitions: []const u16 = &.{0},
};

/// A replicated database wrapper. Intercepts writes and routes them
/// through Calvin when replication is enabled.
pub const ReplicatedDB = struct {
    db_handle: *anyopaque,
    ops: DBOps,
    config: ReplicationConfig,
    repl_mgr: ?ReplicationManager,
    sender: PeerSender,
    receiver: ?PeerReceiver,
    alloc: std.mem.Allocator,
    next_txn_id: std.atomic.Value(u64),

    pub fn init(
        alloc: std.mem.Allocator,
        db_handle: *anyopaque,
        ops: DBOps,
        config: ReplicationConfig,
    ) !ReplicatedDB {
        var rdb = ReplicatedDB{
            .db_handle = db_handle,
            .ops = ops,
            .config = config,
            .repl_mgr = null,
            .sender = PeerSender{},
            .receiver = null,
            .alloc = alloc,
            .next_txn_id = std.atomic.Value(u64).init(1),
        };

        if (config.enabled) {
            var mgr = ReplicationManager.init(alloc, config.node_id, config.is_leader);
            try mgr.executor.setLocalPartitions(config.partitions);
            rdb.repl_mgr = mgr;
        }

        return rdb;
    }

    pub fn deinit(self: *ReplicatedDB) void {
        if (self.receiver) |*r| r.stop();
        if (self.repl_mgr) |*mgr| mgr.deinit();
    }

    /// Start replication (leader batch loop + replica receiver).
    pub fn startReplication(self: *ReplicatedDB) !void {
        if (self.repl_mgr) |*mgr| {
            // Set global state for the executor callback
            g_db_handle = self.db_handle;
            g_ops = self.ops;
            mgr.setBatchReadyHook(self, leaderBatchReady);

            try mgr.start();

            if (!self.config.is_leader) {
                self.receiver = PeerReceiver.init(
                    self.alloc,
                    self.config.repl_port,
                    &mgr.executor,
                    &executeTransaction,
                );
                const t = try std.Thread.spawn(.{}, PeerReceiver.run, .{&self.receiver.?});
                t.detach();
            }
        }
    }

    /// Add a peer replica for batch broadcast (leader only).
    pub fn addPeer(self: *ReplicatedDB, host: []const u8, port: u16) void {
        self.sender.addPeer(host, port);
    }

    // ── Write operations (routed through Calvin when replicated) ─────────

    /// Insert a document. If replicated, submits to sequencer; otherwise direct.
    pub fn insert(self: *ReplicatedDB, col_name: []const u8, key: []const u8, value: []const u8) ?u64 {
        return self.insertOnBranch("", col_name, key, value);
    }

    /// Insert a document into a branch-aware replica stream.
    pub fn insertOnBranch(self: *ReplicatedDB, branch_name: []const u8, col_name: []const u8, key: []const u8, value: []const u8) ?u64 {
        if (self.repl_mgr) |*mgr| {
            const key_hash = fnv1a(key);
            const partition: u16 = @intCast(key_hash % 256);

            var data_buf: [4096]u8 = undefined;
            const data_len = packTxnData(&data_buf, branch_name, col_name, key, value);

            const data = self.alloc.alloc(u8, data_len) catch return null;
            @memcpy(data, data_buf[0..data_len]);

            const ws = self.alloc.alloc(u64, 1) catch return null;
            ws[0] = key_hash;

            const txn_id = self.next_txn_id.fetchAdd(1, .monotonic);
            const txn = Transaction{
                .txn_id = txn_id,
                .txn_type = .put,
                .partition_id = partition,
                .key_hash = key_hash,
                .data = data,
                .read_set = &.{},
                .write_set = ws,
                .owns_buffers = true,
            };

            mgr.submit(txn) catch return null;
            return txn_id;
        }

        // Direct path
        if (branch_name.len > 0) {
            if (self.ops.insert_branch_fn) |f| return f(self.db_handle, branch_name, col_name, key, value);
        }
        if (self.ops.insert_fn) |f| return f(self.db_handle, col_name, key, value);
        return null;
    }

    /// Update a document.
    pub fn update(self: *ReplicatedDB, col_name: []const u8, key: []const u8, value: []const u8) bool {
        return self.updateOnBranch("", col_name, key, value);
    }

    /// Update a document in a named branch.
    pub fn updateOnBranch(self: *ReplicatedDB, branch_name: []const u8, col_name: []const u8, key: []const u8, value: []const u8) bool {
        if (self.repl_mgr) |*mgr| {
            const key_hash = fnv1a(key);
            const partition: u16 = @intCast(key_hash % 256);

            var data_buf: [4096]u8 = undefined;
            const data_len = packTxnData(&data_buf, branch_name, col_name, key, value);

            const data = self.alloc.alloc(u8, data_len) catch return false;
            @memcpy(data, data_buf[0..data_len]);

            const ws = self.alloc.alloc(u64, 1) catch return false;
            ws[0] = key_hash;

            const txn = Transaction{
                .txn_id = self.next_txn_id.fetchAdd(1, .monotonic),
                .txn_type = .put, // update = put with same key
                .partition_id = partition,
                .key_hash = key_hash,
                .data = data,
                .read_set = &.{},
                .write_set = ws,
                .owns_buffers = true,
            };

            mgr.submit(txn) catch return false;
            return true;
        }

        if (branch_name.len > 0) {
            if (self.ops.update_branch_fn) |f| return f(self.db_handle, branch_name, col_name, key, value);
        }
        if (self.ops.update_fn) |f| return f(self.db_handle, col_name, key, value);
        return false;
    }

    /// Delete a document.
    pub fn delete(self: *ReplicatedDB, col_name: []const u8, key: []const u8) bool {
        return self.deleteOnBranch("", col_name, key);
    }

    /// Delete a document in a named branch.
    pub fn deleteOnBranch(self: *ReplicatedDB, branch_name: []const u8, col_name: []const u8, key: []const u8) bool {
        if (self.repl_mgr) |*mgr| {
            const key_hash = fnv1a(key);
            const partition: u16 = @intCast(key_hash % 256);

            var data_buf: [256]u8 = undefined;
            const data_len = packTxnData(&data_buf, branch_name, col_name, key, "");

            const data = self.alloc.alloc(u8, data_len) catch return false;
            @memcpy(data, data_buf[0..data_len]);

            const ws = self.alloc.alloc(u64, 1) catch return false;
            ws[0] = key_hash;

            const txn = Transaction{
                .txn_id = self.next_txn_id.fetchAdd(1, .monotonic),
                .txn_type = .delete,
                .partition_id = partition,
                .key_hash = key_hash,
                .data = data,
                .read_set = &.{},
                .write_set = ws,
                .owns_buffers = true,
            };

            mgr.submit(txn) catch return false;
            return true;
        }

        if (branch_name.len > 0) {
            if (self.ops.delete_branch_fn) |f| return f(self.db_handle, branch_name, col_name, key);
        }
        if (self.ops.delete_fn) |f| return f(self.db_handle, col_name, key);
        return false;
    }

    /// Get replication lag (0 if not replicated).
    pub fn replicationLag(self: *const ReplicatedDB) u64 {
        if (self.repl_mgr) |*mgr| return mgr.lag();
        return 0;
    }

    pub fn isLeader(self: *const ReplicatedDB) bool {
        return self.config.is_leader;
    }
};

// ── Transaction data packing ─────────────────────────────────────────────────
// Format: [branch_len:u8][branch][col_name_len:u8][col_name][key_len:u16 LE][key][value]

fn packTxnData(buf: []u8, branch_name: []const u8, col_name: []const u8, key: []const u8, value: []const u8) usize {
    var pos: usize = 0;
    buf[pos] = @intCast(branch_name.len);
    pos += 1;
    @memcpy(buf[pos..][0..branch_name.len], branch_name);
    pos += branch_name.len;
    buf[pos] = @intCast(col_name.len);
    pos += 1;
    @memcpy(buf[pos..][0..col_name.len], col_name);
    pos += col_name.len;
    std.mem.writeInt(u16, buf[pos..][0..2], @intCast(key.len), .little);
    pos += 2;
    @memcpy(buf[pos..][0..key.len], key);
    pos += key.len;
    @memcpy(buf[pos..][0..value.len], value);
    pos += value.len;
    return pos;
}

fn unpackTxnData(data: []const u8) ?struct { branch: []const u8, col: []const u8, key: []const u8, val: []const u8 } {
    if (data.len < 5) return null;
    var pos: usize = 0;
    const branch_len: usize = data[pos];
    pos += 1;
    if (pos + branch_len > data.len) return null;
    const branch = data[pos..][0..branch_len];
    pos += branch_len;
    const col_len: usize = data[pos];
    pos += 1;
    if (pos + col_len > data.len) return null;
    const col = data[pos..][0..col_len];
    pos += col_len;
    if (pos + 2 > data.len) return null;
    const key_len = std.mem.readInt(u16, data[pos..][0..2], .little);
    pos += 2;
    if (pos + key_len > data.len) return null;
    const key = data[pos..][0..key_len];
    pos += key_len;
    return .{ .branch = branch, .col = col, .key = key, .val = data[pos..] };
}

// ── Transaction executor (called by CalvinExecutor) ──────────────────────────

var g_db_handle: ?*anyopaque = null;
var g_ops: DBOps = .{};

const BranchCall = struct {
    branch_buf: [64]u8 = [_]u8{0} ** 64,
    branch_len: usize = 0,
    col_buf: [64]u8 = [_]u8{0} ** 64,
    col_len: usize = 0,
    key_buf: [64]u8 = [_]u8{0} ** 64,
    key_len: usize = 0,
    value_buf: [256]u8 = [_]u8{0} ** 256,
    value_len: usize = 0,

    fn branch(self: *const BranchCall) []const u8 {
        return self.branch_buf[0..self.branch_len];
    }

    fn col(self: *const BranchCall) []const u8 {
        return self.col_buf[0..self.col_len];
    }

    fn key(self: *const BranchCall) []const u8 {
        return self.key_buf[0..self.key_len];
    }

    fn value(self: *const BranchCall) []const u8 {
        return self.value_buf[0..self.value_len];
    }

    fn setField(buf: []u8, len_out: *usize, input: []const u8) void {
        const n = @min(buf.len, input.len);
        @memcpy(buf[0..n], input[0..n]);
        len_out.* = n;
    }
};

fn executeTransaction(txn: *const Transaction) anyerror!void {
    const db = g_db_handle orelse return error.DatabaseNotInitialized;
    const parsed = unpackTxnData(txn.data) orelse return error.InvalidData;

    switch (txn.txn_type) {
        .put => {
            // put = insert or update (same semantics: upsert)
            if (parsed.branch.len > 0) {
                if (g_ops.insert_branch_fn) |f| {
                    _ = f(db, parsed.branch, parsed.col, parsed.key, parsed.val);
                    return;
                }
            }
            if (g_ops.insert_fn) |f| {
                _ = f(db, parsed.col, parsed.key, parsed.val);
            }
        },
        .delete => {
            if (parsed.branch.len > 0) {
                if (g_ops.delete_branch_fn) |f| {
                    _ = f(db, parsed.branch, parsed.col, parsed.key);
                    return;
                }
            }
            if (g_ops.delete_fn) |f| {
                _ = f(db, parsed.col, parsed.key);
            }
        },
        .read => {},
    }
}

fn leaderBatchReady(ctx: *anyopaque, batch: *const sequencer.Batch) anyerror!void {
    const self: *ReplicatedDB = @ptrCast(@alignCast(ctx));

    const buf = try self.alloc.alloc(u8, 1 << 20);
    defer self.alloc.free(buf);

    const len = try CalvinExecutor.serializeBatch(batch, buf);
    self.sender.broadcast(buf[0..len]);
    try self.repl_mgr.?.executor.executeBatch(batch, &executeTransaction);
}

// ── Tests ────────────────────────────────────────────────────────────────────

test "packTxnData and unpackTxnData round-trip" {
    var buf: [256]u8 = undefined;
    const len = packTxnData(&buf, "", "users", "alice", "{\"name\":\"Alice\"}");

    const parsed = unpackTxnData(buf[0..len]).?;
    try std.testing.expectEqual(@as(usize, 0), parsed.branch.len);
    try std.testing.expectEqualStrings("users", parsed.col);
    try std.testing.expectEqualStrings("alice", parsed.key);
    try std.testing.expectEqualStrings("{\"name\":\"Alice\"}", parsed.val);
}

test "packTxnData preserves branch identity" {
    var buf: [256]u8 = undefined;
    const len = packTxnData(&buf, "feature-x", "users", "alice", "{\"name\":\"Alice\"}");

    const parsed = unpackTxnData(buf[0..len]).?;
    try std.testing.expectEqualStrings("feature-x", parsed.branch);
    try std.testing.expectEqualStrings("users", parsed.col);
    try std.testing.expectEqualStrings("alice", parsed.key);
    try std.testing.expectEqualStrings("{\"name\":\"Alice\"}", parsed.val);
}

test "packTxnData empty value (delete)" {
    var buf: [256]u8 = undefined;
    const len = packTxnData(&buf, "", "users", "alice", "");

    const parsed = unpackTxnData(buf[0..len]).?;
    try std.testing.expectEqualStrings("users", parsed.col);
    try std.testing.expectEqualStrings("alice", parsed.key);
    try std.testing.expectEqual(@as(usize, 0), parsed.val.len);
}

test "unpackTxnData invalid data" {
    const result = unpackTxnData("ab");
    try std.testing.expect(result == null);
}

test "ReplicatedDB direct mode" {
    const ops = DBOps{
        .insert_fn = struct {
            fn f(_: *anyopaque, _: []const u8, _: []const u8, _: []const u8) ?u64 {
                return 42;
            }
        }.f,
    };

    var dummy_db: u8 = 0;
    var rdb = try ReplicatedDB.init(std.testing.allocator, @ptrCast(&dummy_db), ops, .{});
    defer rdb.deinit();

    const result = rdb.insert("col", "key", "val");
    try std.testing.expectEqual(@as(?u64, 42), result);
    try std.testing.expectEqual(@as(u64, 0), rdb.replicationLag());
}

test "ReplicatedDB direct branch mode uses branch callback" {
    const ops = DBOps{
        .insert_branch_fn = struct {
            fn f(db: *anyopaque, branch_name: []const u8, col_name: []const u8, key: []const u8, value: []const u8) ?u64 {
                const seen: *BranchCall = @ptrCast(@alignCast(db));
                BranchCall.setField(&seen.branch_buf, &seen.branch_len, branch_name);
                BranchCall.setField(&seen.col_buf, &seen.col_len, col_name);
                BranchCall.setField(&seen.key_buf, &seen.key_len, key);
                BranchCall.setField(&seen.value_buf, &seen.value_len, value);
                return 77;
            }
        }.f,
    };

    var seen = BranchCall{};
    var rdb = try ReplicatedDB.init(std.testing.allocator, @ptrCast(&seen), ops, .{});
    defer rdb.deinit();

    const result = rdb.insertOnBranch("feature-x", "users", "alice", "{\"name\":\"Alice\"}");
    try std.testing.expectEqual(@as(?u64, 77), result);
    try std.testing.expectEqualStrings("feature-x", seen.branch());
    try std.testing.expectEqualStrings("users", seen.col());
    try std.testing.expectEqualStrings("alice", seen.key());
    try std.testing.expectEqualStrings("{\"name\":\"Alice\"}", seen.value());
}

test "ReplicatedDB replicated mode submits to sequencer" {
    var dummy_db: u8 = 0;
    var rdb = try ReplicatedDB.init(std.testing.allocator, @ptrCast(&dummy_db), .{}, .{
        .enabled = true,
        .node_id = 1,
        .is_leader = true,
    });
    defer rdb.deinit();

    // Insert through sequencer returns a txn_id
    const result = rdb.insert("users", "alice", "{\"name\":\"Alice\"}");
    try std.testing.expect(result != null);
    try std.testing.expect(result.? >= 1);
    try std.testing.expect(rdb.isLeader());
}

test "executeTransaction routes branch payloads to branch callbacks" {
    const ops = DBOps{
        .insert_branch_fn = struct {
            fn f(db: *anyopaque, branch_name: []const u8, col_name: []const u8, key: []const u8, value: []const u8) ?u64 {
                const seen: *BranchCall = @ptrCast(@alignCast(db));
                BranchCall.setField(&seen.branch_buf, &seen.branch_len, branch_name);
                BranchCall.setField(&seen.col_buf, &seen.col_len, col_name);
                BranchCall.setField(&seen.key_buf, &seen.key_len, key);
                BranchCall.setField(&seen.value_buf, &seen.value_len, value);
                return 1;
            }
        }.f,
    };

    var seen = BranchCall{};
    g_db_handle = @ptrCast(&seen);
    g_ops = ops;

    var buf: [256]u8 = undefined;
    const len = packTxnData(&buf, "feature-x", "users", "alice", "{\"name\":\"Alice\"}");
    const txn = Transaction{
        .txn_id = 1,
        .txn_type = .put,
        .partition_id = 0,
        .key_hash = fnv1a("alice"),
        .data = buf[0..len],
        .read_set = &.{},
        .write_set = &.{},
    };

    try executeTransaction(&txn);
    try std.testing.expectEqualStrings("feature-x", seen.branch());
    try std.testing.expectEqualStrings("users", seen.col());
    try std.testing.expectEqualStrings("alice", seen.key());
    try std.testing.expectEqualStrings("{\"name\":\"Alice\"}", seen.value());
}

test "leader batch hook executes queued writes locally" {
    const ops = DBOps{
        .insert_fn = struct {
            fn f(db: *anyopaque, col_name: []const u8, key: []const u8, value: []const u8) ?u64 {
                const seen: *BranchCall = @ptrCast(@alignCast(db));
                BranchCall.setField(&seen.col_buf, &seen.col_len, col_name);
                BranchCall.setField(&seen.key_buf, &seen.key_len, key);
                BranchCall.setField(&seen.value_buf, &seen.value_len, value);
                return 1;
            }
        }.f,
    };

    var seen = BranchCall{};
    var rdb = try ReplicatedDB.init(std.testing.allocator, @ptrCast(&seen), ops, .{
        .enabled = true,
        .node_id = 1,
        .is_leader = true,
    });
    defer rdb.deinit();

    g_db_handle = rdb.db_handle;
    g_ops = rdb.ops;
    try rdb.repl_mgr.?.executor.setLocalPartitions(&.{@as(u16, @intCast(fnv1a("alice") % 256))});

    const txn_id = rdb.insert("users", "alice", "{\"name\":\"Alice\"}");
    try std.testing.expect(txn_id != null);
    var batch = (try rdb.repl_mgr.?.seq.drainBatch(std.testing.allocator)).?;
    defer batch.deinitDeep(std.testing.allocator);
    try leaderBatchReady(&rdb, &batch);
    try std.testing.expectEqualStrings("users", seen.col());
    try std.testing.expectEqualStrings("alice", seen.key());
    try std.testing.expectEqualStrings("{\"name\":\"Alice\"}", seen.value());
}

test "fnv1a matches expected" {
    const h = fnv1a("hello");
    try std.testing.expect(h != 0);
    // Deterministic
    try std.testing.expectEqual(h, fnv1a("hello"));
    // Different inputs → different hashes
    try std.testing.expect(h != fnv1a("world"));
}
