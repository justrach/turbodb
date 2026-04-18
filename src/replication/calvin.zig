const std = @import("std");
const sequencer = @import("sequencer.zig");
const runtime = @import("runtime");
const compat = @import("compat");

pub const NodeId = u16;

/// Peer node connection info.
pub const NodeInfo = struct {
    id: NodeId,
    address: [64]u8,
    address_len: u8,
    port: u16,
    is_local: bool,
};

/// Network message types between nodes.
pub const MsgType = enum(u8) {
    batch_broadcast = 1, // sequencer → all nodes
    batch_ack = 2, // node → sequencer
    read_request = 3, // node → node (for remote reads)
    read_response = 4, // node → node
};

/// Wire message header — fixed 32-byte header for all replication messages.
pub const MsgHeader = extern struct {
    msg_type: u8,
    sender_id: u16,
    payload_len: u32,
    epoch: u64,
    _pad: [13]u8 = std.mem.zeroes([13]u8),

    pub const size = 32;

    comptime {
        std.debug.assert(@sizeOf(MsgHeader) == size);
    }
};

/// The Calvin executor — deterministic transaction execution.
///
/// Calvin's key insight: if every replica executes the same ordered sequence
/// of transactions, they all arrive at the same state — no 2PC needed.
/// The sequencer provides the total order; this executor applies it.
pub const CalvinExecutor = struct {
    node_id: NodeId,
    nodes: std.ArrayListUnmanaged(NodeInfo),
    local_partitions: std.ArrayListUnmanaged(u16),
    alloc: std.mem.Allocator,

    // Execution state
    last_executed_epoch: u64,
    execute_mu: std.Io.Mutex,

    pub fn init(alloc: std.mem.Allocator, node_id: NodeId) CalvinExecutor {
        return .{
            .node_id = node_id,
            .nodes = .empty,
            .local_partitions = .empty,
            .alloc = alloc,
            .last_executed_epoch = 0,
            .execute_mu = .init,
        };
    }

    pub fn deinit(self: *CalvinExecutor) void {
        self.nodes.deinit(self.alloc);
        self.local_partitions.deinit(self.alloc);
    }

    /// Register a peer node.
    pub fn addNode(self: *CalvinExecutor, info: NodeInfo) !void {
        try self.nodes.append(self.alloc, info);
    }

    /// Set which partitions this node owns.
    pub fn setLocalPartitions(self: *CalvinExecutor, partitions: []const u16) !void {
        self.local_partitions.clearRetainingCapacity();
        try self.local_partitions.appendSlice(self.alloc, partitions);
    }

    /// Execute a batch deterministically. All nodes execute the same batch
    /// in the same order, producing identical state. Only transactions
    /// touching local partitions are actually executed; the rest are skipped
    /// (they run on their owning node).
    pub fn executeBatch(
        self: *CalvinExecutor,
        batch: *const sequencer.Batch,
        executor_fn: *const fn (txn: *const sequencer.Transaction) anyerror!void,
    ) !void {
        self.execute_mu.lockUncancelable(runtime.io);
        defer self.execute_mu.unlock(runtime.io);

        // Deterministic execution: iterate in batch order.
        for (batch.transactions) |*txn| {
            if (self.isLocalTxn(txn)) {
                try executor_fn(txn);
            }
        }

        self.last_executed_epoch = batch.epoch;
    }

    /// Check if a transaction touches local partitions.
    pub fn isLocalTxn(self: *const CalvinExecutor, txn: *const sequencer.Transaction) bool {
        for (self.local_partitions.items) |pid| {
            if (txn.partition_id == pid) return true;
        }
        return false;
    }

    /// Serialize a batch for network transmission.
    /// Format: [epoch:u64][seq_start:u64][count:u32][txn0][txn1]...
    /// Each txn: [txn_id:u64][type:u8][partition:u16][key_hash:u64]
    ///           [data_len:u32][data...][rs_len:u32][rs...][ws_len:u32][ws...]
    pub fn serializeBatch(batch: *const sequencer.Batch, buf: []u8) !usize {
        var w = std.Io.Writer.fixed(buf);

        try w.writeInt(u64, batch.epoch, .little);
        try w.writeInt(u64, batch.sequence_start, .little);
        try w.writeInt(u32, @intCast(batch.transactions.len), .little);

        for (batch.transactions) |txn| {
            try w.writeInt(u64, txn.txn_id, .little);
            try w.writeByte(@intFromEnum(txn.txn_type));
            try w.writeInt(u16, txn.partition_id, .little);
            try w.writeInt(u64, txn.key_hash, .little);

            // data
            try w.writeInt(u32, @intCast(txn.data.len), .little);
            try w.writeAll(txn.data);

            // read_set
            try w.writeInt(u32, @intCast(txn.read_set.len), .little);
            for (txn.read_set) |h| try w.writeInt(u64, h, .little);

            // write_set
            try w.writeInt(u32, @intCast(txn.write_set.len), .little);
            for (txn.write_set) |h| try w.writeInt(u64, h, .little);
        }

        return w.buffered().len;
    }

    /// Deserialize a batch from network bytes.
    pub fn deserializeBatch(data: []const u8, alloc: std.mem.Allocator) !sequencer.Batch {
        var r = std.Io.Reader.fixed(data);

        const epoch = try r.takeInt(u64, .little);
        const seq_start = try r.takeInt(u64, .little);
        const count = try r.takeInt(u32, .little);

        const txns = try alloc.alloc(sequencer.Transaction, count);
        errdefer alloc.free(txns);

        for (txns) |*txn| {
            txn.txn_id = try r.takeInt(u64, .little);
            txn.txn_type = @enumFromInt(try r.takeByte());
            txn.partition_id = try r.takeInt(u16, .little);
            txn.key_hash = try r.takeInt(u64, .little);

            // data
            const data_len = try r.takeInt(u32, .little);
            if (data_len > 0) {
                const d = try alloc.alloc(u8, data_len);
                try r.readSliceAll(d);
                txn.data = d;
            } else {
                txn.data = &.{};
            }

            // read_set
            const rs_len = try r.takeInt(u32, .little);
            if (rs_len > 0) {
                const rs = try alloc.alloc(u64, rs_len);
                for (rs) |*h| h.* = try r.takeInt(u64, .little);
                txn.read_set = rs;
            } else {
                txn.read_set = &.{};
            }

            // write_set
            const ws_len = try r.takeInt(u32, .little);
            if (ws_len > 0) {
                const ws = try alloc.alloc(u64, ws_len);
                for (ws) |*h| h.* = try r.takeInt(u64, .little);
                txn.write_set = ws;
            } else {
                txn.write_set = &.{};
            }
            txn.owns_buffers = true;
        }

        return .{
            .epoch = epoch,
            .sequence_start = seq_start,
            .transactions = txns,
        };
    }
};

/// ReplicationManager — ties together sequencer + executor.
/// On the leader node, the sequencer batches and orders transactions.
/// On all nodes (including leader), the executor applies batches deterministically.
pub const ReplicationManager = struct {
    seq: sequencer.Sequencer,
    executor: CalvinExecutor,
    is_leader: bool,
    running: std.atomic.Value(bool),
    batch_thread: ?std.Thread,
    alloc: std.mem.Allocator,
    batch_ready_ctx: ?*anyopaque,
    batch_ready_fn: ?*const fn (*anyopaque, *const sequencer.Batch) anyerror!void,

    pub fn init(alloc: std.mem.Allocator, node_id: NodeId, is_leader: bool) ReplicationManager {
        return .{
            .seq = sequencer.Sequencer.init(),
            .executor = CalvinExecutor.init(alloc, node_id),
            .is_leader = is_leader,
            .running = std.atomic.Value(bool).init(false),
            .batch_thread = null,
            .alloc = alloc,
            .batch_ready_ctx = null,
            .batch_ready_fn = null,
        };
    }

    pub fn deinit(self: *ReplicationManager) void {
        self.stop();
        self.seq.deinit(self.alloc);
        self.executor.deinit();
    }

    /// Start the batching loop (leader only) and executor.
    pub fn start(self: *ReplicationManager) !void {
        if (self.running.load(.acquire)) return;
        self.running.store(true, .release);

        if (self.is_leader) {
            self.batch_thread = try std.Thread.spawn(.{}, batchLoop, .{self});
        }
    }

    /// Stop replication.
    pub fn stop(self: *ReplicationManager) void {
        if (!self.running.load(.acquire)) return;
        self.running.store(false, .release);

        if (self.batch_thread) |t| {
            t.join();
            self.batch_thread = null;
        }
    }

    /// Submit a transaction (routes to sequencer on leader).
    pub fn submit(self: *ReplicationManager, txn: sequencer.Transaction) !void {
        if (!self.is_leader) return error.NotLeader;
        try self.seq.submit(self.alloc, txn);
    }

    pub fn setBatchReadyHook(
        self: *ReplicationManager,
        ctx: *anyopaque,
        hook: *const fn (*anyopaque, *const sequencer.Batch) anyerror!void,
    ) void {
        self.batch_ready_ctx = ctx;
        self.batch_ready_fn = hook;
    }

    pub fn processOnce(self: *ReplicationManager) !bool {
        if (try self.seq.drainBatch(self.alloc)) |batch_val| {
            var batch = batch_val;
            defer batch.deinitDeep(self.alloc);

            if (self.batch_ready_ctx) |ctx| {
                if (self.batch_ready_fn) |hook| {
                    try hook(ctx, &batch);
                    return true;
                }
            }

            try self.executor.executeBatch(&batch, &noopExecutor);
            return true;
        }
        return false;
    }

    /// Get replication lag (epochs behind leader).
    pub fn lag(self: *const ReplicationManager) u64 {
        const current = self.seq.current_epoch;
        const executed = self.executor.last_executed_epoch;
        if (current <= executed) return 0;
        return current - executed;
    }

    fn batchLoop(self: *ReplicationManager) void {
        while (self.running.load(.acquire)) {
            self.processOnce() catch {};

            // Sleep for the batch window.
            std.time.sleep(self.seq.batch_window_ns);
        }
    }

    fn noopExecutor(_: *const sequencer.Transaction) anyerror!void {}
};

// ─── Tests ───────────────────────────────────────────────────────────

fn testExecutor(txn: *const sequencer.Transaction) anyerror!void {
    // Verify we received a valid transaction by checking txn_id is reasonable.
    if (txn.txn_id > 1_000_000) return error.InvalidTxn;
}

fn makeTxn(id: u64, typ: sequencer.TxnType, partition: u16, key: u64, rs: []const u64, ws: []const u64) sequencer.Transaction {
    return .{
        .txn_id = id,
        .txn_type = typ,
        .partition_id = partition,
        .key_hash = key,
        .data = &.{},
        .read_set = rs,
        .write_set = ws,
    };
}

test "calvin: executeBatch runs local transactions" {
    const alloc = std.testing.allocator;
    var exec = CalvinExecutor.init(alloc, 1);
    defer exec.deinit();

    try exec.setLocalPartitions(&.{ 0, 1 });

    var txns = [_]sequencer.Transaction{
        makeTxn(10, .put, 0, 100, &.{}, &.{}),
        makeTxn(11, .put, 2, 200, &.{}, &.{}), // remote — partition 2 not local
        makeTxn(12, .delete, 1, 300, &.{}, &.{}),
    };

    const batch = sequencer.Batch{
        .epoch = 5,
        .sequence_start = 100,
        .transactions = &txns,
    };

    try exec.executeBatch(&batch, &testExecutor);
    try std.testing.expectEqual(@as(u64, 5), exec.last_executed_epoch);
}

test "calvin: isLocalTxn filtering" {
    const alloc = std.testing.allocator;
    var exec = CalvinExecutor.init(alloc, 1);
    defer exec.deinit();

    try exec.setLocalPartitions(&.{ 0, 3 });

    const local_txn = makeTxn(1, .put, 3, 0, &.{}, &.{});
    const remote_txn = makeTxn(2, .put, 7, 0, &.{}, &.{});

    try std.testing.expect(exec.isLocalTxn(&local_txn));
    try std.testing.expect(!exec.isLocalTxn(&remote_txn));
}

test "calvin: serialize/deserialize batch round-trip" {
    const alloc = std.testing.allocator;

    const ws = [_]u64{ 10, 20 };
    const rs = [_]u64{30};
    const data = "hello";

    var txns = [_]sequencer.Transaction{
        .{
            .txn_id = 42,
            .txn_type = .put,
            .partition_id = 3,
            .key_hash = 0xDEAD,
            .data = data,
            .read_set = &rs,
            .write_set = &ws,
        },
        .{
            .txn_id = 43,
            .txn_type = .delete,
            .partition_id = 1,
            .key_hash = 0xBEEF,
            .data = &.{},
            .read_set = &.{},
            .write_set = &.{},
        },
    };

    const batch = sequencer.Batch{
        .epoch = 7,
        .sequence_start = 100,
        .transactions = &txns,
    };

    var buf: [4096]u8 = undefined;
    const len = try CalvinExecutor.serializeBatch(&batch, &buf);

    var decoded = try CalvinExecutor.deserializeBatch(buf[0..len], alloc);
    defer decoded.deinitDeep(alloc);

    try std.testing.expectEqual(@as(u64, 7), decoded.epoch);
    try std.testing.expectEqual(@as(u64, 100), decoded.sequence_start);
    try std.testing.expectEqual(@as(usize, 2), decoded.transactions.len);

    // First txn
    const t0 = decoded.transactions[0];
    try std.testing.expectEqual(@as(u64, 42), t0.txn_id);
    try std.testing.expectEqual(sequencer.TxnType.put, t0.txn_type);
    try std.testing.expectEqual(@as(u16, 3), t0.partition_id);
    try std.testing.expectEqual(@as(u64, 0xDEAD), t0.key_hash);
    try std.testing.expectEqualStrings("hello", t0.data);
    try std.testing.expectEqual(@as(usize, 1), t0.read_set.len);
    try std.testing.expectEqual(@as(u64, 30), t0.read_set[0]);
    try std.testing.expectEqual(@as(usize, 2), t0.write_set.len);
    try std.testing.expectEqual(@as(u64, 10), t0.write_set[0]);
    try std.testing.expectEqual(@as(u64, 20), t0.write_set[1]);

    // Second txn
    const t1 = decoded.transactions[1];
    try std.testing.expectEqual(@as(u64, 43), t1.txn_id);
    try std.testing.expectEqual(sequencer.TxnType.delete, t1.txn_type);
}

test "calvin: ReplicationManager init, submit, and lag" {
    const alloc = std.testing.allocator;

    var mgr = ReplicationManager.init(alloc, 0, true);
    defer mgr.deinit();

    try mgr.executor.setLocalPartitions(&.{0});

    // Submit a transaction before starting (just queues it).
    try mgr.submit(makeTxn(1, .put, 0, 55, &.{}, &.{}));

    // Manually drain and execute to verify epoch advancement
    // (without starting the background thread).
    var batch = (try mgr.seq.drainBatch(alloc)).?;
    defer batch.deinit(alloc);

    try mgr.executor.executeBatch(&batch, &testExecutor);

    // After executing epoch 0: seq.current_epoch=1, last_executed_epoch=0
    // lag = 1 - 0 = 1
    try std.testing.expectEqual(@as(u64, 1), mgr.lag());
}

test "calvin: non-leader cannot submit" {
    const alloc = std.testing.allocator;
    var mgr = ReplicationManager.init(alloc, 1, false);
    defer mgr.deinit();

    const result = mgr.submit(makeTxn(1, .put, 0, 0, &.{}, &.{}));
    try std.testing.expectError(error.NotLeader, result);
}
