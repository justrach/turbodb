const std = @import("std");
const runtime = @import("runtime");

pub const TxnType = enum(u8) { put, delete, read };

/// A transaction to be sequenced.
pub const Transaction = struct {
    txn_id: u64,
    txn_type: TxnType,
    partition_id: u16,
    key_hash: u64,
    data: []const u8, // serialized key+value
    read_set: []const u64, // key_hashes read
    write_set: []const u64, // key_hashes written
    owns_buffers: bool = false,
};

/// An ordered batch of transactions ready for deterministic execution.
pub const Batch = struct {
    epoch: u64,
    sequence_start: u64,
    transactions: []Transaction,

    pub fn deinit(self: *Batch, alloc: std.mem.Allocator) void {
        alloc.free(self.transactions);
    }

    pub fn deinitDeep(self: *Batch, alloc: std.mem.Allocator) void {
        for (self.transactions) |txn| {
            if (txn.owns_buffers) {
                if (txn.data.len > 0) alloc.free(txn.data);
                if (txn.write_set.len > 0) alloc.free(txn.write_set);
                if (txn.read_set.len > 0) alloc.free(txn.read_set);
            }
        }
        alloc.free(self.transactions);
    }
};

/// The Sequencer collects transactions over a batching window,
/// assigns sequence numbers, and produces ordered batches.
/// Calvin (Thomson et al., SIGMOD 2012) requires a deterministic ordering
/// of all transactions before execution — the sequencer provides this.
pub const Sequencer = struct {
    pending: std.ArrayListUnmanaged(Transaction),
    next_seq: u64,
    current_epoch: u64,
    batch_window_ns: u64, // batching window in nanoseconds (default 5ms)
    mu: std.Io.Mutex,

    const default_batch_window_ns: u64 = 5_000_000; // 5ms

    pub fn init() Sequencer {
        return .{
            .pending = .empty,
            .next_seq = 0,
            .current_epoch = 0,
            .batch_window_ns = default_batch_window_ns,
            .mu = .{},
        };
    }

    pub fn deinit(self: *Sequencer, alloc: std.mem.Allocator) void {
        for (self.pending.items) |txn| {
            if (txn.owns_buffers) {
                if (txn.data.len > 0) alloc.free(txn.data);
                if (txn.write_set.len > 0) alloc.free(txn.write_set);
                if (txn.read_set.len > 0) alloc.free(txn.read_set);
            }
        }
        self.pending.deinit(alloc);
    }

    /// Submit a transaction for sequencing. Thread-safe.
    pub fn submit(self: *Sequencer, alloc: std.mem.Allocator, txn: Transaction) !void {
        self.mu.lockUncancelable(runtime.io);
        defer self.mu.unlock(runtime.io);
        try self.pending.append(alloc, txn);
    }

    /// Drain pending transactions into an ordered batch.
    /// Called periodically by the batch timer. Each batch gets a monotonically
    /// increasing epoch and sequence range so every node applies the same order.
    pub fn drainBatch(self: *Sequencer, alloc: std.mem.Allocator) !?Batch {
        self.mu.lockUncancelable(runtime.io);
        defer self.mu.unlock(runtime.io);

        if (self.pending.items.len == 0) return null;

        const count = self.pending.items.len;
        const txns = try alloc.alloc(Transaction, count);
        @memcpy(txns, self.pending.items);

        const seq_start = self.next_seq;
        self.next_seq += count;

        const epoch = self.current_epoch;
        self.current_epoch += 1;

        self.pending.clearRetainingCapacity();

        return Batch{
            .epoch = epoch,
            .sequence_start = seq_start,
            .transactions = txns,
        };
    }

    /// Check for write-write conflicts within a batch.
    /// Two transactions conflict if their write sets overlap.
    /// Returns pairs of indices that conflict.
    pub fn detectConflicts(batch: *const Batch, alloc: std.mem.Allocator) ![][2]usize {
        var conflicts: std.ArrayListUnmanaged([2]usize) = .empty;
        errdefer conflicts.deinit(alloc);

        const txns = batch.transactions;
        for (0..txns.len) |i| {
            for ((i + 1)..txns.len) |j| {
                if (writeSetOverlaps(txns[i].write_set, txns[j].write_set)) {
                    try conflicts.append(alloc, .{ i, j });
                }
            }
        }

        return conflicts.toOwnedSlice(alloc);
    }

    /// Reorder batch to resolve conflicts by aborting the later conflicting txn.
    /// In Calvin, the sequencer decides ordering before execution, so conflicts
    /// are resolved here by removing the later transaction from the batch.
    /// Reorder batch to resolve conflicts by aborting the later conflicting txn.
    /// In Calvin, the sequencer decides ordering before execution, so conflicts
    /// are resolved here by removing the later transaction from the batch.
    /// Note: this modifies the transactions slice length in-place. The caller
    /// retains ownership of the original allocation for freeing.
    /// Reorder batch to resolve conflicts by aborting the later conflicting txn.
    /// In Calvin, the sequencer decides ordering before execution, so conflicts
    /// are resolved here by removing the later transaction from the batch.
    /// Reallocates the transactions slice to contain only surviving transactions.
    pub fn resolveConflicts(batch: *Batch, alloc: std.mem.Allocator) !void {
        const conflicts = try detectConflicts(batch, alloc);
        defer alloc.free(conflicts);

        if (conflicts.len == 0) return;

        // Collect indices to remove (always the later txn in each pair).
        var remove_set = std.AutoHashMap(usize, void).init(alloc);
        defer remove_set.deinit();

        for (conflicts) |pair| {
            try remove_set.put(pair[1], {});
        }

        // Count survivors.
        var keep_count: usize = 0;
        for (0..batch.transactions.len) |i| {
            if (!remove_set.contains(i)) keep_count += 1;
        }

        // Allocate new slice and copy survivors.
        const new_txns = try alloc.alloc(Transaction, keep_count);
        var write_idx: usize = 0;
        for (0..batch.transactions.len) |read_idx| {
            if (!remove_set.contains(read_idx)) {
                new_txns[write_idx] = batch.transactions[read_idx];
                write_idx += 1;
            }
        }

        // Free old slice and replace.
        alloc.free(batch.transactions);
        batch.transactions = new_txns;
    }

    fn writeSetOverlaps(a: []const u64, b: []const u64) bool {
        for (a) |ka| {
            for (b) |kb| {
                if (ka == kb) return true;
            }
        }
        return false;
    }
};

// ─── Tests ───────────────────────────────────────────────────────────

fn makeTxn(id: u64, typ: TxnType, partition: u16, key: u64, rs: []const u64, ws: []const u64) Transaction {
        return .{
            .txn_id = id,
            .txn_type = typ,
            .partition_id = partition,
            .key_hash = key,
            .data = &.{},
            .read_set = rs,
            .write_set = ws,
            .owns_buffers = false,
        };
}

test "sequencer: submit and drain batch preserves ordering" {
    const alloc = std.testing.allocator;
    var seq = Sequencer.init();
    defer seq.deinit(alloc);

    // Submit 3 transactions.
    for (0..3) |i| {
        try seq.submit(alloc, makeTxn(i, .put, 0, i * 100, &.{}, &.{}));
    }

    var batch = (try seq.drainBatch(alloc)).?;
    defer batch.deinit(alloc);

    // Epoch 0, sequence starts at 0, 3 transactions.
    try std.testing.expectEqual(@as(u64, 0), batch.epoch);
    try std.testing.expectEqual(@as(u64, 0), batch.sequence_start);
    try std.testing.expectEqual(@as(usize, 3), batch.transactions.len);

    // Verify ordering is preserved (txn_id 0, 1, 2).
    for (batch.transactions, 0..) |txn, i| {
        try std.testing.expectEqual(@as(u64, i), txn.txn_id);
    }

    // Second drain returns null (no pending).
    const empty = try seq.drainBatch(alloc);
    try std.testing.expect(empty == null);
}

test "sequencer: epoch and sequence numbers advance" {
    const alloc = std.testing.allocator;
    var seq = Sequencer.init();
    defer seq.deinit(alloc);

    try seq.submit(alloc, makeTxn(0, .put, 0, 10, &.{}, &.{}));
    var b1 = (try seq.drainBatch(alloc)).?;
    defer b1.deinit(alloc);

    try seq.submit(alloc, makeTxn(1, .put, 0, 20, &.{}, &.{}));
    try seq.submit(alloc, makeTxn(2, .delete, 0, 30, &.{}, &.{}));
    var b2 = (try seq.drainBatch(alloc)).?;
    defer b2.deinit(alloc);

    try std.testing.expectEqual(@as(u64, 0), b1.epoch);
    try std.testing.expectEqual(@as(u64, 1), b2.epoch);
    try std.testing.expectEqual(@as(u64, 0), b1.sequence_start);
    try std.testing.expectEqual(@as(u64, 1), b2.sequence_start); // b1 had 1 txn
    try std.testing.expectEqual(@as(usize, 2), b2.transactions.len);
}

test "sequencer: detect write-write conflicts" {
    const alloc = std.testing.allocator;

    const ws_a = [_]u64{ 100, 200 };
    const ws_b = [_]u64{ 200, 300 }; // overlaps with a on key 200
    const ws_c = [_]u64{400};

    var txns = [_]Transaction{
        makeTxn(0, .put, 0, 0, &.{}, &ws_a),
        makeTxn(1, .put, 0, 0, &.{}, &ws_b),
        makeTxn(2, .put, 0, 0, &.{}, &ws_c),
    };

    const batch = Batch{
        .epoch = 0,
        .sequence_start = 0,
        .transactions = &txns,
    };

    const conflicts = try Sequencer.detectConflicts(&batch, alloc);
    defer alloc.free(conflicts);

    // Only txn 0 and txn 1 conflict (key 200).
    try std.testing.expectEqual(@as(usize, 1), conflicts.len);
    try std.testing.expectEqual(@as(usize, 0), conflicts[0][0]);
    try std.testing.expectEqual(@as(usize, 1), conflicts[0][1]);
}

test "sequencer: resolve conflicts removes later txn" {
    const alloc = std.testing.allocator;
    var seq = Sequencer.init();
    defer seq.deinit(alloc);

    const ws_shared = [_]u64{42};
    const ws_unique = [_]u64{99};

    try seq.submit(alloc, makeTxn(0, .put, 0, 0, &.{}, &ws_shared));
    try seq.submit(alloc, makeTxn(1, .put, 0, 0, &.{}, &ws_shared));
    try seq.submit(alloc, makeTxn(2, .put, 0, 0, &.{}, &ws_unique));

    var batch = (try seq.drainBatch(alloc)).?;
    defer batch.deinit(alloc);

    try Sequencer.resolveConflicts(&batch, alloc);

    // txn 1 removed (later conflict), txn 0 and txn 2 remain.
    try std.testing.expectEqual(@as(usize, 2), batch.transactions.len);
    try std.testing.expectEqual(@as(u64, 0), batch.transactions[0].txn_id);
    try std.testing.expectEqual(@as(u64, 2), batch.transactions[1].txn_id);
}
