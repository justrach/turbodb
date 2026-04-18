/// TurboDB — End-to-end Calvin replication test
///
/// Simulates a 2-node cluster in a single process:
///   1. Opens two separate databases (leader + replica)
///   2. Writes go through leader's Sequencer
///   3. Batches are serialized, then deserialized on the replica
///   4. Both nodes execute deterministically
///   5. Verify both databases have identical state
const std = @import("std");
const collection_mod = @import("collection.zig");
const doc_mod = @import("doc.zig");
const sequencer = @import("replication/sequencer.zig");
const calvin = @import("replication/calvin.zig");
const runtime = @import("runtime");
const compat = @import("compat");

const Database = collection_mod.Database;
const Collection = collection_mod.Collection;
const Transaction = sequencer.Transaction;
const Sequencer = sequencer.Sequencer;
const CalvinExecutor = calvin.CalvinExecutor;

// ── Global state for executor callbacks ──────────────────────────────────────

var g_leader_db: ?*Database = null;
var g_replica_db: ?*Database = null;

fn leaderExecutor(txn: *const Transaction) anyerror!void {
    const db = g_leader_db orelse return error.NoDatabase;
    return applyTxn(db, txn);
}

fn replicaExecutor(txn: *const Transaction) anyerror!void {
    const db = g_replica_db orelse return error.NoDatabase;
    return applyTxn(db, txn);
}

fn applyTxn(db: *Database, txn: *const Transaction) !void {
    const data = txn.data;
    if (data.len == 0) return;

    // Unpack: [col_name_len:u8][col_name][key_len:u16 LE][key][value]
    var pos: usize = 0;
    const col_len: usize = data[pos];
    pos += 1;
    const col_name = data[pos..][0..col_len];
    pos += col_len;
    const key_len = std.mem.readInt(u16, data[pos..][0..2], .little);
    pos += 2;
    const key = data[pos..][0..key_len];
    pos += key_len;
    const value = data[pos..];

    const col = try db.collection(col_name);

    switch (txn.txn_type) {
        .put => { _ = try col.insert(key, value); },
        .delete => { _ = try col.delete(key); },
        .read => {},
    }
}

fn makeTxn(alloc: std.mem.Allocator, txn_id: u64, txn_type: sequencer.TxnType, col_name: []const u8, key: []const u8, value: []const u8) !Transaction {
    // Pack transaction data
    const data_len = 1 + col_name.len + 2 + key.len + value.len;
    const data = try alloc.alloc(u8, data_len);
    var pos: usize = 0;
    data[pos] = @intCast(col_name.len);
    pos += 1;
    @memcpy(data[pos..][0..col_name.len], col_name);
    pos += col_name.len;
    std.mem.writeInt(u16, data[pos..][0..2], @intCast(key.len), .little);
    pos += 2;
    @memcpy(data[pos..][0..key.len], key);
    pos += key.len;
    @memcpy(data[pos..][0..value.len], value);

    const key_hash = doc_mod.fnv1a(key);
    const ws = try alloc.alloc(u64, 1);
    ws[0] = key_hash;

    return Transaction{
        .txn_id = txn_id,
        .txn_type = txn_type,
        .partition_id = 0,
        .key_hash = key_hash,
        .data = data,
        .read_set = &.{},
        .write_set = ws,
    };
}

pub fn main(init: std.process.Init) !void {
    const alloc = init.gpa;
    runtime.setIo(init.io);

    // Clean up any previous test data
    compat.fs.cwdDeleteTree("/tmp/calvin_test_leader") catch {};
    compat.fs.cwdDeleteTree("/tmp/calvin_test_replica") catch {};

    // Create data directories
    compat.fs.makeDirAbsolute("/tmp/calvin_test_leader") catch {};
    compat.fs.makeDirAbsolute("/tmp/calvin_test_replica") catch {};

    // ── Step 1: Open two separate databases ──────────────────────────────
    std.debug.print("\n=== TurboDB Calvin Replication E2E Test ===\n\n", .{});

    std.debug.print("[1] Opening leader database...\n", .{});
    const leader_db = try Database.open(alloc, "/tmp/calvin_test_leader");
    defer leader_db.close();
    g_leader_db = leader_db;

    std.debug.print("[1] Opening replica database...\n", .{});
    const replica_db = try Database.open(alloc, "/tmp/calvin_test_replica");
    defer replica_db.close();
    g_replica_db = replica_db;

    // ── Step 2: Create sequencer + executors ─────────────────────────────
    std.debug.print("[2] Creating Calvin sequencer + executors...\n", .{});

    var seq = Sequencer.init();
    defer seq.deinit(alloc);

    var leader_exec = CalvinExecutor.init(alloc, 0); // node 0
    defer leader_exec.deinit();
    try leader_exec.setLocalPartitions(&.{0});

    var replica_exec = CalvinExecutor.init(alloc, 1); // node 1
    defer replica_exec.deinit();
    try replica_exec.setLocalPartitions(&.{0}); // same partition — full replica

    // ── Step 3: Submit transactions to sequencer ─────────────────────────
    std.debug.print("[3] Submitting 5 transactions to sequencer...\n", .{});

    try seq.submit(alloc, try makeTxn(alloc, 1, .put, "users", "alice", "{\"name\":\"Alice\",\"age\":30}"));
    try seq.submit(alloc, try makeTxn(alloc, 2, .put, "users", "bob", "{\"name\":\"Bob\",\"age\":25}"));
    try seq.submit(alloc, try makeTxn(alloc, 3, .put, "users", "charlie", "{\"name\":\"Charlie\",\"age\":35}"));
    try seq.submit(alloc, try makeTxn(alloc, 4, .put, "orders", "ord-001", "{\"user\":\"alice\",\"total\":99.99}"));
    try seq.submit(alloc, try makeTxn(alloc, 5, .put, "orders", "ord-002", "{\"user\":\"bob\",\"total\":42.50}"));

    std.debug.print("    Sequencer has {d} pending transactions\n", .{seq.pending.items.len});

    // ── Step 4: Drain batch from sequencer ───────────────────────────────
    std.debug.print("[4] Draining batch from sequencer...\n", .{});

    const maybe_batch = try seq.drainBatch(alloc);
    if (maybe_batch) |batch_val| {
        var batch = batch_val;
        defer {
            for (batch.transactions) |txn| {
                alloc.free(txn.data);
                alloc.free(txn.write_set);
            }
            alloc.free(batch.transactions);
        }

        std.debug.print("    Batch: epoch={d}, seq_start={d}, txn_count={d}\n", .{
            batch.epoch, batch.sequence_start, batch.transactions.len,
        });

        // ── Step 5: Serialize batch (simulates network transmission) ─────
        std.debug.print("[5] Serializing batch for network transmission...\n", .{});

        var serial_buf: [65536]u8 = undefined;
        const serial_len = try CalvinExecutor.serializeBatch(&batch, &serial_buf);
        std.debug.print("    Serialized batch: {d} bytes\n", .{serial_len});

        // ── Step 6: Execute on leader ────────────────────────────────────
        std.debug.print("[6] Executing batch on LEADER (node 0)...\n", .{});
        try leader_exec.executeBatch(&batch, &leaderExecutor);
        std.debug.print("    Leader executed epoch {d}\n", .{leader_exec.last_executed_epoch});

        // ── Step 7: Deserialize + execute on replica ─────────────────────
        std.debug.print("[7] Deserializing + executing batch on REPLICA (node 1)...\n", .{});
        var replica_batch = try CalvinExecutor.deserializeBatch(serial_buf[0..serial_len], alloc);
        defer replica_batch.deinitDeep(alloc);
        try replica_exec.executeBatch(&replica_batch, &replicaExecutor);
        std.debug.print("    Replica executed epoch {d}\n", .{replica_exec.last_executed_epoch});

        // ── Step 8: Verify both databases have identical state ───────────
        std.debug.print("\n[8] Verifying replication consistency...\n\n", .{});

        const checks = [_]struct { col: []const u8, key: []const u8 }{
            .{ .col = "users", .key = "alice" },
            .{ .col = "users", .key = "bob" },
            .{ .col = "users", .key = "charlie" },
            .{ .col = "orders", .key = "ord-001" },
            .{ .col = "orders", .key = "ord-002" },
        };

        var pass: u32 = 0;
        var fail: u32 = 0;

        for (checks) |check| {
            const leader_col = leader_db.collection(check.col) catch {
                std.debug.print("    FAIL: leader has no collection '{s}'\n", .{check.col});
                fail += 1;
                continue;
            };
            const replica_col = replica_db.collection(check.col) catch {
                std.debug.print("    FAIL: replica has no collection '{s}'\n", .{check.col});
                fail += 1;
                continue;
            };

            const leader_doc = leader_col.get(check.key);
            const replica_doc = replica_col.get(check.key);

            if (leader_doc != null and replica_doc != null) {
                if (std.mem.eql(u8, leader_doc.?.value, replica_doc.?.value)) {
                    std.debug.print("    PASS  {s}/{s} -> leader={s}  replica={s}\n", .{
                        check.col, check.key, leader_doc.?.value, replica_doc.?.value,
                    });
                    pass += 1;
                } else {
                    std.debug.print("    FAIL  {s}/{s} -> leader={s}  replica={s}\n", .{
                        check.col, check.key, leader_doc.?.value, replica_doc.?.value,
                    });
                    fail += 1;
                }
            } else {
                std.debug.print("    FAIL  {s}/{s} -> leader={any}  replica={any}\n", .{
                    check.col, check.key, leader_doc != null, replica_doc != null,
                });
                fail += 1;
            }
        }

        std.debug.print("\n=== Results: {d} passed, {d} failed ===\n", .{ pass, fail });
        if (fail == 0) {
            std.debug.print("Calvin replication: CONSISTENT -- both nodes have identical state.\n\n", .{});
        } else {
            std.debug.print("Calvin replication: INCONSISTENT -- {d} mismatches.\n\n", .{fail});
        }
    } else {
        std.debug.print("ERROR: No batch drained from sequencer!\n", .{});
    }

    // Cleanup
    compat.fs.cwdDeleteTree("/tmp/calvin_test_leader") catch {};
    compat.fs.cwdDeleteTree("/tmp/calvin_test_replica") catch {};
}
