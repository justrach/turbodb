const std = @import("std");
const shard_mod = @import("shard.zig");

pub const NodeId = shard_mod.NodeId;
pub const PartitionId = shard_mod.PartitionId;

/// A routed request targeting one or more shards.
pub const RoutedRequest = struct {
    request_type: RequestType,
    key: []const u8,
    value: []const u8,
    filter: ?[]const u8, // JSON filter for queries
    target_partitions: []PartitionId,
};

pub const RequestType = enum(u8) {
    get,
    put,
    delete,
    scan,
    query,
};

/// Result from one shard.
pub const ShardResult = struct {
    partition_id: PartitionId,
    node_id: NodeId,
    status: ResultStatus,
    data: []const u8,
    doc_count: u32,
};

pub const ResultStatus = enum(u8) { ok, not_found, error_status };

/// Cross-shard query router.
pub const Router = struct {
    shard_mgr: *shard_mod.ShardManager,
    n_partitions: u16,
    alloc: std.mem.Allocator,

    pub fn init(alloc: std.mem.Allocator, shard_mgr: *shard_mod.ShardManager, n_partitions: u16) Router {
        return .{
            .shard_mgr = shard_mgr,
            .n_partitions = n_partitions,
            .alloc = alloc,
        };
    }

    pub fn deinit(self: *Router) void {
        _ = self;
    }

    /// Route a single-key request to the correct partition via modular hashing.
    pub fn routeKey(self: *Router, key_hash: u64) PartitionId {
        return @intCast(key_hash % @as(u64, self.n_partitions));
    }

    /// Determine which partitions a scan/query touches.
    /// If no filter or filter doesn't reference partition key: all partitions.
    /// If filter contains "partition_id" field: prune to the referenced partition.
    pub fn planQuery(self: *Router, filter: ?[]const u8) ![]PartitionId {
        if (filter) |f| {
            // Simple heuristic: if the filter contains "partition_id:" try to extract it.
            if (std.mem.indexOf(u8, f, "partition_id:")) |pos| {
                const start = pos + "partition_id:".len;
                if (start < f.len) {
                    const val = parseU16(f[start..]);
                    if (val) |pid| {
                        if (pid < self.n_partitions) {
                            const result = try self.alloc.alloc(PartitionId, 1);
                            result[0] = pid;
                            return result;
                        }
                    }
                }
            }
        }
        // No filter or no partition key in filter — return all partitions.
        const all = try self.alloc.alloc(PartitionId, self.n_partitions);
        for (0..self.n_partitions) |i| {
            all[i] = @intCast(i);
        }
        return all;
    }

    /// Execute a single-shard request (local dispatch stub).
    pub fn executeSingle(self: *Router, req: RoutedRequest) !ShardResult {
        if (req.target_partitions.len == 0) return error.NoTargetPartition;
        const pid = req.target_partitions[0];

        // Check if local via shard manager.
        if (!self.shard_mgr.isLocal(pid)) {
            return ShardResult{
                .partition_id = pid,
                .node_id = 0,
                .status = .error_status,
                .data = "not_local",
                .doc_count = 0,
            };
        }

        return ShardResult{
            .partition_id = pid,
            .node_id = self.shard_mgr.local_node_id,
            .status = .ok,
            .data = req.value,
            .doc_count = 1,
        };
    }

    /// Scatter-gather: fan out to multiple shards, collect results.
    pub fn scatter(self: *Router, req: RoutedRequest, results: []ShardResult) !usize {
        var count: usize = 0;
        for (req.target_partitions) |pid| {
            if (count >= results.len) break;

            var single_target = [_]PartitionId{pid};
            const single_req = RoutedRequest{
                .request_type = req.request_type,
                .key = req.key,
                .value = req.value,
                .filter = req.filter,
                .target_partitions = &single_target,
            };

            results[count] = self.executeSingle(single_req) catch |err| {
                results[count] = ShardResult{
                    .partition_id = pid,
                    .node_id = 0,
                    .status = .error_status,
                    .data = @errorName(err),
                    .doc_count = 0,
                };
                count += 1;
                continue;
            };
            count += 1;
        }
        return count;
    }

    /// Merge results from multiple shards by concatenating data with newline separators.
    pub fn mergeResults(results: []const ShardResult, alloc_: std.mem.Allocator) ![]const u8 {
        var total_len: usize = 0;
        for (results, 0..) |r, i| {
            if (r.status == .ok) {
                total_len += r.data.len;
                if (i < results.len - 1) total_len += 1; // newline separator
            }
        }

        if (total_len == 0) return try alloc_.alloc(u8, 0);

        const buf = try alloc_.alloc(u8, total_len);
        var offset: usize = 0;
        var first = true;
        for (results) |r| {
            if (r.status == .ok) {
                if (!first) {
                    buf[offset] = '\n';
                    offset += 1;
                }
                @memcpy(buf[offset .. offset + r.data.len], r.data);
                offset += r.data.len;
                first = false;
            }
        }
        return buf[0..offset];
    }

    /// Aggregation push-down: merge partial aggregates from shards.
    pub fn mergeAggregates(results: []const ShardResult, agg_type: AggType) AggResult {
        var agg = AggResult{
            .count = 0,
            .sum = 0.0,
            .min = std.math.floatMax(f64),
            .max = -std.math.floatMax(f64),
        };

        for (results) |r| {
            if (r.status != .ok) continue;

            agg.count += r.doc_count;

            // Parse numeric value from data if present.
            const val = parseF64(r.data) orelse continue;

            agg.sum += val;
            if (val < agg.min) agg.min = val;
            if (val > agg.max) agg.max = val;
        }

        _ = agg_type; // All fields are populated regardless; caller picks what they need.

        // Fix min/max if no values were seen.
        if (agg.count == 0) {
            agg.min = 0.0;
            agg.max = 0.0;
        }

        return agg;
    }

    pub const AggType = enum { count, sum, avg, min, max };

    pub const AggResult = struct {
        count: u64,
        sum: f64,
        min: f64,
        max: f64,
    };

    fn parseU16(s: []const u8) ?u16 {
        var val: u16 = 0;
        for (s) |c| {
            if (c >= '0' and c <= '9') {
                val = val *% 10 +% (@as(u16, c) - '0');
            } else break;
        }
        if (s.len == 0) return null;
        if (s[0] < '0' or s[0] > '9') return null;
        return val;
    }

    fn parseF64(s: []const u8) ?f64 {
        if (s.len == 0) return null;
        return std.fmt.parseFloat(f64, s) catch null;
    }
};

// ─── Tests ──────────────────────────────────────────────────────────────────

test "Router: routeKey partitioning" {
    var mgr = shard_mod.ShardManager.init(std.testing.allocator, 1);
    defer mgr.deinit();

    var router = Router.init(std.testing.allocator, &mgr, 8);
    defer router.deinit();

    // Key hash 0 should map to partition 0.
    try std.testing.expectEqual(@as(PartitionId, 0), router.routeKey(0));
    // Key hash 7 should map to partition 7.
    try std.testing.expectEqual(@as(PartitionId, 7), router.routeKey(7));
    // Key hash 8 wraps to partition 0.
    try std.testing.expectEqual(@as(PartitionId, 0), router.routeKey(8));
    // Key hash 13 should map to partition 5.
    try std.testing.expectEqual(@as(PartitionId, 5), router.routeKey(13));
}

test "Router: planQuery returns all partitions for unfiltered scan" {
    var mgr = shard_mod.ShardManager.init(std.testing.allocator, 1);
    defer mgr.deinit();

    var router = Router.init(std.testing.allocator, &mgr, 4);
    defer router.deinit();

    // Null filter → all partitions.
    const all = try router.planQuery(null);
    defer std.testing.allocator.free(all);
    try std.testing.expectEqual(@as(usize, 4), all.len);
    try std.testing.expectEqual(@as(PartitionId, 0), all[0]);
    try std.testing.expectEqual(@as(PartitionId, 1), all[1]);
    try std.testing.expectEqual(@as(PartitionId, 2), all[2]);
    try std.testing.expectEqual(@as(PartitionId, 3), all[3]);
}

test "Router: planQuery prunes to specific partition" {
    var mgr = shard_mod.ShardManager.init(std.testing.allocator, 1);
    defer mgr.deinit();

    var router = Router.init(std.testing.allocator, &mgr, 8);
    defer router.deinit();

    const pruned = try router.planQuery("partition_id:3");
    defer std.testing.allocator.free(pruned);
    try std.testing.expectEqual(@as(usize, 1), pruned.len);
    try std.testing.expectEqual(@as(PartitionId, 3), pruned[0]);
}

test "Router: planQuery with non-partition filter returns all" {
    var mgr = shard_mod.ShardManager.init(std.testing.allocator, 1);
    defer mgr.deinit();

    var router = Router.init(std.testing.allocator, &mgr, 4);
    defer router.deinit();

    const all = try router.planQuery("category:books");
    defer std.testing.allocator.free(all);
    try std.testing.expectEqual(@as(usize, 4), all.len);
}

test "Router: mergeResults combines shard results" {
    const results = [_]ShardResult{
        .{ .partition_id = 0, .node_id = 1, .status = .ok, .data = "hello", .doc_count = 1 },
        .{ .partition_id = 1, .node_id = 2, .status = .ok, .data = "world", .doc_count = 1 },
    };

    const merged = try Router.mergeResults(&results, std.testing.allocator);
    defer std.testing.allocator.free(merged);

    try std.testing.expectEqualStrings("hello\nworld", merged);
}

test "Router: mergeResults skips error results" {
    const results = [_]ShardResult{
        .{ .partition_id = 0, .node_id = 1, .status = .ok, .data = "alpha", .doc_count = 1 },
        .{ .partition_id = 1, .node_id = 2, .status = .error_status, .data = "fail", .doc_count = 0 },
        .{ .partition_id = 2, .node_id = 3, .status = .ok, .data = "beta", .doc_count = 1 },
    };

    const merged = try Router.mergeResults(&results, std.testing.allocator);
    defer std.testing.allocator.free(merged);

    try std.testing.expectEqualStrings("alpha\nbeta", merged);
}

test "Router: mergeAggregates correctly combines counts and sums" {
    const results = [_]ShardResult{
        .{ .partition_id = 0, .node_id = 1, .status = .ok, .data = "10.5", .doc_count = 5 },
        .{ .partition_id = 1, .node_id = 2, .status = .ok, .data = "20.0", .doc_count = 3 },
        .{ .partition_id = 2, .node_id = 3, .status = .ok, .data = "5.0", .doc_count = 7 },
    };

    const agg = Router.mergeAggregates(&results, .sum);

    try std.testing.expectEqual(@as(u64, 15), agg.count); // 5 + 3 + 7
    try std.testing.expectApproxEqAbs(@as(f64, 35.5), agg.sum, 0.001); // 10.5 + 20.0 + 5.0
    try std.testing.expectApproxEqAbs(@as(f64, 5.0), agg.min, 0.001);
    try std.testing.expectApproxEqAbs(@as(f64, 20.0), agg.max, 0.001);
}

test "Router: mergeAggregates with no ok results" {
    const results = [_]ShardResult{
        .{ .partition_id = 0, .node_id = 1, .status = .error_status, .data = "err", .doc_count = 0 },
    };

    const agg = Router.mergeAggregates(&results, .count);

    try std.testing.expectEqual(@as(u64, 0), agg.count);
    try std.testing.expectEqual(@as(f64, 0.0), agg.min);
    try std.testing.expectEqual(@as(f64, 0.0), agg.max);
}

test "Router: mergeAggregates with mixed error and ok" {
    const results = [_]ShardResult{
        .{ .partition_id = 0, .node_id = 1, .status = .ok, .data = "42.0", .doc_count = 10 },
        .{ .partition_id = 1, .node_id = 2, .status = .error_status, .data = "err", .doc_count = 0 },
        .{ .partition_id = 2, .node_id = 3, .status = .ok, .data = "8.0", .doc_count = 2 },
    };

    const agg = Router.mergeAggregates(&results, .avg);

    try std.testing.expectEqual(@as(u64, 12), agg.count);
    try std.testing.expectApproxEqAbs(@as(f64, 50.0), agg.sum, 0.001);
    try std.testing.expectApproxEqAbs(@as(f64, 8.0), agg.min, 0.001);
    try std.testing.expectApproxEqAbs(@as(f64, 42.0), agg.max, 0.001);
}
