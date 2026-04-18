const std = @import("std");
const compat = @import("compat");

pub const ResourceState = enum(u8) {
    deep_sleep,
    light_sleep,
    warm,
    hot,
};

pub const ActivityTracker = struct {
    last_query_ms: std.atomic.Value(i64),
    window_start_ms: std.atomic.Value(i64),
    queries_in_window: std.atomic.Value(u64),

    pub fn init() ActivityTracker {
        const now = compat.milliTimestamp();
        return .{
            .last_query_ms = std.atomic.Value(i64).init(now),
            .window_start_ms = std.atomic.Value(i64).init(now),
            .queries_in_window = std.atomic.Value(u64).init(0),
        };
    }

    pub fn recordQuery(self: *ActivityTracker) void {
        const now = compat.milliTimestamp();
        self.last_query_ms.store(now, .release);

        const start = self.window_start_ms.load(.acquire);
        if (now - start >= 1000) {
            self.window_start_ms.store(now, .release);
            self.queries_in_window.store(1, .release);
        } else {
            _ = self.queries_in_window.fetchAdd(1, .monotonic);
        }
    }

    pub fn queriesPerSecond(self: *const ActivityTracker) u64 {
        return self.queries_in_window.load(.acquire);
    }

    pub fn lastQueryMs(self: *const ActivityTracker) i64 {
        return self.last_query_ms.load(.acquire);
    }

    pub fn state(self: *const ActivityTracker) ResourceState {
        const now = compat.milliTimestamp();
        const idle_ms = now - self.last_query_ms.load(.acquire);
        const qps = self.queries_in_window.load(.acquire);
        if (idle_ms >= 30 * 60 * 1000) return .deep_sleep;
        if (idle_ms >= 60 * 1000) return .light_sleep;
        if (qps >= 100) return .hot;
        return .warm;
    }
};

test "activity tracker state machine transitions" {
    var tracker = ActivityTracker.init();
    tracker.last_query_ms.store(compat.milliTimestamp() - 31 * 60 * 1000, .release);
    try std.testing.expectEqual(ResourceState.deep_sleep, tracker.state());

    tracker.last_query_ms.store(compat.milliTimestamp() - 2 * 60 * 1000, .release);
    try std.testing.expectEqual(ResourceState.light_sleep, tracker.state());

    tracker.last_query_ms.store(compat.milliTimestamp(), .release);
    tracker.queries_in_window.store(150, .release);
    try std.testing.expectEqual(ResourceState.hot, tracker.state());

    tracker.queries_in_window.store(1, .release);
    try std.testing.expectEqual(ResourceState.warm, tracker.state());
}
