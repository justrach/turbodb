const std = @import("std");
const crypto = @import("crypto.zig");
const runtime = @import("runtime");

pub const Op = enum(u8) {
    insert,
    update,
    delete,
};

pub const Event = struct {
    seq: u64,
    tenant_id: [64]u8,
    tenant_id_len: u8,
    collection: [64]u8,
    collection_len: u8,
    key: [128]u8,
    key_len: u8,
    value: [512]u8,
    value_len: u16,
    doc_id: u64,
    op: Op,

    pub fn tenant(self: *const Event) []const u8 {
        return self.tenant_id[0..self.tenant_id_len];
    }

    pub fn collectionName(self: *const Event) []const u8 {
        return self.collection[0..self.collection_len];
    }

    pub fn keySlice(self: *const Event) []const u8 {
        return self.key[0..self.key_len];
    }

    pub fn valueSlice(self: *const Event) []const u8 {
        return self.value[0..self.value_len];
    }
};

pub const Subscription = struct {
    id: u64,
    tenant_id: [64]u8,
    tenant_id_len: u8,
    collection: [64]u8,
    collection_len: u8,
    webhook_url: [256]u8,
    webhook_url_len: u16,
    secret: [64]u8,
    secret_len: u8,

    pub fn tenant(self: *const Subscription) []const u8 {
        return self.tenant_id[0..self.tenant_id_len];
    }

    pub fn collectionName(self: *const Subscription) []const u8 {
        return self.collection[0..self.collection_len];
    }

    pub fn webhook(self: *const Subscription) []const u8 {
        return self.webhook_url[0..self.webhook_url_len];
    }

    pub fn secretSlice(self: *const Subscription) []const u8 {
        return self.secret[0..self.secret_len];
    }
};

pub const Delivery = struct {
    subscription_id: u64,
    seq: u64,
    tenant_id: [64]u8,
    tenant_id_len: u8,
    collection: [64]u8,
    collection_len: u8,
    webhook_url: [256]u8,
    webhook_url_len: u16,
    signature_hex: [64]u8,
    payload: [1024]u8,
    payload_len: u16,

    pub fn tenant(self: *const Delivery) []const u8 {
        return self.tenant_id[0..self.tenant_id_len];
    }

    pub fn collectionName(self: *const Delivery) []const u8 {
        return self.collection[0..self.collection_len];
    }

    pub fn webhook(self: *const Delivery) []const u8 {
        return self.webhook_url[0..self.webhook_url_len];
    }

    pub fn payloadSlice(self: *const Delivery) []const u8 {
        return self.payload[0..self.payload_len];
    }
};

pub const CDCManager = struct {
    allocator: std.mem.Allocator,
    subscriptions: std.ArrayList(Subscription),
    pending: std.ArrayList(Event),
    deliveries: std.ArrayList(Delivery),
    next_subscription_id: std.atomic.Value(u64),
    next_seq: std.atomic.Value(u64),
    mu: std.Io.Mutex,
    cond: std.Io.Condition,
    running: std.atomic.Value(bool),
    worker: ?std.Thread,

    pub fn init(allocator: std.mem.Allocator) CDCManager {
        return .{
            .allocator = allocator,
            .subscriptions = .empty,
            .pending = .empty,
            .deliveries = .empty,
            .next_subscription_id = std.atomic.Value(u64).init(1),
            .next_seq = std.atomic.Value(u64).init(1),
            .mu = .init,
            .cond = .init,
            .running = std.atomic.Value(bool).init(false),
            .worker = null,
        };
    }

    pub fn deinit(self: *CDCManager) void {
        self.stop();
        self.subscriptions.deinit(self.allocator);
        self.pending.deinit(self.allocator);
        self.deliveries.deinit(self.allocator);
    }

    pub fn start(self: *CDCManager) !void {
        if (self.running.load(.acquire)) return;
        self.running.store(true, .release);
        self.worker = try std.Thread.spawn(.{}, workerMain, .{self});
    }

    pub fn stop(self: *CDCManager) void {
        if (!self.running.load(.acquire)) return;
        self.running.store(false, .release);
        self.cond.broadcast(runtime.io);
        if (self.worker) |t| t.join();
        self.worker = null;
    }

    pub fn registerWebhook(self: *CDCManager, tenant_id: []const u8, collection: []const u8, webhook_url: []const u8, secret: []const u8) !u64 {
        var sub = std.mem.zeroes(Subscription);
        sub.id = self.next_subscription_id.fetchAdd(1, .monotonic);
        fill(&sub.tenant_id, &sub.tenant_id_len, tenant_id);
        fill(&sub.collection, &sub.collection_len, collection);
        fillU16(&sub.webhook_url, &sub.webhook_url_len, webhook_url);
        fill(&sub.secret, &sub.secret_len, secret);

        self.mu.lockUncancelable(runtime.io);
        defer self.mu.unlock(runtime.io);
        try self.subscriptions.append(self.allocator, sub);
        return sub.id;
    }

    pub fn emit(self: *CDCManager, tenant_id: []const u8, collection: []const u8, key: []const u8, value: []const u8, doc_id: u64, op: Op) void {
        var ev = std.mem.zeroes(Event);
        ev.seq = self.next_seq.fetchAdd(1, .monotonic);
        ev.doc_id = doc_id;
        ev.op = op;
        fill(&ev.tenant_id, &ev.tenant_id_len, tenant_id);
        fill(&ev.collection, &ev.collection_len, collection);
        fill(&ev.key, &ev.key_len, key);
        fillU16(&ev.value, &ev.value_len, value);

        self.mu.lockUncancelable(runtime.io);
        defer self.mu.unlock(runtime.io);
        self.pending.append(self.allocator, ev) catch return;
        self.cond.signal(runtime.io);
    }

    pub fn listDeliveries(self: *CDCManager, alloc: std.mem.Allocator, tenant_filter: ?[]const u8) ![]Delivery {
        self.mu.lockUncancelable(runtime.io);
        defer self.mu.unlock(runtime.io);
        var out: std.ArrayList(Delivery) = .empty;
        errdefer out.deinit(alloc);
        for (self.deliveries.items) |entry| {
            if (tenant_filter) |tenant| {
                if (!std.mem.eql(u8, entry.tenant(), tenant)) continue;
            }
            try out.append(alloc, entry);
        }
        return out.toOwnedSlice(alloc);
    }

    fn workerMain(self: *CDCManager) void {
        while (true) {
            self.mu.lockUncancelable(runtime.io);
            while (self.pending.items.len == 0 and self.running.load(.acquire)) {
                self.cond.waitUncancelable(runtime.io, &self.mu);
            }
            if (self.pending.items.len == 0 and !self.running.load(.acquire)) {
                self.mu.unlock(runtime.io);
                return;
            }
            const ev = self.pending.orderedRemove(0);
            const subs = self.subscriptions.items;
            self.mu.unlock(runtime.io);

            for (subs) |sub| {
                if (!matches(sub, ev)) continue;
                const delivery = makeDelivery(sub, ev);
                self.mu.lockUncancelable(runtime.io);
                self.deliveries.append(self.allocator, delivery) catch {};
                if (self.deliveries.items.len > 4096) {
                    _ = self.deliveries.orderedRemove(0);
                }
                self.mu.unlock(runtime.io);
            }
        }
    }
};

fn makeDelivery(sub: Subscription, ev: Event) Delivery {
    var delivery = std.mem.zeroes(Delivery);
    delivery.subscription_id = sub.id;
    delivery.seq = ev.seq;
    fill(&delivery.tenant_id, &delivery.tenant_id_len, ev.tenant());
    fill(&delivery.collection, &delivery.collection_len, ev.collectionName());
    fillU16(&delivery.webhook_url, &delivery.webhook_url_len, sub.webhook());

    const op_str = switch (ev.op) {
        .insert => "insert",
        .update => "update",
        .delete => "delete",
    };
    const payload = std.fmt.bufPrint(
        &delivery.payload,
        "{{\"seq\":{d},\"tenant\":\"{s}\",\"collection\":\"{s}\",\"op\":\"{s}\",\"key\":\"{s}\",\"doc_id\":{d},\"value\":{s}}}",
        .{ ev.seq, ev.tenant(), ev.collectionName(), op_str, ev.keySlice(), ev.doc_id, if (ev.value_len > 0) ev.valueSlice() else "null" },
    ) catch "{}";
    delivery.payload_len = @intCast(payload.len);
    delivery.signature_hex = crypto.hmacSha256Hex(sub.secretSlice(), payload);
    return delivery;
}

fn matches(sub: Subscription, ev: Event) bool {
    if (!std.mem.eql(u8, sub.tenant(), ev.tenant())) return false;
    if (sub.collection_len == 0) return true;
    return std.mem.eql(u8, sub.collectionName(), ev.collectionName());
}

fn fill(dest: anytype, len_ptr: anytype, src: []const u8) void {
    const n = @min(dest.len, src.len);
    @memcpy(dest[0..n], src[0..n]);
    len_ptr.* = @intCast(n);
}

fn fillU16(dest: anytype, len_ptr: *u16, src: []const u8) void {
    const n = @min(dest.len, src.len);
    @memcpy(dest[0..n], src[0..n]);
    len_ptr.* = @intCast(n);
}

test "cdc filters by tenant and collection and signs payload" {
    const alloc = std.testing.allocator;
    var cdc = CDCManager.init(alloc);
    defer cdc.deinit();
    try cdc.start();

    _ = try cdc.registerWebhook("tenant-a", "users", "memory://users", "secret-a");
    _ = try cdc.registerWebhook("tenant-b", "", "memory://all", "secret-b");

    cdc.emit("tenant-a", "users", "u1", "{\"name\":\"alice\"}", 1, .insert);
    cdc.emit("tenant-a", "orders", "o1", "{\"id\":1}", 2, .insert);
    cdc.emit("tenant-b", "users", "u2", "{\"name\":\"bob\"}", 3, .update);
    std.Thread.sleep(20_000_000);

    const a = try cdc.listDeliveries(alloc, "tenant-a");
    defer alloc.free(a);
    try std.testing.expectEqual(@as(usize, 1), a.len);
    try std.testing.expect(std.mem.indexOf(u8, a[0].payloadSlice(), "\"op\":\"insert\"") != null);
    try std.testing.expectEqualStrings("tenant-a", a[0].tenant());

    const b = try cdc.listDeliveries(alloc, "tenant-b");
    defer alloc.free(b);
    try std.testing.expectEqual(@as(usize, 1), b.len);
    const expected = crypto.hmacSha256Hex("secret-b", b[0].payloadSlice());
    try std.testing.expectEqualStrings(&expected, &b[0].signature_hex);
}

test "cdc preserves event order under queueing" {
    const alloc = std.testing.allocator;
    var cdc = CDCManager.init(alloc);
    defer cdc.deinit();
    try cdc.start();

    _ = try cdc.registerWebhook("tenant-a", "", "memory://all", "secret");
    cdc.emit("tenant-a", "users", "u1", "{\"v\":1}", 1, .insert);
    cdc.emit("tenant-a", "users", "u1", "{\"v\":2}", 1, .update);
    cdc.emit("tenant-a", "users", "u1", "", 1, .delete);
    std.Thread.sleep(20_000_000);

    const deliveries = try cdc.listDeliveries(alloc, "tenant-a");
    defer alloc.free(deliveries);
    try std.testing.expectEqual(@as(usize, 3), deliveries.len);
    try std.testing.expect(deliveries[0].seq < deliveries[1].seq);
    try std.testing.expect(deliveries[1].seq < deliveries[2].seq);
}
