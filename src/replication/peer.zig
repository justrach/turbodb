/// TurboDB — Calvin Peer Communication Layer
///
/// Handles inter-node batch broadcast over TCP. The leader's sequencer
/// produces batches, which are serialized and sent to all replica peers.
/// Each peer deserializes the batch and feeds it to its CalvinExecutor.
///
/// Protocol: simple framed TCP — [4-byte big-endian length][batch payload]
const std = @import("std");
const runtime = @import("runtime");
const sequencer = @import("sequencer.zig");
const calvin = @import("calvin.zig");

pub const MAX_PEERS = 16;
pub const PEER_PORT_OFFSET: u16 = 100; // repl port = base_port + 100

/// A peer node's address.
pub const PeerAddr = struct {
    host: [64]u8,
    host_len: u8,
    port: u16,

    pub fn hostSlice(self: *const PeerAddr) []const u8 {
        return self.host[0..self.host_len];
    }
};

/// Manages outbound connections to peer replicas (used by leader).
pub const PeerSender = struct {
    peers: [MAX_PEERS]PeerAddr = undefined,
    count: u32 = 0,

    pub fn addPeer(self: *PeerSender, host: []const u8, port: u16) void {
        if (self.count >= MAX_PEERS) return;
        var addr = PeerAddr{
            .host = undefined,
            .host_len = @intCast(@min(host.len, 64)),
            .port = port,
        };
        @memcpy(addr.host[0..addr.host_len], host[0..addr.host_len]);
        self.peers[self.count] = addr;
        self.count += 1;
    }

    /// Broadcast a serialized batch to all peers. Best-effort — failures are logged.
    pub fn broadcast(self: *PeerSender, payload: []const u8) void {
        for (self.peers[0..self.count]) |*peer| {
            sendToPeer(peer, payload) catch {
                std.log.warn("Failed to send batch to {s}:{d}", .{ peer.hostSlice(), peer.port });
            };
        }
    }

    fn sendToPeer(peer: *const PeerAddr, payload: []const u8) !void {
        const addr = try std.Io.net.IpAddress.parse(peer.hostSlice(), peer.port);
        const stream = try std.Io.net.IpAddress.connect(&addr, runtime.io, .{ .mode = .stream });
        defer stream.close(runtime.io);

        var write_buf: [4096]u8 = undefined;
        var writer = stream.writer(runtime.io, &write_buf);
        var len_buf: [4]u8 = undefined;
        std.mem.writeInt(u32, &len_buf, @intCast(payload.len), .big);
        try writer.interface.writeAll(&len_buf);
        try writer.interface.writeAll(payload);
        try writer.interface.flush();
    }
};

/// Listens for inbound batch frames from the leader (used by replicas).
pub const PeerReceiver = struct {
    port: u16,
    running: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),
    executor: *calvin.CalvinExecutor,
    exec_fn: *const fn (txn: *const sequencer.Transaction) anyerror!void,
    alloc: std.mem.Allocator,

    pub fn init(
        alloc: std.mem.Allocator,
        port: u16,
        executor: *calvin.CalvinExecutor,
        exec_fn: *const fn (txn: *const sequencer.Transaction) anyerror!void,
    ) PeerReceiver {
        return .{
            .port = port,
            .executor = executor,
            .exec_fn = exec_fn,
            .alloc = alloc,
        };
    }

    pub fn run(self: *PeerReceiver) !void {
        const addr = try std.Io.net.IpAddress.parse("0.0.0.0", self.port);
        var listener = try std.Io.net.IpAddress.listen(&addr, runtime.io, .{ .reuse_address = true });
        defer listener.deinit(runtime.io);

        self.running.store(true, .release);
        std.log.info("Calvin peer receiver on :{d}", .{self.port});

        while (self.running.load(.acquire)) {
            const stream = listener.accept(runtime.io) catch continue;
            const t = std.Thread.spawn(.{}, handlePeerConn, .{ self, stream }) catch {
                stream.close(runtime.io);
                continue;
            };
            t.detach();
        }
    }

    pub fn stop(self: *PeerReceiver) void {
        self.running.store(false, .release);
    }

    fn handlePeerConn(self: *PeerReceiver, stream: std.Io.net.Stream) void {
        defer stream.close(runtime.io);

        var read_buf: [4096]u8 = undefined;
        var reader = stream.reader(runtime.io, &read_buf);

        // Read length-prefixed frame
        var len_buf: [4]u8 = undefined;
        reader.interface.readSliceAll(&len_buf) catch return;
        const payload_len = std.mem.readInt(u32, &len_buf, .big);
        if (payload_len > 4 * 1024 * 1024) return; // 4MB max batch

        const payload = self.alloc.alloc(u8, payload_len) catch return;
        defer self.alloc.free(payload);

        reader.interface.readSliceAll(payload) catch return;

        // Deserialize and execute the batch
        var batch = calvin.CalvinExecutor.deserializeBatch(payload, self.alloc) catch return;
        defer batch.deinitDeep(self.alloc);

        self.executor.executeBatch(&batch, self.exec_fn) catch |err| {
            std.log.err("Calvin batch execution failed: {}", .{err});
        };
    }
};

// ── Tests ────────────────────────────────────────────────────────────────────

test "PeerSender addPeer" {
    var sender = PeerSender{};
    sender.addPeer("127.0.0.1", 27117);
    sender.addPeer("127.0.0.1", 27217);
    try std.testing.expectEqual(@as(u32, 2), sender.count);
    try std.testing.expectEqualStrings("127.0.0.1", sender.peers[0].hostSlice());
    try std.testing.expectEqual(@as(u16, 27117), sender.peers[0].port);
}

test "PeerAddr host slice" {
    var addr = PeerAddr{ .host = undefined, .host_len = 9, .port = 8080 };
    @memcpy(addr.host[0..9], "127.0.0.1");
    try std.testing.expectEqualStrings("127.0.0.1", addr.hostSlice());
}
