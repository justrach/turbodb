/// ZagDB — Multi-registry configuration
///
/// Manages ~/.zag/config.json with registry URLs, auth keys, and defaults.
///
/// Config format:
/// {
///   "registries": {
///     "default": {"url": "https://registry.zag.dev"},
///     "internal": {"url": "https://zag.company.com", "pubkey": "a1b2..."}
///   },
///   "default_registry": "default",
///   "default_org": null
/// }
const std = @import("std");

pub const RegistryEntry = struct {
    name: []const u8,
    url: []const u8,
    pubkey_hex: ?[]const u8 = null, // auth key for this registry
};

pub const Config = struct {
    registries: []const RegistryEntry = &.{},
    default_registry: []const u8 = "default",
    default_org: ?[]const u8 = null,

    /// Get a registry entry by name.
    pub fn getRegistry(self: Config, name: []const u8) ?RegistryEntry {
        for (self.registries) |reg| {
            if (std.mem.eql(u8, reg.name, name)) return reg;
        }
        return null;
    }

    /// Get the default registry.
    pub fn getDefault(self: Config) ?RegistryEntry {
        return self.getRegistry(self.default_registry);
    }

    /// Serialize config to JSON.
    pub fn toJson(self: Config, buf: []u8) ![]const u8 {
        var fbs = std.io.fixedBufferStream(buf);
        const w = fbs.writer();

        try w.writeAll("{\"registries\":{");
        for (self.registries, 0..) |reg, i| {
            if (i > 0) try w.writeAll(",");
            try std.fmt.format(w, "\"{s}\":{{\"url\":\"{s}\"", .{ reg.name, reg.url });
            if (reg.pubkey_hex) |pk| {
                try std.fmt.format(w, ",\"pubkey\":\"{s}\"", .{pk});
            }
            try w.writeAll("}");
        }
        try std.fmt.format(w, "}},\"default_registry\":\"{s}\"", .{self.default_registry});
        if (self.default_org) |org| {
            try std.fmt.format(w, ",\"default_org\":\"{s}\"", .{org});
        } else {
            try w.writeAll(",\"default_org\":null");
        }
        try w.writeAll("}");
        return fbs.getWritten();
    }
};

/// Create a default config template.
pub fn defaultConfig() Config {
    return .{
        .registries = &.{
            .{ .name = "default", .url = "https://registry.zag.dev" },
        },
        .default_registry = "default",
        .default_org = null,
    };
}

/// Get the global config directory path.
pub fn globalConfigDir(buf: []u8) ![]const u8 {
    const home = std.posix.getenv("HOME") orelse "/tmp";
    return std.fmt.bufPrint(buf, "{s}/.zag", .{home});
}

/// Get the global config file path.
pub fn globalConfigPath(buf: []u8) ![]const u8 {
    const home = std.posix.getenv("HOME") orelse "/tmp";
    return std.fmt.bufPrint(buf, "{s}/.zag/config.json", .{home});
}

/// Save config to ~/.zag/config.json
pub fn saveGlobalConfig(config: Config) !void {
    var dir_buf: [512]u8 = undefined;
    const dir = try globalConfigDir(&dir_buf);
    std.fs.cwd().makePath(dir) catch {};

    var path_buf: [512]u8 = undefined;
    const path = try globalConfigPath(&path_buf);

    var json_buf: [4096]u8 = undefined;
    const json = try config.toJson(&json_buf);

    const file = try std.fs.cwd().createFile(path, .{});
    defer file.close();
    try file.writeAll(json);
    try file.writeAll("\n");
}

// ─── Tests ───────────────────────────────────────────────────────────────────

test "config toJson" {
    const regs = [_]RegistryEntry{
        .{ .name = "default", .url = "https://registry.zag.dev" },
        .{ .name = "internal", .url = "https://zag.company.com", .pubkey_hex = "abc123" },
    };
    const config = Config{
        .registries = &regs,
        .default_registry = "default",
        .default_org = "myorg",
    };

    var buf: [4096]u8 = undefined;
    const json = try config.toJson(&buf);

    try std.testing.expect(std.mem.indexOf(u8, json, "registry.zag.dev") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "zag.company.com") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "abc123") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "myorg") != null);
}

test "config getRegistry" {
    const regs = [_]RegistryEntry{
        .{ .name = "default", .url = "https://registry.zag.dev" },
        .{ .name = "internal", .url = "https://internal.co" },
    };
    const config = Config{ .registries = &regs };

    const def = config.getRegistry("default").?;
    try std.testing.expectEqualStrings("https://registry.zag.dev", def.url);

    const internal = config.getRegistry("internal").?;
    try std.testing.expectEqualStrings("https://internal.co", internal.url);

    try std.testing.expectEqual(@as(?RegistryEntry, null), config.getRegistry("nonexistent"));
}

test "config getDefault" {
    const regs = [_]RegistryEntry{
        .{ .name = "default", .url = "https://registry.zag.dev" },
    };
    const config = Config{ .registries = &regs, .default_registry = "default" };

    const def = config.getDefault().?;
    try std.testing.expectEqualStrings("https://registry.zag.dev", def.url);
}

test "default config" {
    const config = defaultConfig();
    try std.testing.expectEqualStrings("default", config.default_registry);
    try std.testing.expect(config.registries.len > 0);
}

test "config null org" {
    const regs = [_]RegistryEntry{
        .{ .name = "default", .url = "https://registry.zag.dev" },
    };
    const config = Config{ .registries = &regs, .default_org = null };

    var buf: [4096]u8 = undefined;
    const json = try config.toJson(&buf);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"default_org\":null") != null);
}
