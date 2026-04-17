/// ZagDB — `zag` CLI tool
///
/// Usage: zag <command> [args]
///
/// Commands:
///   init                     Create zag.json in current directory
///   add <package>            Add dependency
///   remove <package>         Remove dependency
///   install                  Resolve deps, download, verify, extract
///   publish                  Pack source, sign, upload to registry
///   search <query>           Search packages
///   yank <name> <version>    Yank a published version
///   audit                    Verify all dep signatures and hashes
///   keygen                   Generate Ed25519 keypair
const std = @import("std");
const manifest_mod = @import("manifest.zig");
const sign_mod = @import("sign.zig");
const hash_mod = @import("hash.zig");
const registry_mod = @import("registry.zig");
const resolver_mod = @import("resolver.zig");
const config_mod = @import("config.zig");
const auth_mod = @import("auth.zig");
const runtime = @import("runtime");
const compat = @import("compat");

const DEFAULT_REGISTRY = "http://localhost:8080";
const ZAG_DIR = ".zag";
const GLOBAL_DIR = ".zag"; // ~/.zag/

pub fn main(init: std.process.Init) !void {
    const alloc = init.gpa;
    runtime.setIo(init.io);

    const args = try compat.argsAlloc(alloc, init.minimal.args);
    defer compat.argsFree(alloc, args);

    if (args.len < 2) {
        printUsage();
        return;
    }

    const cmd = args[1];
    if (std.mem.eql(u8, cmd, "init")) {
        try cmdInit(alloc, args[2..]);
    } else if (std.mem.eql(u8, cmd, "keygen")) {
        try cmdKeygen();
    } else if (std.mem.eql(u8, cmd, "search")) {
        try cmdSearch(alloc, args[2..]);
    } else if (std.mem.eql(u8, cmd, "publish")) {
        try cmdPublish(alloc);
    } else if (std.mem.eql(u8, cmd, "audit")) {
        try cmdAudit(alloc);
    } else if (std.mem.eql(u8, cmd, "--help") or std.mem.eql(u8, cmd, "-h") or std.mem.eql(u8, cmd, "help")) {
        printUsage();
    } else {
        std.debug.print("Unknown command: {s}\n\n", .{cmd});
        printUsage();
    }
}

fn printUsage() void {
    std.debug.print(
        \\zag — Package manager for the Zag language
        \\
        \\Usage: zag <command> [args]
        \\
        \\Commands:
        \\  init                  Create zag.json in current directory
        \\  keygen                Generate Ed25519 keypair (~/.zag/keys/)
        \\  search <query>        Search packages on the registry
        \\  publish               Publish current package to registry
        \\  audit                 Verify all dependency hashes
        \\  help                  Show this help
        \\
        \\Local Layout:
        \\  zag.json              Package manifest
        \\  .zag/deps/            Extracted dependencies (flat)
        \\  .zag/cache/           Downloaded tarballs (content-addressed)
        \\  .zag/lock.json        Resolved dependency lockfile
        \\
        \\Global Config:
        \\  ~/.zag/keys/          Ed25519 keypair (default.pub, default.sec)
        \\  ~/.zag/config.json    Registry URL, mirrors, defaults
        \\
    , .{});
}

// ─── Commands ───────────────────────────────────────────────────────────────

fn cmdInit(alloc: std.mem.Allocator, args: []const []const u8) !void {
    _ = alloc;
    var name: []const u8 = "my-package";
    var org: ?[]const u8 = null;
    var private = false;

    var i: usize = 0;
    while (i < args.len) : (i += 1) {
        if (std.mem.eql(u8, args[i], "--org") and i + 1 < args.len) {
            i += 1;
            org = args[i];
        } else if (std.mem.eql(u8, args[i], "--private")) {
            private = true;
        } else {
            name = args[i];
        }
    }

    // Check if zag.json already exists
    compat.fs.cwdAccess("zag.json", .{}) catch |e| {
        if (e == error.FileNotFound) {
            var buf: [2048]u8 = undefined;
            var fbs = std.io.fixedBufferStream(&buf);
            const w = fbs.writer();

            try w.writeAll("{\n");
            try std.fmt.format(w, "  \"name\": \"{s}\",\n", .{name});
            try w.writeAll("  \"version\": \"0.1.0\",\n");
            try w.writeAll("  \"description\": \"\",\n");
            try std.fmt.format(w, "  \"visibility\": \"{s}\",\n", .{if (private) "private" else "public"});
            if (org) |o| {
                try std.fmt.format(w, "  \"org\": \"{s}\",\n", .{o});
            }
            try w.writeAll("  \"license\": \"MIT\",\n");
            try w.writeAll("  \"zig_version\": \"0.15.0\",\n");
            try w.writeAll("  \"dependencies\": {},\n");
            try w.writeAll("  \"dev_dependencies\": {}\n");
            try w.writeAll("}");

            const content = fbs.getWritten();
            const file = try compat.fs.cwdCreateFile("zag.json", .{});
            defer file.close();
            try file.writeAll(content);

            if (org) |o| {
                std.debug.print("Created zag.json for @{s}/{s} ({s})\n", .{ o, name, if (private) "private" else "public" });
            } else {
                std.debug.print("Created zag.json for \"{s}\" ({s})\n", .{ name, if (private) "private" else "public" });
            }
            return;
        }
        return e;
    };
    std.debug.print("zag.json already exists\n", .{});
}

fn cmdKeygen() !void {
    const home = std.posix.getenv("HOME") orelse "/tmp";
    var path_buf: [512]u8 = undefined;
    const keys_dir = try std.fmt.bufPrint(&path_buf, "{s}/.zag/keys", .{home});

    const kp = sign_mod.KeyPair.generate();
    try sign_mod.saveKeyPair(kp, keys_dir);

    const pk_hex = sign_mod.pubkeyHex(kp.public_key);
    std.debug.print("Generated Ed25519 keypair\n", .{});
    std.debug.print("  Public key: {s}\n", .{&pk_hex});
    std.debug.print("  Saved to:   {s}/\n", .{keys_dir});
}

fn cmdSearch(alloc: std.mem.Allocator, args: []const []const u8) !void {
    if (args.len == 0) {
        std.debug.print("Usage: zag search <query>\n", .{});
        return;
    }
    const query = args[0];

    // For MVP: search a local registry
    const tmp_dir = "/tmp/zagdb-cli-search";
    var reg = registry_mod.Registry.init(alloc, tmp_dir) catch {
        std.debug.print("No registry available. Start zagdb first.\n", .{});
        return;
    };
    defer reg.deinit();

    var results: [20]registry_mod.PackageInfo = undefined;
    const count = reg.search(query, 20, &results) catch 0;

    if (count == 0) {
        std.debug.print("No packages found for \"{s}\"\n", .{query});
        return;
    }

    std.debug.print("Found {d} package(s):\n\n", .{count});
    for (0..count) |i| {
        std.debug.print("  {s} @ {s}\n", .{ results[i].name, results[i].version });
        if (results[i].description.len > 0) {
            std.debug.print("    {s}\n", .{results[i].description});
        }
    }
}

fn cmdPublish(alloc: std.mem.Allocator) !void {
    // Read zag.json
    const manifest_content = compat.fs.cwdReadFileAlloc(alloc, "zag.json", 64 * 1024) catch {
        std.debug.print("No zag.json found. Run 'zag init' first.\n", .{});
        return;
    };
    defer alloc.free(manifest_content);

    // Parse manifest
    const m = manifest_mod.parse(alloc, manifest_content) catch {
        std.debug.print("Invalid zag.json\n", .{});
        return;
    };

    // Hash the source tree
    const tree_result = hash_mod.hashSourceTree(alloc, ".") catch {
        std.debug.print("Failed to hash source tree\n", .{});
        return;
    };
    var hash_hex: [64]u8 = undefined;
    hash_mod.hexEncode(tree_result.hash, &hash_hex);

    std.debug.print("Publishing {s}@{s}\n", .{ m.name, m.version });
    std.debug.print("  Source hash: {s}\n", .{&hash_hex});
    std.debug.print("  TODO: Upload to registry\n", .{});
}

fn cmdAudit(alloc: std.mem.Allocator) !void {
    // Read lockfile
    const lockfile = compat.fs.cwdReadFileAlloc(alloc, ".zag/lock.json", 1024 * 1024) catch {
        std.debug.print("No .zag/lock.json found. Run 'zag install' first.\n", .{});
        return;
    };
    defer alloc.free(lockfile);

    std.debug.print("Auditing dependencies...\n", .{});
    std.debug.print("  Lockfile: {d} bytes\n", .{lockfile.len});
    std.debug.print("  TODO: Verify hashes and signatures\n", .{});
}

fn cmdLogin(args: []const []const u8) !void {
    if (args.len == 0) {
        std.debug.print("Usage: zag login <registry-url>\n", .{});
        return;
    }
    const url = args[0];

    // Generate or load keypair
    const home = std.posix.getenv("HOME") orelse "/tmp";
    var keys_buf: [512]u8 = undefined;
    const keys_dir = try std.fmt.bufPrint(&keys_buf, "{s}/.zag/keys", .{home});

    const kp = sign_mod.loadKeyPair(keys_dir) catch blk: {
        std.debug.print("No keypair found. Generating one...\n", .{});
        const new_kp = sign_mod.KeyPair.generate();
        try sign_mod.saveKeyPair(new_kp, keys_dir);
        break :blk new_kp;
    };

    const pk_hex = sign_mod.pubkeyHex(kp.public_key);

    // Save registry config
    const config = config_mod.Config{
        .registries = &.{
            .{ .name = "default", .url = url, .pubkey_hex = &pk_hex },
        },
        .default_registry = "default",
    };
    try config_mod.saveGlobalConfig(config);

    std.debug.print("Logged in to {s}\n", .{url});
    std.debug.print("  Public key: {s}\n", .{&pk_hex});
    std.debug.print("  Config saved to ~/.zag/config.json\n", .{});
}

fn cmdLogout(args: []const []const u8) !void {
    if (args.len == 0) {
        std.debug.print("Usage: zag logout <registry-url>\n", .{});
        return;
    }
    const url = args[0];

    // Save config without auth key
    const config = config_mod.Config{
        .registries = &.{
            .{ .name = "default", .url = url },
        },
        .default_registry = "default",
    };
    try config_mod.saveGlobalConfig(config);

    std.debug.print("Logged out from {s}\n", .{url});
    std.debug.print("  Auth key removed from ~/.zag/config.json\n", .{});
}

// ─── Tests ──────────────────────────────────────────────────────────────────

test "cli module compiles" {
    _ = manifest_mod;
    _ = sign_mod;
    _ = hash_mod;
    _ = registry_mod;
    _ = resolver_mod;
    _ = config_mod;
    _ = auth_mod;
}
