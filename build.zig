const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    // ── Storage modules (WAL, mmap, epoch, seqlock) ─────────────────────────
    const mmap_mod = b.createModule(.{
        .root_source_file = b.path("src/storage/mmap.zig"),
        .target = target,
        .optimize = optimize,
    });
    const seqlock_mod = b.createModule(.{
        .root_source_file = b.path("src/storage/seqlock.zig"),
        .target = target,
        .optimize = optimize,
    });
    const epoch_mod = b.createModule(.{
        .root_source_file = b.path("src/storage/epoch.zig"),
        .target = target,
        .optimize = optimize,
    });
    const wal_mod = b.createModule(.{
        .root_source_file = b.path("src/storage/wal.zig"),
        .target = target,
        .optimize = optimize,
    });

    // ── TurboDB executable ──────────────────────────────────────────────────
    const turbodb_mod = b.createModule(.{
        .root_source_file = b.path("src/main.zig"),
        .target = target,
        .optimize = optimize,
        .link_libc = true,
    });
    turbodb_mod.addImport("mmap", mmap_mod);
    turbodb_mod.addImport("wal", wal_mod);
    turbodb_mod.addImport("epoch", epoch_mod);
    turbodb_mod.addImport("seqlock", seqlock_mod);

    const turbodb = b.addExecutable(.{
        .name = "turbodb",
        .root_module = turbodb_mod,
    });
    b.installArtifact(turbodb);

    // ── Run step ────────────────────────────────────────────────────────────
    const run_cmd = b.addRunArtifact(turbodb);
    run_cmd.step.dependOn(b.getInstallStep());
    if (b.args) |args| run_cmd.addArgs(args);
    const run_step = b.step("run", "Run TurboDB server");
    run_step.dependOn(&run_cmd.step);
}
