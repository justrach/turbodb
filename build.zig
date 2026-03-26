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

    // ── Helper: wire storage imports into a module ──────────────────────────
    const wireStorage = struct {
        fn f(mod: *std.Build.Module, mmap: *std.Build.Module, wal: *std.Build.Module, epoch: *std.Build.Module, seqlock: *std.Build.Module) void {
            mod.addImport("mmap", mmap);
            mod.addImport("wal", wal);
            mod.addImport("epoch", epoch);
            mod.addImport("seqlock", seqlock);
        }
    }.f;

    // ── TurboDB executable ──────────────────────────────────────────────────
    const turbodb_mod = b.createModule(.{
        .root_source_file = b.path("src/main.zig"),
        .target = target,
        .optimize = optimize,
        .link_libc = true,
    });
    wireStorage(turbodb_mod, mmap_mod, wal_mod, epoch_mod, seqlock_mod);

    const turbodb = b.addExecutable(.{
        .name = "turbodb",
        .root_module = turbodb_mod,
    });
    b.installArtifact(turbodb);

    // ── Shared library (FFI for Python/JS) ──────────────────────────────────
    const ffi_mod = b.createModule(.{
        .root_source_file = b.path("src/ffi.zig"),
        .target = target,
        .optimize = optimize,
        .link_libc = true,
    });
    wireStorage(ffi_mod, mmap_mod, wal_mod, epoch_mod, seqlock_mod);

    const lib = b.addLibrary(.{
        .linkage = .dynamic,
        .name = "turbodb",
        .root_module = ffi_mod,
    });
    b.installArtifact(lib);

    const lib_step = b.step("lib", "Build libturbodb shared library");
    lib_step.dependOn(&lib.step);

    // ── Run step ────────────────────────────────────────────────────────────
    const run_cmd = b.addRunArtifact(turbodb);
    run_cmd.step.dependOn(b.getInstallStep());
    if (b.args) |args| run_cmd.addArgs(args);
    const run_step = b.step("run", "Run TurboDB server");
    run_step.dependOn(&run_cmd.step);

    // ── Test step ───────────────────────────────────────────────────────────
    const test_mod = b.createModule(.{
        .root_source_file = b.path("src/doc.zig"),
        .target = target,
        .optimize = optimize,
    });
    const unit_tests = b.addTest(.{
        .name = "turbodb-tests",
        .root_module = test_mod,
    });
    const run_tests = b.addRunArtifact(unit_tests);
    const test_step = b.step("test", "Run unit tests");
    test_step.dependOn(&run_tests.step);
}
