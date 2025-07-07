// build.zig - Build configuration with clean step

const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    // Create executable for the test driver
    const exe = b.addExecutable(.{
        .name = "math_test",
        .root_source_file = b.path("main.zig"),
        .target = target,
        .optimize = optimize,
    });

    // Add math module to the executable
    exe.root_module.addImport("math", b.createModule(.{
        .root_source_file = b.path("src/math/math.zig"),
    }));

    // Install the executable
    b.installArtifact(exe);

    // Add test step
    const test_step = b.step("test", "Run unit tests");
    const unit_tests = b.addTest(.{
        .root_source_file = b.path("src/math/math.zig"),
        .target = target,
        .optimize = optimize,
    });
    test_step.dependOn(&unit_tests.step);

    // Add run step
    const run_cmd = b.addRunArtifact(exe);
    const run_step = b.step("run", "Run the app");
    run_step.dependOn(&run_cmd.step);

    // Add clean step
    const clean_step = b.step("clean", "Remove build artifacts and cache");
    const remove_out = b.addRemoveDirTree(b.path("zig-out"));
    const remove_cache = b.addRemoveDirTree(b.path("zig-cache"));
    clean_step.dependOn(&remove_out.step);
    clean_step.dependOn(&remove_cache.step);
}

