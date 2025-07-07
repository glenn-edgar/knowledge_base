const std = @import("std");

pub fn build(b: *std.Build) void {
    // Define target and optimization options
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    // Create a module for kb_memory_driver
    const kb_memory_module = b.createModule(.{
        .root_source_file = .{ .path = "src/kb_memory_driver/kb_memory_module.zig" },
    });

    // Create a static library for kb_memory_driver
    const lib = b.addStaticLibrary(.{
        .name = "kb_memory_driver",
        .root_source_file = .{ .path = "src/kb_memory_driver/kb_memory_module.zig" },
        .target = target,
        .optimize = optimize,
    });

    // Attach the module to the library
    lib.root_module.addImport("kb_memory_driver", kb_memory_module);

    // Install the library artifact
    b.installArtifact(lib);

    // Create a test executable for test_driver.zig
    const test_exe = b.addTest(.{
        .root_source_file = .{ .path = "tests/test_driver.zig" },
        .target = target,
        .optimize = optimize,
    });

    // Attach the module to the test executable
    test_exe.root_module.addImport("kb_memory_driver", kb_memory_module);

    // Define a test step
    const run_tests = b.addRunArtifact(test_exe);
    const test_step = b.step("test", "Run the tests");
    test_step.dependOn(&run_tests.step);

    // Optional: Install the test executable (for debugging)
    b.installArtifact(test_exe);
}

