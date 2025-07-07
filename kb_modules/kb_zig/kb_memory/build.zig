const std = @import("std");

pub fn build(b: *std.Build) void {
    // Standard target options allows the person running `zig build` to choose
    // what target to build for. Here we do not override the defaults, which
    // means any target is allowed, and the default is native.
    const target = b.standardTargetOptions(.{});

    // Standard optimization options allow the person running `zig build` to select
    // between Debug, ReleaseSafe, ReleaseFast, and ReleaseSmall. Here we do not
    // set a preferred release mode, allowing the user to decide how to optimize.
    const optimize = b.standardOptimizeOption(.{});

    // Create a module for the kb_memory_driver library
    const kb_memory_module = b.addModule("kb_memory_driver", .{
        .root_source_file = b.path("src/kb_memory_driver/lib.zig"),
    });

    // Create the static library for kb_memory_driver
    const kb_memory_lib = b.addStaticLibrary(.{
        .name = "kb_memory_driver",
        .root_source_file = b.path("src/kb_memory_driver/lib.zig"),
        .target = target,
        .optimize = optimize,
    });

    // Install the library so it can be used by other projects
    b.installArtifact(kb_memory_lib);

    // Create the test executable
    const test_exe = b.addExecutable(.{
        .name = "test_driver",
        .root_source_file = b.path("tests/test_driver.zig"),
        .target = target,
        .optimize = optimize,
    });

    // Add the kb_memory_driver module to the test executable
    test_exe.root_module.addImport("kb_memory_driver", kb_memory_module);

    // Install the test executable
    b.installArtifact(test_exe);

    // Create a run step for the test executable
    const run_test = b.addRunArtifact(test_exe);
    
    // This allows the user to pass arguments to the application in the build
    // command itself, like this: `zig build run -- arg1 arg2 etc`
    if (b.args) |args| {
        run_test.addArgs(args);
    }

    // Create a build step for running the test
    const run_step = b.step("run", "Run the test driver");
    run_step.dependOn(&run_test.step);

    // Create unit tests for the library
    const lib_unit_tests = b.addTest(.{
        .root_source_file = b.path("src/kb_memory_driver/lib.zig"),
        .target = target,
        .optimize = optimize,
    });

    const run_lib_unit_tests = b.addRunArtifact(lib_unit_tests);

    // Create a build step for running library unit tests
    const test_step = b.step("test", "Run unit tests");
    test_step.dependOn(&run_lib_unit_tests.step);

    // Create separate test steps for individual modules
    const kb_memory_module_tests = b.addTest(.{
        .root_source_file = b.path("src/kb_memory_driver/kb_memory_module.zig"),
        .target = target,
        .optimize = optimize,
    });

    const construct_mem_db_tests = b.addTest(.{
        .root_source_file = b.path("src/kb_memory_driver/construct_mem_db.zig"),
        .target = target,
        .optimize = optimize,
    });

    const search_mem_db_tests = b.addTest(.{
        .root_source_file = b.path("src/kb_memory_driver/search_mem_db.zig"),
        .target = target,
        .optimize = optimize,
    });

    const run_kb_memory_tests = b.addRunArtifact(kb_memory_module_tests);
    const run_construct_tests = b.addRunArtifact(construct_mem_db_tests);
    const run_search_tests = b.addRunArtifact(search_mem_db_tests);

    // Create individual test steps
    const test_kb_memory_step = b.step("test-kb-memory", "Run kb_memory_module tests");
    test_kb_memory_step.dependOn(&run_kb_memory_tests.step);

    const test_construct_step = b.step("test-construct", "Run construct_mem_db tests");
    test_construct_step.dependOn(&run_construct_tests.step);

    const test_search_step = b.step("test-search", "Run search_mem_db tests");
    test_search_step.dependOn(&run_search_tests.step);

    // Make the main test step depend on all individual tests
    test_step.dependOn(&run_kb_memory_tests.step);
    test_step.dependOn(&run_construct_tests.step);
    test_step.dependOn(&run_search_tests.step);

    // Create a build step for building everything
    const build_all_step = b.step("build-all", "Build library and test executable");
    build_all_step.dependOn(&kb_memory_lib.step);
    build_all_step.dependOn(&test_exe.step);
}

