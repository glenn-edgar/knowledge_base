// lib.zig - Root module for kb_memory_driver static library
//
// This file serves as the public API for the kb_memory_driver library.
// It exports all the necessary types and functions that clients can use.

const std = @import("std");

// Re-export all public APIs from the individual modules
pub const kb_memory_module = @import("kb_memory_module.zig");
pub const construct_mem_db = @import("construct_mem_db.zig");
pub const search_mem_db = @import("search_mem_db.zig");

// Re-export commonly used types for convenience
// From kb_memory_module.zig
pub const BasicConstructDB = kb_memory_module.BasicConstructDB;
pub const TreeNode = kb_memory_module.TreeNode;
pub const QueryResult = kb_memory_module.QueryResult;
pub const TreeStats = kb_memory_module.TreeStats;
pub const SyncStats = kb_memory_module.SyncStats;

// From construct_mem_db.zig
pub const ConstructMemDB = construct_mem_db.ConstructMemDB;

// From search_mem_db.zig  
pub const SearchMemDB = search_mem_db.SearchMemDB;

// Helper functions for JSON creation (if they exist in your modules)
// Uncomment these if your kb_memory_module.zig exports these functions
// pub const jsonString = kb_memory_module.jsonString;
// pub const jsonObject = kb_memory_module.jsonObject;

// Version information
pub const version = std.SemanticVersion{
    .major = 0,
    .minor = 1,
    .patch = 0,
};

// Library information
pub const lib_name = "kb_memory_driver";
pub const lib_description = "Knowledge Base Memory Driver - A comprehensive tree-structured data storage system with ltree compatibility";

// Test all modules to ensure they compile
test "kb_memory_driver library tests" {
    // Import and reference all modules to ensure they compile
    _ = @import("kb_memory_module.zig");
    _ = @import("construct_mem_db.zig");
    _ = @import("search_mem_db.zig");
}

// Convenience function to get library version as string
pub fn getVersionString(allocator: std.mem.Allocator) ![]const u8 {
    return std.fmt.allocPrint(allocator, "{}.{}.{}", .{ version.major, version.minor, version.patch });
}

// Library initialization function (if needed)
pub fn init() void {
    // Any global initialization can go here
}

// Library cleanup function (if needed)
pub fn deinit() void {
    // Any global cleanup can go here
}

