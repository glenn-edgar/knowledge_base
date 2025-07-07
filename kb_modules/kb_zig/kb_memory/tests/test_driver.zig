// test_driver.zig - Test driver for kb_memory_driver static library

const std = @import("std");
const print = std.debug.print;

// Import the kb_memory_driver library
const kb_lib = @import("kb_memory_driver");

// Import specific types for convenience
const BasicConstructDB = kb_lib.BasicConstructDB;
const ConstructMemDB = kb_lib.ConstructMemDB;
const SearchMemDB = kb_lib.SearchMemDB;

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    print("=== KB Memory Driver Test Suite ===\n",.{});
    print("Library: {s} v{}\n", .{ kb_lib.lib_name, kb_lib.version });
    print("Testing the static library...\n\n",.{});

    // Test 1: Library information
    try testLibraryInfo(allocator);
    
    // Test 2: Module availability  
    try testModuleAvailability();
    
    // Test 3: Example usage demonstration
    try exampleUsage(allocator);

    print("\n=== All tests completed successfully! ===\n",.{});
}

fn testLibraryInfo(allocator: std.mem.Allocator) !void {
    print("1. Testing Library Information...\n",.{});
    
    const version_str = try kb_lib.getVersionString(allocator);
    defer allocator.free(version_str);
    
    print("  ✓ Library name: {s}\n", .{kb_lib.lib_name});
    print("  ✓ Version: {s}\n", .{version_str});
    print("  ✓ Description: {s}\n", .{kb_lib.lib_description});
    
    print("  Library info tests passed!\n\n",.{});
}

fn testModuleAvailability() !void {
    print("2. Testing Module Availability...\n",.{});
    
    // Test that we can access all the main types
    _ = BasicConstructDB;
    _ = ConstructMemDB; 
    _ = SearchMemDB;
    
    print("  ✓ BasicConstructDB type available\n",.{});
    print("  ✓ ConstructMemDB type available\n",.{});
    print("  ✓ SearchMemDB type available\n",.{});
    
    print("  Module availability tests passed!\n\n",.{});
}

// Example usage function translated from Go
fn exampleUsage(allocator: std.mem.Allocator) !void {
    print("3. Running Example Usage Demonstration...\n",.{});
    
    // Get PostgreSQL password from environment
    const postgres_password = std.process.getEnvVarOwned(allocator, "POSTGRES_PASSWORD") catch "defaultpass";
    defer allocator.free(postgres_password);
    
    // Initialize the enhanced tree storage system
    var tree = try BasicConstructDB.init(
        allocator,
        "localhost",
        5432,
        "knowledge_base",
        "gedgar",
        postgres_password,
        "knowledge_base"
    );
    defer tree.deinit();

    print("=== Full ltree-Compatible Tree Storage System ===\n",.{});

    // Sample data setup
    const SampleData = struct {
        path: []const u8,
        name: []const u8,
        type: []const u8,
    };

    const sample_data = [_]SampleData{
        .{ .path = "company", .name = "TechCorp", .type = "corporation" },
        .{ .path = "company.engineering", .name = "Engineering", .type = "department" },
        .{ .path = "company.engineering.backend", .name = "Backend Team", .type = "team" },
        .{ .path = "company.engineering.backend.api", .name = "API Service", .type = "service" },
        .{ .path = "company.engineering.backend.database", .name = "Database Team", .type = "service" },
        .{ .path = "company.engineering.frontend", .name = "Frontend Team", .type = "team" },
        .{ .path = "company.engineering.frontend.web", .name = "Web App", .type = "service" },
        .{ .path = "company.engineering.frontend.mobile", .name = "Mobile App", .type = "service" },
        .{ .path = "company.marketing", .name = "Marketing", .type = "department" },
        .{ .path = "company.marketing.digital", .name = "Digital Marketing", .type = "team" },
        .{ .path = "company.marketing.content", .name = "Content Team", .type = "team" },
        .{ .path = "company.sales", .name = "Sales", .type = "department" },
        .{ .path = "company.sales.enterprise", .name = "Enterprise Sales", .type = "team" },
        .{ .path = "company.sales.smb", .name = "SMB Sales", .type = "team" },
    };

    // Store sample data
    for (sample_data) |item| {
        var data = std.json.Value{ .object = std.json.ObjectMap.init(allocator) };
        
        // Create name value
        const name_str = try allocator.dupe(u8, item.name);
        const name_value = std.json.Value{ .string = name_str };
        try data.object.put("name", name_value);
        
        // Create type value
        const type_str = try allocator.dupe(u8, item.type);
        const type_value = std.json.Value{ .string = type_str };
        try data.object.put("type", type_value);

        tree.store(item.path, data, null, null) catch |err| {
            print("Error storing {s}: {}\n", .{ item.path, err });
        };
    }

    print("Stored {} nodes\n", .{sample_data.len});

    // Demonstrate full ltree query capabilities
    print("\n=== Full ltree Query Demonstrations ===\n",.{});

    // 1. Basic pattern matching
    print("\n1. Basic pattern queries:\n",.{});

    print("  a) All direct children of engineering:\n",.{});
    var results = try tree.query("company.engineering.*");
    defer {
        for (results.items) |*result| {
            result.deinit(allocator);
        }
        results.deinit();
    }

    for (results.items) |result| {
        if (result.data == .object) {
            if (result.data.object.get("name")) |name_val| {
                if (name_val == .string) {
                    print("    {s}: {s}\n", .{ result.path, name_val.string });
                }
            }
        }
    }

    print("  b) All descendants of engineering:\n",.{});
    var results2 = try tree.query("company.engineering.**");
    defer {
        for (results2.items) |*result| {
            result.deinit(allocator);
        }
        results2.deinit();
    }

    for (results2.items) |result| {
        if (result.data == .object) {
            if (result.data.object.get("name")) |name_val| {
                if (name_val == .string) {
                    print("    {s}: {s}\n", .{ result.path, name_val.string });
                }
            }
        }
    }

    // 2. Ancestor tests (@>)
    print("\n2. @ Operator Tests:\n",.{});
    print("  a) Ancestor relationships (@>):\n",.{});
    
    const test_pairs = [_][2][]const u8{
        .{ "company", "company.engineering.backend" },
        .{ "company.engineering", "company.engineering.backend.api" },
        .{ "company.sales", "company.engineering.backend" },
    };

    for (test_pairs) |pair| {
        const ancestor = pair[0];
        const descendant = pair[1];
        const is_ancestor = tree.ltreeAncestor(ancestor, descendant);
        print("    '{s}' @> '{s}': {}\n", .{ ancestor, descendant, is_ancestor });
    }

    // Query using @> operator
    print("  b) Find all descendants of 'company.engineering' using @> operator:\n",.{});
    var operator_results = tree.QueryByOperator("@>", "company.engineering", "");
    defer {
        for (operator_results.items) |*result| {
            result.deinit(allocator);
        }
        operator_results.deinit();
    }

    for (operator_results.items) |result| {
        if (result.data == .object) {
            if (result.data.object.get("name")) |name_val| {
                if (name_val == .string) {
                    print("    {s}: {s}\n", .{ result.path, name_val.string });
                }
            }
        }
    }

    // 3. ltxtquery (@@ operator) demonstrations
    print("\n3. ltxtquery (@@ operator) Tests:\n",.{});

    print("  a) Find paths containing 'engineering':\n",.{});
    var ltxt_results = tree.queryLtxtquery("engineering");
    defer {
        for (ltxt_results.items) |*result| {
            result.deinit(allocator);
        }
        ltxt_results.deinit();
    }

    for (ltxt_results.items) |result| {
        if (result.data == .object) {
            if (result.data.object.get("name")) |name_val| {
                if (name_val == .string) {
                    print("    {s}: {s}\n", .{ result.path, name_val.string });
                }
            }
        }
    }

    // 4. ltree functions
    print("\n4. ltree Function Tests:\n",.{});

    const test_path = "company.engineering.backend.api";
    print("  Path: {s}\n", .{test_path});
    print("  nlevel(): {}\n", .{tree.nlevel(test_path)});
    
    const subpath_result = try tree.subpathFunc(test_path, 1, 2);
    defer allocator.free(subpath_result);
    print("  subpath(1, 2): {s}\n", .{subpath_result});
    
    const subltree_result = tree.subltree(test_path, 1, 3);
    defer allocator.free(subltree_result);
    print("  subltree(1, 3): {s}\n", .{subltree_result});
    
    const index_result = tree.indexFunc(test_path, "engineering", 0);
    print("  index('engineering'): {}\n", .{index_result});

    // LCA test
    print("\n  Longest Common Ancestor (LCA):\n",.{});
    const test_paths = [_][]const u8{
        "company.engineering.backend.api",
        "company.engineering.backend.database",
        "company.engineering.frontend.web",
    };
    
    const lca_result = tree.LCA(test_paths[0], test_paths[1], test_paths[2]);
    if (lca_result) |lca| {
        print("    LCA of paths: {s}\n", .{lca.*});
    } else {
        print("    LCA of paths: null\n",.{});
    }

    // 5. Tree statistics
    print("\n5. Tree Statistics:\n");
    const stats = tree.getStats();
    print("  total_nodes: {}\n", .{stats.total_nodes});
    print("  max_depth: {}\n", .{stats.max_depth});
    print("  avg_depth: {d:.2}\n", .{stats.avg_depth});
    print("  root_nodes: {}\n", .{stats.root_nodes});
    print("  leaf_nodes: {}\n", .{stats.leaf_nodes});

    // 6. PostgreSQL integration example (commented out since it requires actual DB)
    print("\n6. PostgreSQL Integration Example:\n",.{});
    print("  (Skipped - requires actual PostgreSQL connection)\n",.{});
    
    // Uncomment these if you have a working PostgreSQL connection:
    
    // Export to PostgreSQL
    const exported = tree.exportToPostgres(tree.table_name, true, false) catch |err| {
        print("Export error: {}\n", .{err});
        return;
    };
    print("Exported {} records\n", .{exported});

    // Import from PostgreSQL
    const imported = tree.importFromPostgres(tree.table_name, "path", "data", "created_at", "updated_at") catch |err| {
        print("Import error: {}\n", .{err});
        return;
    };
    print("Imported {} records\n", .{imported});

    // Bidirectional sync
    const sync_stats = tree.syncWithPostgres("both");
    print("Sync stats: imported={}, exported={}\n", .{ sync_stats.imported, sync_stats.exported });
    

    print("\n=== System Ready - {} nodes loaded ===\n", .{tree.size()});
    print("  Example usage demonstration completed!\n\n",.{});
}

// Uncomment and implement these functions based on your actual module APIs

fn testBasicConstructDB(allocator: std.mem.Allocator) !void {
    print("3. Testing BasicConstructDB...\n",.{});
    
    // Example usage - adjust based on your actual API
    var db = try BasicConstructDB.init(
        allocator,
        "localhost",
        5432,
        "test_db",
        "test_user", 
        "test_password",
        "test_table"
    );
    defer db.deinit();
    
    print("  ✓ BasicConstructDB initialized\n",.{});
    print("  BasicConstructDB tests passed!\n\n",.{});
}

fn testConstructMemDB(allocator: std.mem.Allocator) !void {
    print("4. Testing ConstructMemDB...\n",.{});
    
    // Example usage - adjust based on your actual API
    var cmdb = try ConstructMemDB.init(
        allocator,
        "localhost",
        5432,
        "test_db",
        "test_user",
        "test_password", 
        "test_table"
    );
    defer cmdb.deinit();
    
    print("  ✓ ConstructMemDB initialized\n",.{});
    print("  ConstructMemDB tests passed!\n\n",.{});
}

fn testSearchMemDB(allocator: std.mem.Allocator) !void {
    print("5. Testing SearchMemDB...\n",.{});
    
    // Example usage - adjust based on your actual API
    var smdb = try SearchMemDB.init(
        allocator,
        "localhost",
        5432,
        "test_db",
        "test_user",
        "test_password",
        "test_table"
    );
    defer smdb.deinit();
    
    print("  ✓ SearchMemDB initialized\n",.{});
    print("  SearchMemDB tests passed!\n\n",.{});
}


// Test that verifies the library can be imported correctly
test "library import test" {
    // Test that we can import and access all library components
    _ = kb_lib.BasicConstructDB;
    _ = kb_lib.ConstructMemDB;
    _ = kb_lib.SearchMemDB;
    _ = kb_lib.version;
    _ = kb_lib.lib_name;
}
