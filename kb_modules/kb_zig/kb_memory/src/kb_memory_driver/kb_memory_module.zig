const std = @import("std");
const Allocator = std.mem.Allocator;
const ArrayList = std.ArrayList;
const HashMap = std.HashMap;
const StringHashMap = std.StringHashMap;
const json = std.json;
const testing = std.testing;

// TreeNode represents a node in the tree with metadata
pub const TreeNode = struct {
    path: []const u8,
    data: json.Value,
    created_at: ?[]const u8,
    updated_at: ?[]const u8,
    
    pub fn init(allocator: Allocator, path: []const u8, data: json.Value, created_at: ?[]const u8, updated_at: ?[]const u8) !TreeNode {
        return TreeNode{
            .path = try allocator.dupe(u8, path),
            .data = data,
            .created_at = if (created_at) |ca| try allocator.dupe(u8, ca) else null,
            .updated_at = if (updated_at) |ua| try allocator.dupe(u8, ua) else null,
        };
    }
    
    pub fn deinit(self: *TreeNode, allocator: Allocator) void {
        allocator.free(self.path);
        if (self.created_at) |ca| allocator.free(ca);
        if (self.updated_at) |ua| allocator.free(ua);
    }
};

// QueryResult represents a query result
pub const QueryResult = struct {
    path: []const u8,
    data: json.Value,
    created_at: ?[]const u8,
    updated_at: ?[]const u8,
    
    pub fn init(allocator: Allocator, path: []const u8, data: json.Value, created_at: ?[]const u8, updated_at: ?[]const u8) !QueryResult {
        return QueryResult{
            .path = try allocator.dupe(u8, path),
            .data = data,
            .created_at = if (created_at) |ca| try allocator.dupe(u8, ca) else null,
            .updated_at = if (updated_at) |ua| try allocator.dupe(u8, ua) else null,
        };
    }
    
    pub fn deinit(self: *QueryResult, allocator: Allocator) void {
        allocator.free(self.path);
        if (self.created_at) |ca| allocator.free(ca);
        if (self.updated_at) |ua| allocator.free(ua);
    }
};

// TreeStats represents tree statistics
pub const TreeStats = struct {
    total_nodes: u32,
    max_depth: u32,
    avg_depth: f64,
    root_nodes: u32,
    leaf_nodes: u32,
};

// SyncStats represents synchronization statistics
pub const SyncStats = struct {
    imported: u32,
    exported: u32,
};

// BasicConstructDB is a comprehensive system for storing and querying tree-structured data with full ltree compatibility
pub const BasicConstructDB = struct {
    allocator: Allocator,
    data: StringHashMap(*TreeNode),
    kb_dict: StringHashMap(StringHashMap(json.Value)),
    host: []const u8,
    port: u16,
    dbname: []const u8,
    user: []const u8,
    password: []const u8,
    table_name: []const u8,
    
    pub fn init(allocator: Allocator, host: []const u8, port: u16, dbname: []const u8, user: []const u8, password: []const u8, table_name: []const u8) !BasicConstructDB {
        return BasicConstructDB{
            .allocator = allocator,
            .data = StringHashMap(*TreeNode).init(allocator),
            .kb_dict = StringHashMap(StringHashMap(json.Value)).init(allocator),
            .host = try allocator.dupe(u8, host),
            .port = port,
            .dbname = try allocator.dupe(u8, dbname),
            .user = try allocator.dupe(u8, user),
            .password = try allocator.dupe(u8, password),
            .table_name = try allocator.dupe(u8, table_name),
        };
    }
    
    pub fn deinit(self: *BasicConstructDB) void {
        // Free all nodes
        var iterator = self.data.iterator();
        while (iterator.next()) |entry| {
            entry.value_ptr.*.deinit(self.allocator);
            self.allocator.destroy(entry.value_ptr.*);
        }
        self.data.deinit();
        
        // Free kb_dict
        var kb_iterator = self.kb_dict.iterator();
        while (kb_iterator.next()) |entry| {
            entry.value_ptr.deinit();
        }
        self.kb_dict.deinit();
        
        self.allocator.free(self.host);
        self.allocator.free(self.dbname);
        self.allocator.free(self.user);
        self.allocator.free(self.password);
        self.allocator.free(self.table_name);
    }
    
    // AddKB adds a knowledge base
    pub fn addKB(self: *BasicConstructDB, kb_name: []const u8, description: []const u8) KBError!void {
        if (self.kb_dict.contains(kb_name)) {
            return KBError.KBAlreadyExists;
        }
        
        var kb_data = StringHashMap(json.Value).init(self.allocator);
        const desc_value = try jsonString(self.allocator, description);
        try kb_data.put("description", desc_value);
        try self.kb_dict.put(try self.allocator.dupe(u8, kb_name), kb_data);
    }
    
    // ValidatePath validates that a path conforms to ltree format
    pub fn validatePath(self: *BasicConstructDB, path: []const u8) bool {
        _ = self;
        if (path.len == 0) return false;
        
        var labels = std.mem.split(u8, path, ".");
        while (labels.next()) |label| {
            if (label.len == 0 or label.len > 256) return false;
            
            // Check first character
            if (!std.ascii.isAlphabetic(label[0]) and label[0] != '_') return false;
            
            // Check remaining characters
            for (label[1..]) |char| {
                if (!std.ascii.isAlphanumeric(char) and char != '_') return false;
            }
        }
        return true;
    }
    
    // PathDepth gets the depth (number of levels) of a path
    pub fn pathDepth(self: *BasicConstructDB, path: []const u8) u32 {
        _ = self;
        var count: u32 = 1;
        for (path) |char| {
            if (char == '.') count += 1;
        }
        return count;
    }
    
    // PathLabels gets the labels of a path as a slice
    pub fn pathLabels(self: *BasicConstructDB, path: []const u8) !ArrayList([]const u8) {
        var labels = ArrayList([]const u8).init(self.allocator);
        var split = std.mem.split(u8, path, ".");
        while (split.next()) |label| {
            try labels.append(try self.allocator.dupe(u8, label));
        }
        return labels;
    }
    
    // Subpath extracts a subpath from a path
    pub fn subpath(self: *BasicConstructDB, path: []const u8, start: i32, length: ?u32) ![]const u8 {
        var labels = try self.pathLabels(path);
        defer {
            for (labels.items) |label| {
                self.allocator.free(label);
            }
            labels.deinit();
        }
        
        var actual_start: usize = 0;
        if (start < 0) {
            const abs_start = @as(usize, @intCast(-start));
            if (abs_start > labels.items.len) {
                actual_start = 0;
            } else {
                actual_start = labels.items.len - abs_start;
            }
        } else {
            actual_start = @intCast(start);
        }
        
        if (actual_start >= labels.items.len) {
            return try self.allocator.dupe(u8, "");
        }
        
        const end = if (length) |len| 
            @min(actual_start + len, labels.items.len) 
        else 
            labels.items.len;
        
        var result = ArrayList(u8).init(self.allocator);
        defer result.deinit();
        
        for (labels.items[actual_start..end], 0..) |label, i| {
            if (i > 0) try result.append('.');
            try result.appendSlice(label);
        }
        
        return try result.toOwnedSlice();
    }
    
    // LtreeMatch checks if path matches ltree querya using ~ operator
    pub fn ltreeMatch(self: *BasicConstructDB, path: []const u8, querya: []const u8) bool {
        //_ = self;
        // Simplified implementation - would need full regex support for complete ltree matching
        if (std.mem.indexOf(u8, querya, "*") != null) {
            // Handle wildcard patterns
            return self.matchWildcard(path, querya);
        } else {
            // Exact match
            return std.mem.eql(u8, path, querya);
        }
    }
    
    fn matchWildcard(self: *BasicConstructDB, path: []const u8, pattern: []const u8) bool {
        _ = self;
        // Simplified wildcard matching
        if (std.mem.eql(u8, pattern, "*")) return true;
        
        var path_parts = std.mem.split(u8, path, ".");
        var pattern_parts = std.mem.split(u8, pattern, ".");
        
        while (true) {
            const path_part = path_parts.next();
            const pattern_part = pattern_parts.next();
            
            if (pattern_part == null and path_part == null) return true;
            if (pattern_part == null or path_part == null) return false;
            
            if (!std.mem.eql(u8, pattern_part.?, "*") and !std.mem.eql(u8, pattern_part.?, path_part.?)) {
                return false;
            }
        }
    }
    
    // LtreeAncestor checks if ancestor @> descendant (ancestor-of relationship)
    pub fn ltreeAncestor(self: *BasicConstructDB, ancestor: []const u8, descendant: []const u8) bool {
        _ = self;
        if (std.mem.eql(u8, ancestor, descendant)) return false;
        
        if (descendant.len <= ancestor.len) return false;
        
        return std.mem.startsWith(u8, descendant, ancestor) and 
               descendant[ancestor.len] == '.';
    }
    
    // LtreeDescendant checks if descendant <@ ancestor (descendant-of relationship)
    pub fn ltreeDescendant(self: *BasicConstructDB, descendant: []const u8, ancestor: []const u8) bool {
        return self.ltreeAncestor(ancestor, descendant);
    }
    
    // LtreeAncestorOrEqual checks if ancestor @> descendant or ancestor = descendant
    pub fn ltreeAncestorOrEqual(self: *BasicConstructDB, ancestor: []const u8, descendant: []const u8) bool {
        return std.mem.eql(u8, ancestor, descendant) or self.ltreeAncestor(ancestor, descendant);
    }
    
    // LtreeDescendantOrEqual checks if descendant <@ ancestor or descendant = ancestor
    pub fn ltreeDescendantOrEqual(self: *BasicConstructDB, descendant: []const u8, ancestor: []const u8) bool {
        return std.mem.eql(u8, descendant, ancestor) or self.ltreeDescendant(descendant, ancestor);
    }
    
    // LtreeConcatenate concatenates two ltree paths using || operator
    pub fn ltreeConcatenate(self: *BasicConstructDB, path1: []const u8, path2: []const u8) ![]const u8 {
        if (path1.len == 0) return try self.allocator.dupe(u8, path2);
        if (path2.len == 0) return try self.allocator.dupe(u8, path1);
        
        const result = try std.fmt.allocPrint(self.allocator, "{s}.{s}", .{ path1, path2 });
        return result;
    }
    
    // Nlevel returns the number of labels in the path (ltree nlevel function)
    pub fn nlevel(self: *BasicConstructDB, path: []const u8) u32 {
        return self.pathDepth(path);
    }
    
    // Store stores data at a specific path in the tree
    pub fn store(self: *BasicConstructDB, path: []const u8, data: json.Value, created_at: ?[]const u8, updated_at: ?[]const u8) KBError!void {
        if (!self.validatePath(path)) {
            return KBError.InvalidPath;
        }
        
        const node = self.allocator.create(TreeNode) catch return KBError.OutOfMemory;
        node.* = TreeNode.init(self.allocator, path, data, created_at, updated_at) catch return KBError.OutOfMemory;
        
        // Remove existing node if present
        if (self.data.get(path)) |existing_node| {
            existing_node.deinit(self.allocator);
            self.allocator.destroy(existing_node);
        }
        
        self.data.put(self.allocator.dupe(u8, path) catch return KBError.OutOfMemory, node) catch return KBError.OutOfMemory;
    }
    
    // Get retrieves data from a specific path
    pub fn get(self: *BasicConstructDB, path: []const u8) KBError!?json.Value {
        if (!self.validatePath(path)) {
            return KBError.InvalidPath;
        }
        
        if (self.data.get(path)) |node| {
            return node.data;
        }
        return null;
    }
    
    // GetNode retrieves the full node (with metadata) from a specific path
    pub fn getNode(self: *BasicConstructDB, path: []const u8) KBError!?TreeNode {
        if (!self.validatePath(path)) {
            return KBError.InvalidPath;
        }
        
        if (self.data.get(path)) |node| {
            return TreeNode.init(self.allocator, node.path, node.data, node.created_at, node.updated_at) catch return KBError.OutOfMemory;
        }
        return null;
    }
    
    // Query queries using ltree pattern matching (~)
    pub fn query(self: *BasicConstructDB, pattern: []const u8) !ArrayList(QueryResult) {
        var results = ArrayList(QueryResult).init(self.allocator);
        
        var iterator = self.data.iterator();
        while (iterator.next()) |entry| {
            if (self.ltreeMatch(entry.key_ptr.*, pattern)) {
                const result = try QueryResult.init(
                    self.allocator,
                    entry.value_ptr.*.path,
                    entry.value_ptr.*.data,
                    entry.value_ptr.*.created_at,
                    entry.value_ptr.*.updated_at
                );
                try results.append(result);
            }
        }
        
        // Sort results by path
        std.sort.insertion(QueryResult, results.items, {}, struct {
            fn lessThan(context: void, a: QueryResult, b: QueryResult) bool {
                _ = context;
                return std.mem.lessThan(u8, a.path, b.path);
            }
        }.lessThan);
        
        return results;
    }
    
    // QueryAncestors gets all ancestors using @> operator
    pub fn queryAncestors(self: *BasicConstructDB, path: []const u8) !ArrayList(QueryResult) {
        if (!self.validatePath(path)) {
            return error.InvalidPath;
        }
        
        var results = ArrayList(QueryResult).init(self.allocator);
        
        var iterator = self.data.iterator();
        while (iterator.next()) |entry| {
            if (self.ltreeAncestor(entry.key_ptr.*, path)) {
                const result = try QueryResult.init(
                    self.allocator,
                    entry.value_ptr.*.path,
                    entry.value_ptr.*.data,
                    entry.value_ptr.*.created_at,
                    entry.value_ptr.*.updated_at
                );
                try results.append(result);
            }
        }
        
        // Sort by depth (ancestors first)
        std.sort.insertion(QueryResult, results.items, self, struct {
            fn lessThan(db: *BasicConstructDB, a: QueryResult, b: QueryResult) bool {
                return db.pathDepth(a.path) < db.pathDepth(b.path);
            }
        }.lessThan);
        
        return results;
    }
    
    // QueryDescendants gets all descendants using <@ operator
    pub fn queryDescendants(self: *BasicConstructDB, path: []const u8) !ArrayList(QueryResult) {
        if (!self.validatePath(path)) {
            return error.InvalidPath;
        }
        
        var results = ArrayList(QueryResult).init(self.allocator);
        
        var iterator = self.data.iterator();
        while (iterator.next()) |entry| {
            if (self.ltreeDescendant(entry.key_ptr.*, path)) {
                const result = try QueryResult.init(
                    self.allocator,
                    entry.value_ptr.*.path,
                    entry.value_ptr.*.data,
                    entry.value_ptr.*.created_at,
                    entry.value_ptr.*.updated_at
                );
                try results.append(result);
            }
        }
        
        // Sort results by path
        std.sort.insertion(QueryResult, results.items, {}, struct {
            fn lessThan(context: void, a: QueryResult, b: QueryResult) bool {
                _ = context;
                return std.mem.lessThan(u8, a.path, b.path);
            }
        }.lessThan);
        
        return results;
    }
    
    // QuerySubtree gets node and all its descendants
    pub fn querySubtree(self: *BasicConstructDB, path: []const u8) !ArrayList(QueryResult) {
        var results = ArrayList(QueryResult).init(self.allocator);
        
        // Add the node itself if it exists
        if (self.exists(path)) {
            if (self.data.get(path)) |node| {
                const result = try QueryResult.init(
                    self.allocator,
                    node.path,
                    node.data,
                    node.created_at,
                    node.updated_at
                );
                try results.append(result);
            }
        }
        
        // Add all descendants
        var descendants = try self.queryDescendants(path);
        defer {
            for (descendants.items) |*desc| {
                desc.deinit(self.allocator);
            }
            descendants.deinit();
        }
        
        for (descendants.items) |desc| {
            const result = try QueryResult.init(
                self.allocator,
                desc.path,
                desc.data,
                desc.created_at,
                desc.updated_at
            );
            try results.append(result);
        }
        
        // Sort results by path
        std.sort.insertion(QueryResult, results.items, {}, struct {
            fn lessThan(context: void, a: QueryResult, b: QueryResult) bool {
                _ = context;
                return std.mem.lessThan(u8, a.path, b.path);
            }
        }.lessThan);
        
        return results;
    }
    
    // Exists checks if a path exists
    pub fn exists(self: *BasicConstructDB, path: []const u8) bool {
        return self.data.contains(path) and self.validatePath(path);
    }
    
    // Delete deletes a specific node
    pub fn delete(self: *BasicConstructDB, path: []const u8) bool {
        if (self.data.fetchRemove(path)) |kv| {
            kv.value.deinit(self.allocator);
            self.allocator.destroy(kv.value);
            self.allocator.free(kv.key);
            return true;
        }
        return false;
    }
    
    // DeleteSubtree deletes a node and all its descendants
    pub fn deleteSubtree(self: *BasicConstructDB, path: []const u8) u32 {
        var to_delete = ArrayList([]const u8).init(self.allocator);
        defer to_delete.deinit();
        
        // Find all paths to delete
        var iterator = self.data.iterator();
        while (iterator.next()) |entry| {
            if (std.mem.eql(u8, entry.key_ptr.*, path) or self.ltreeDescendant(entry.key_ptr.*, path)) {
                to_delete.append(entry.key_ptr.*) catch continue;
            }
        }
        
        // Delete them
        var deleted_count: u32 = 0;
        for (to_delete.items) |delete_path| {
            if (self.delete(delete_path)) {
                deleted_count += 1;
            }
        }
        
        return deleted_count;
    }
    
    // GetStats gets comprehensive tree statistics
    pub fn getStats(self: *BasicConstructDB) TreeStats {
        if (self.data.count() == 0) {
            return TreeStats{
                .total_nodes = 0,
                .max_depth = 0,
                .avg_depth = 0.0,
                .root_nodes = 0,
                .leaf_nodes = 0,
            };
        }
        
        var depths = ArrayList(u32).init(self.allocator);
        defer depths.deinit();
        
        var root_nodes: u32 = 0;
        var max_depth: u32 = 0;
        var total_depth: u32 = 0;
        
        var iterator = self.data.iterator();
        while (iterator.next()) |entry| {
            const depth = self.pathDepth(entry.key_ptr.*);
            depths.append(depth) catch continue;
            
            if (depth == 1) root_nodes += 1;
            if (depth > max_depth) max_depth = depth;
            total_depth += depth;
        }
        
        // Count leaf nodes
        var leaf_nodes: u32 = 0;
        var outer_iterator = self.data.iterator();
        while (outer_iterator.next()) |entry| {
            var has_children = false;
            var inner_iterator = self.data.iterator();
            while (inner_iterator.next()) |other_entry| {
                if (self.ltreeAncestor(entry.key_ptr.*, other_entry.key_ptr.*)) {
                    has_children = true;
                    break;
                }
            }
            if (!has_children) leaf_nodes += 1;
        }
        
        const avg_depth = if (depths.items.len > 0) 
            @as(f64, @floatFromInt(total_depth)) / @as(f64, @floatFromInt(depths.items.len))
        else 
            0.0;
        
        return TreeStats{
            .total_nodes = @intCast(self.data.count()),
            .max_depth = max_depth,
            .avg_depth = avg_depth,
            .root_nodes = root_nodes,
            .leaf_nodes = leaf_nodes,
        };
    }
    
    // Clear clears all data
    pub fn clear(self: *BasicConstructDB) void {
        var iterator = self.data.iterator();
        while (iterator.next()) |entry| {
            entry.value_ptr.*.deinit(self.allocator);
            self.allocator.destroy(entry.value_ptr.*);
            self.allocator.free(entry.key_ptr.*);
        }
        self.data.clearAndFree();
    }
    
    // Size gets the number of nodes
    pub fn size(self: *BasicConstructDB) u32 {
        return @intCast(self.data.count());
    }
    
    // GetAllPaths gets all paths sorted
    pub fn getAllPaths(self: *BasicConstructDB) !ArrayList([]const u8) {
        var paths = ArrayList([]const u8).init(self.allocator);
        
        var iterator = self.data.iterator();
        while (iterator.next()) |entry| {
            try paths.append(try self.allocator.dupe(u8, entry.key_ptr.*));
        }
        
        std.sort.insertion([]const u8, paths.items, {}, struct {
            fn lessThan(context: void, a: []const u8, b: []const u8) bool {
                _ = context;
                return std.mem.lessThan(u8, a, b);
            }
        }.lessThan);
        
        return paths;
    }
};

// Error types for better error handling
pub const KBError = error{
    InvalidPath,
    KBAlreadyExists,
    NodeNotFound,
    OutOfMemory,
    DatabaseError,
};

// Helper function to create a JSON string value
pub fn jsonString(allocator: Allocator, str: []const u8) !json.Value {
    return json.Value{ .string = try allocator.dupe(u8, str) };
}

// Helper function to create a JSON object value
pub fn jsonObject(allocator: Allocator) json.Value {
    return json.Value{ .object = json.ObjectMap.init(allocator) };
}

// Helper function to create a JSON number value
pub fn jsonNumber(value: anytype) json.Value {
    return switch (@TypeOf(value)) {
        i32, i64, u32, u64 => json.Value{ .integer = @intCast(value) },
        f32, f64 => json.Value{ .float = @floatCast(value) },
        else => @compileError("Unsupported number type"),
    };
}

// Helper function to create a JSON boolean value
pub fn jsonBool(value: bool) json.Value {
    return json.Value{ .bool = value };
}

// Helper function to create a JSON null value
pub fn jsonNull() json.Value {
    return json.Value.null;
}

test "basic functionality" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();
    
    var db = try BasicConstructDB.init(allocator, "localhost", 5432, "test", "user", "pass", "test_table");
    defer db.deinit();
    
    // Test path validation
    try testing.expect(db.validatePath("a.b.c"));
    try testing.expect(!db.validatePath(""));
    try testing.expect(!db.validatePath("1invalid"));
    
    // Test storing and retrieving
    const test_data = json.Value{ .string = "test_value" };
    try db.store("a.b.c", test_data, null, null);
    
    try testing.expect(db.exists("a.b.c"));
    try testing.expect(!db.exists("a.b.d"));
    
    const retrieved = try db.get("a.b.c");
    try testing.expect(retrieved != null);
    
    // Test tree relationships
    try testing.expect(db.ltreeAncestor("a.b", "a.b.c"));
    try testing.expect(!db.ltreeAncestor("a.b.c", "a.b"));
    try testing.expect(db.ltreeDescendant("a.b.c", "a.b"));
}

