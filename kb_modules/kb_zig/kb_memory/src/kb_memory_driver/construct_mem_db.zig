const std = @import("std");
const Allocator = std.mem.Allocator;
const ArrayList = std.ArrayList;
const StringHashMap = std.StringHashMap;
const json = std.json;
const print = std.debug.print;

// Import the BasicConstructDB from the previous module
// In practice, this would be: const BasicConstructDB = @import("kb_memory_module.zig").BasicConstructDB;
// For this example, I'll assume it's available

const BasicConstructDB = @import("kb_memory_module.zig").BasicConstructDB;

// Error types for ConstructMemDB
pub const ConstructMemDBError = error{
    KBAlreadyExists,
    KBNotFound,
    NoWorkingKB,
    InvalidNodeData,
    PathAlreadyExists,
    PathEmpty,
    NotEnoughElements,
    AssertionError,
    InstallationCheckFailed,
    OutOfMemory,
};

// ConstructMemDB extends BasicConstructDB with knowledge base management and composite path tracking
pub const ConstructMemDB = struct {
    allocator: Allocator,
    basic_db: *BasicConstructDB,  // Composition instead of inheritance
    kb_name: ?[]const u8,         // Currently selected knowledge base name
    working_kb: ?[]const u8,      // Working knowledge base
    composite_path: StringHashMap(ArrayList([]const u8)),      // Tracks composite paths for each KB
    composite_path_values: StringHashMap(StringHashMap(bool)), // Tracks existing paths in each KB
    
    pub fn init(allocator: Allocator, host: []const u8, port: u16, dbname: []const u8, user: []const u8, password: []const u8, database: []const u8) !ConstructMemDB {
        const basic_db = try allocator.create(BasicConstructDB);
        basic_db.* = try BasicConstructDB.init(allocator, host, port, dbname, user, password, database);
        
        return ConstructMemDB{
            .allocator = allocator,
            .basic_db = basic_db,
            .kb_name = null,
            .working_kb = null,
            .composite_path = StringHashMap(ArrayList([]const u8)).init(allocator),
            .composite_path_values = StringHashMap(StringHashMap(bool)).init(allocator),
        };
    }
    
    pub fn deinit(self: *ConstructMemDB) void {
        // Free composite paths
        var path_iterator = self.composite_path.iterator();
        while (path_iterator.next()) |entry| {
            for (entry.value_ptr.items) |path_component| {
                self.allocator.free(path_component);
            }
            entry.value_ptr.deinit();
            self.allocator.free(entry.key_ptr.*);
        }
        self.composite_path.deinit();
        
        // Free composite path values
        var values_iterator = self.composite_path_values.iterator();
        while (values_iterator.next()) |entry| {
            var inner_iterator = entry.value_ptr.iterator();
            while (inner_iterator.next()) |inner_entry| {
                self.allocator.free(inner_entry.key_ptr.*);
            }
            entry.value_ptr.deinit();
            self.allocator.free(entry.key_ptr.*);
        }
        self.composite_path_values.deinit();
        
        // Free string fields
        if (self.kb_name) |name| self.allocator.free(name);
        if (self.working_kb) |kb| self.allocator.free(kb);
        
        // Free basic_db
        self.basic_db.deinit();
        self.allocator.destroy(self.basic_db);
    }
    
    // AddKB adds a knowledge base with composite path tracking
    pub fn addKB(self: *ConstructMemDB, kb_name: []const u8, description: []const u8) ConstructMemDBError!void {
        // Check if KB already exists in composite path
        if (self.composite_path.contains(kb_name)) {
            return ConstructMemDBError.KBAlreadyExists;
        }
        
        // Initialize composite path structures
        const kb_name_owned = self.allocator.dupe(u8, kb_name) catch return ConstructMemDBError.OutOfMemory;
        
        var path_list = ArrayList([]const u8).init(self.allocator);
        const kb_name_copy = self.allocator.dupe(u8, kb_name) catch return ConstructMemDBError.OutOfMemory;
        path_list.append(kb_name_copy) catch return ConstructMemDBError.OutOfMemory;
        
        const  path_values = StringHashMap(bool).init(self.allocator);
        
        self.composite_path.put(kb_name_owned, path_list) catch return ConstructMemDBError.OutOfMemory;
        self.composite_path_values.put(try self.allocator.dupe(u8, kb_name), path_values) catch return ConstructMemDBError.OutOfMemory;
        
        // Call parent method
        self.basic_db.addKB(kb_name, description) catch return ConstructMemDBError.OutOfMemory;
    }
    
    // SelectKB selects a knowledge base to work with
    pub fn selectKB(self: *ConstructMemDB, kb_name: []const u8) ConstructMemDBError!void {
        if (!self.composite_path.contains(kb_name)) {
            return ConstructMemDBError.KBNotFound;
        }
        
        // Free previous working_kb if it exists
        if (self.working_kb) |old_kb| {
            self.allocator.free(old_kb);
        }
        
        self.working_kb = self.allocator.dupe(u8, kb_name) catch return ConstructMemDBError.OutOfMemory;
    }
    
    // AddHeaderNode adds a header node to the knowledge base
    pub fn addHeaderNode(self: *ConstructMemDB, link: []const u8, node_name: []const u8, node_data: json.Value, description: ?[]const u8) ConstructMemDBError!void {
        if (self.working_kb == null) {
            return ConstructMemDBError.NoWorkingKB;
        }
        
        const working_kb = self.working_kb.?;
        
        // Validate input - ensure node_data is an object
        if (node_data != .object) {
            return ConstructMemDBError.InvalidNodeData;
        }
        
        // Create a mutable copy of node_data
        var mutable_data = json.Value{ .object = json.ObjectMap.init(self.allocator) };
        
        // Copy existing data
        var original_iterator = node_data.object.iterator();
        while (original_iterator.next()) |entry| {
            const key_copy = self.allocator.dupe(u8, entry.key_ptr.*) catch return ConstructMemDBError.OutOfMemory;
            mutable_data.object.put(key_copy, entry.value_ptr.*) catch return ConstructMemDBError.OutOfMemory;
        }
        
        // Add description if provided
        if (description) |desc| {
            const desc_key = self.allocator.dupe(u8, "description") catch return ConstructMemDBError.OutOfMemory;
            const desc_value = json.Value{ .string = self.allocator.dupe(u8, desc) catch return ConstructMemDBError.OutOfMemory };
            mutable_data.object.put(desc_key, desc_value) catch return ConstructMemDBError.OutOfMemory;
        }
        
        // Get mutable reference to composite path
        var path_list = self.composite_path.getPtr(working_kb).?;
        
        // Build composite path
        const link_copy = self.allocator.dupe(u8, link) catch return ConstructMemDBError.OutOfMemory;
        const node_name_copy = self.allocator.dupe(u8, node_name) catch return ConstructMemDBError.OutOfMemory;
        
        path_list.append(link_copy) catch return ConstructMemDBError.OutOfMemory;
        path_list.append(node_name_copy) catch return ConstructMemDBError.OutOfMemory;
        
        // Build path string
        const node_path = try self.joinPath(path_list.items);
        defer self.allocator.free(node_path);
        
        // Check if path already exists
        const path_values = self.composite_path_values.getPtr(working_kb).?;
        if (path_values.contains(node_path)) {
            return ConstructMemDBError.PathAlreadyExists;
        }
        
        // Mark path as used
        const path_key = self.allocator.dupe(u8, node_path) catch return ConstructMemDBError.OutOfMemory;
        path_values.put(path_key, true) catch return ConstructMemDBError.OutOfMemory;
        
        // Store in the underlying BasicConstructDB
        const final_path = try self.joinPath(path_list.items);
        defer self.allocator.free(final_path);
        
        print("path: {s}\n", .{final_path});
        self.basic_db.store(final_path, mutable_data, null, null) catch return ConstructMemDBError.OutOfMemory;
    }
    
    // AddInfoNode adds an info node (temporary header node that gets removed from path)
    pub fn addInfoNode(self: *ConstructMemDB, link: []const u8, node_name: []const u8, node_data: json.Value, description: ?[]const u8) ConstructMemDBError!void {
        if (self.working_kb == null) {
            return ConstructMemDBError.NoWorkingKB;
        }
        
        // Add as header node first
        try self.addHeaderNode(link, node_name, node_data, description);
        
        const working_kb = self.working_kb.?;
        var path_list = self.composite_path.getPtr(working_kb).?;
        
        // Remove node_name and link from path (reverse order)
        if (path_list.items.len >= 2) {
            // Remove nodeName
            if (path_list.items.len > 0) {
                const removed_name = path_list.pop();
                self.allocator.free(removed_name);
            }
            // Remove link
            if (path_list.items.len > 0) {
                const removed_link = path_list.pop();
                self.allocator.free(removed_link);
            }
        }
    }
    
    // LeaveHeaderNode leaves a header node, verifying the label and name
    pub fn leaveHeaderNode(self: *ConstructMemDB, label: []const u8, name: []const u8) ConstructMemDBError!void {
        if (self.working_kb == null) {
            return ConstructMemDBError.NoWorkingKB;
        }
        
        const working_kb = self.working_kb.?;
        var path_list = self.composite_path.getPtr(working_kb).?;
        
        // Check if path is empty
        if (path_list.items.len == 0) {
            return ConstructMemDBError.PathEmpty;
        }
        
        // Pop the name
        if (path_list.items.len < 1) {
            return ConstructMemDBError.PathEmpty;
        }
        
        const ref_name = path_list.pop();
        defer self.allocator.free(ref_name);
        
        // Check if we have enough elements for label
        if (path_list.items.len == 0) {
            // Put the name back and raise an error
            const name_copy = self.allocator.dupe(u8, ref_name) catch return ConstructMemDBError.OutOfMemory;
            path_list.append(name_copy) catch return ConstructMemDBError.OutOfMemory;
            return ConstructMemDBError.NotEnoughElements;
        }
        
        // Pop the label
        const ref_label = path_list.pop();
        defer self.allocator.free(ref_label);
        
        // Verify the popped values
        var has_error = false;
        if (!std.mem.eql(u8, ref_name, name)) {
            has_error = true;
        }
        if (!std.mem.eql(u8, ref_label, label)) {
            has_error = true;
        }
        
        if (has_error) {
            return ConstructMemDBError.AssertionError;
        }
    }
    
    // CheckInstallation checks if the installation is correct by verifying that all paths are properly reset
    pub fn checkInstallation(self: *ConstructMemDB) ConstructMemDBError!void {
        var iterator = self.composite_path.iterator();
        while (iterator.next()) |entry| {
            const kb_name = entry.key_ptr.*;
            const path = entry.value_ptr;
            
            if (path.items.len != 1) {
                return ConstructMemDBError.InstallationCheckFailed;
            }
            if (!std.mem.eql(u8, path.items[0], kb_name)) {
                return ConstructMemDBError.InstallationCheckFailed;
            }
        }
    }
    
    // GetCurrentPath returns the current composite path for the working KB
    pub fn getCurrentPath(self: *ConstructMemDB) ?ArrayList([]const u8) {
        if (self.working_kb == null) {
            return null;
        }
        
        const working_kb = self.working_kb.?;
        const path_list = self.composite_path.get(working_kb) orelse return null;
        
        // Return a copy to prevent external modification
        var path_copy = ArrayList([]const u8).init(self.allocator);
        for (path_list.items) |component| {
            const component_copy = self.allocator.dupe(u8, component) catch return null;
            path_copy.append(component_copy) catch return null;
        }
        return path_copy;
    }
    
    // GetCurrentPathString returns the current composite path as a string
    pub fn getCurrentPathString(self: *ConstructMemDB) ?[]const u8 {
        if (self.working_kb == null) {
            return null;
        }
        
        const working_kb = self.working_kb.?;
        const path_list = self.composite_path.get(working_kb) orelse return null;
        
        return self.joinPath(path_list.items) catch null;
    }
    
    // GetWorkingKB returns the currently selected working knowledge base
    pub fn getWorkingKB(self: *ConstructMemDB) ?[]const u8 {
        return self.working_kb;
    }
    
    // GetAllKBNames returns all knowledge base names
    pub fn getAllKBNames(self: *ConstructMemDB) !ArrayList([]const u8) {
        var names = ArrayList([]const u8).init(self.allocator);
        
        var iterator = self.composite_path.iterator();
        while (iterator.next()) |entry| {
            const name_copy = try self.allocator.dupe(u8, entry.key_ptr.*);
            try names.append(name_copy);
        }
        
        return names;
    }
    
    // Helper function to join path components with "."
    fn joinPath(self: *ConstructMemDB, components: []const []const u8) ![]const u8 {
        if (components.len == 0) {
            return try self.allocator.dupe(u8, "");
        }
        
        var total_len: usize = 0;
        for (components) |component| {
            total_len += component.len;
        }
        total_len += components.len - 1; // For dots
        
        var result = try self.allocator.alloc(u8, total_len);
        var pos: usize = 0;
        
        for (components, 0..) |component, i| {
            if (i > 0) {
                result[pos] = '.';
                pos += 1;
            }
            @memcpy(result[pos..pos + component.len], component);
            pos += component.len;
        }
        
        return result;
    }
    
    // Delegate methods to BasicConstructDB
    pub fn validatePath(self: *ConstructMemDB, path: []const u8) bool {
        return self.basic_db.validatePath(path);
    }
    
    pub fn pathDepth(self: *ConstructMemDB, path: []const u8) u32 {
        return self.basic_db.pathDepth(path);
    }
    
    pub fn store(self: *ConstructMemDB, path: []const u8, data: json.Value, created_at: ?[]const u8, updated_at: ?[]const u8) !void {
        return self.basic_db.store(path, data, created_at, updated_at);
    }
    
    pub fn get(self: *ConstructMemDB, path: []const u8) !?json.Value {
        return self.basic_db.get(path);
    }
    
    pub fn exists(self: *ConstructMemDB, path: []const u8) bool {
        return self.basic_db.exists(path);
    }
    
    pub fn delete(self: *ConstructMemDB, path: []const u8) bool {
        return self.basic_db.delete(path);
    }
    
    pub fn query(self: *ConstructMemDB, pattern: []const u8) !ArrayList(self.basic_db.QueryResult) {
        return self.basic_db.query(pattern);
    }
    
    pub fn queryAncestors(self: *ConstructMemDB, path: []const u8) !ArrayList(self.basic_db.QueryResult) {
        return self.basic_db.queryAncestors(path);
    }
    
    pub fn queryDescendants(self: *ConstructMemDB, path: []const u8) !ArrayList(self.basic_db.QueryResult) {
        return self.basic_db.queryDescendants(path);
    }
    
    pub fn getStats(self: *ConstructMemDB) self.basic_db.TreeStats {
        return self.basic_db.getStats();
    }
    
    pub fn clear(self: *ConstructMemDB) void {
        self.basic_db.clear();
    }
    
    pub fn size(self: *ConstructMemDB) u32 {
        return self.basic_db.size();
    }
};

// Test function
test "ConstructMemDB basic functionality" {
    const testing = std.testing;
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();
    
    var cmdb = try ConstructMemDB.init(allocator, "localhost", 5432, "test", "user", "pass", "test_table");
    defer cmdb.deinit();
    
    // Test adding KB
    try cmdb.addKB("test_kb", "Test knowledge base");
    
    // Test selecting KB
    try cmdb.selectKB("test_kb");
    
    // Test getting working KB
    const working_kb = cmdb.getWorkingKB();
    try testing.expect(working_kb != null);
    try testing.expect(std.mem.eql(u8, working_kb.?, "test_kb"));
    
    // Test getting current path
    const current_path = cmdb.getCurrentPathString();
    try testing.expect(current_path != null);
    try testing.expect(std.mem.eql(u8, current_path.?, "test_kb"));
    
    // Test installation check
    try cmdb.checkInstallation();
}

