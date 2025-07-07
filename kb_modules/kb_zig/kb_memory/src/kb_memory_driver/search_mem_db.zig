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
const TreeNode = @import("kb_memory_module.zig").TreeNode;
const TreeStats = @import("kb_memory_module.zig").TreeStats;

// Error types for SearchMemDB
pub const SearchMemDBError = error{
    ImportFailed,
    QueryError,
    OutOfMemory,
    InvalidData,
};

// SearchMemDB extends BasicConstructDB with search and filtering capabilities
pub const SearchMemDB = struct {
    allocator: Allocator,
    basic_db: *BasicConstructDB,  // Composition instead of inheritance
    keys: StringHashMap(ArrayList([]const u8)),                // Generated decoded keys
    kbs: StringHashMap(ArrayList([]const u8)),                 // Knowledge bases mapping
    labels: StringHashMap(ArrayList([]const u8)),              // Labels mapping
    names: StringHashMap(ArrayList([]const u8)),               // Names mapping
    decoded_keys: StringHashMap(ArrayList([]const u8)),        // Decoded path keys
    filter_results: StringHashMap(*TreeNode),                  // Current filter results
    
    pub fn init(allocator: Allocator, host: []const u8, port: u16, dbname: []const u8, user: []const u8, password: []const u8, table_name: []const u8) SearchMemDBError!SearchMemDB {
        const basic_db = allocator.create(BasicConstructDB) catch return SearchMemDBError.OutOfMemory;
        basic_db.* = BasicConstructDB.init(allocator, host, port, dbname, user, password, table_name) catch return SearchMemDBError.OutOfMemory;
        
        var smdb = SearchMemDB{
            .allocator = allocator,
            .basic_db = basic_db,
            .keys = StringHashMap(ArrayList([]const u8)).init(allocator),
            .kbs = StringHashMap(ArrayList([]const u8)).init(allocator),
            .labels = StringHashMap(ArrayList([]const u8)).init(allocator),
            .names = StringHashMap(ArrayList([]const u8)).init(allocator),
            .decoded_keys = StringHashMap(ArrayList([]const u8)).init(allocator),
            .filter_results = StringHashMap(*TreeNode).init(allocator),
        };
        
        // Import data from PostgreSQL (would need actual implementation)
        // For now, we'll skip this step as it requires database connectivity
        
        // Generate decoded keys
        try smdb.generateDecodedKeys();
        
        // Initialize filter results with all data
        try smdb.clearFilters();
        
        return smdb;
    }
    
    pub fn deinit(self: *SearchMemDB) void {
        // Free keys
        self.freeStringArrayHashMap(&self.keys);
        
        // Free kbs
        self.freeStringArrayHashMap(&self.kbs);
        
        // Free labels
        self.freeStringArrayHashMap(&self.labels);
        
        // Free names
        self.freeStringArrayHashMap(&self.names);
        
        // Free decoded_keys
        self.freeStringArrayHashMap(&self.decoded_keys);
        
        // Free filter_results keys (the TreeNode values are owned by basic_db)
        var filter_iterator = self.filter_results.iterator();
        while (filter_iterator.next()) |entry| {
            self.allocator.free(entry.key_ptr.*);
        }
        self.filter_results.deinit();
        
        // Free basic_db
        self.basic_db.deinit();
        self.allocator.destroy(self.basic_db);
    }
    
    // Helper function to free StringHashMap(ArrayList([]const u8))
    fn freeStringArrayHashMap(self: *SearchMemDB, map: *StringHashMap(ArrayList([]const u8))) void {
        var iterator = map.iterator();
        while (iterator.next()) |entry| {
            for (entry.value_ptr.items) |item| {
                self.allocator.free(item);
            }
            entry.value_ptr.deinit();
            self.allocator.free(entry.key_ptr.*);
        }
        map.deinit();
    }
    
    // generateDecodedKeys processes the data and creates lookup maps
    fn generateDecodedKeys(self: *SearchMemDB) SearchMemDBError!void {
        // Clear existing maps
        self.freeStringArrayHashMap(&self.kbs);
        self.freeStringArrayHashMap(&self.labels);
        self.freeStringArrayHashMap(&self.names);
        self.freeStringArrayHashMap(&self.decoded_keys);
        
        // Reinitialize maps
        self.kbs = StringHashMap(ArrayList([]const u8)).init(self.allocator);
        self.labels = StringHashMap(ArrayList([]const u8)).init(self.allocator);
        self.names = StringHashMap(ArrayList([]const u8)).init(self.allocator);
        self.decoded_keys = StringHashMap(ArrayList([]const u8)).init(self.allocator);
        
        var data_iterator = self.basic_db.data.iterator();
        while (data_iterator.next()) |entry| {
            const key = entry.key_ptr.*;
            
            // Split the key into components
            var components = ArrayList([]const u8).init(self.allocator);
            var split = std.mem.split(u8, key, ".");
            while (split.next()) |component| {
                const component_copy = self.allocator.dupe(u8, component) catch return SearchMemDBError.OutOfMemory;
                components.append(component_copy) catch return SearchMemDBError.OutOfMemory;
            }
            
            const key_copy = self.allocator.dupe(u8, key) catch return SearchMemDBError.OutOfMemory;
            self.decoded_keys.put(key_copy, components) catch return SearchMemDBError.OutOfMemory;
            
            if (components.items.len < 3) {
                // Skip keys that don't have at least kb.label.name structure
                continue;
            }
            
            const kb = components.items[0];
            const label = components.items[components.items.len - 2];
            const name = components.items[components.items.len - 1];
            
            // Add to knowledge bases map
            try self.addToMap(&self.kbs, kb, key);
            
            // Add to labels map
            try self.addToMap(&self.labels, label, key);
            
            // Add to names map
            try self.addToMap(&self.names, name, key);
        }
    }
    
    // Helper function to add key to a map
    fn addToMap(self: *SearchMemDB, map: *StringHashMap(ArrayList([]const u8)), map_key: []const u8, value: []const u8) SearchMemDBError!void {
        if (map.getPtr(map_key)) |existing_list| {
            const value_copy = self.allocator.dupe(u8, value) catch return SearchMemDBError.OutOfMemory;
            existing_list.append(value_copy) catch return SearchMemDBError.OutOfMemory;
        } else {
            var new_list = ArrayList([]const u8).init(self.allocator);
            const value_copy = self.allocator.dupe(u8, value) catch return SearchMemDBError.OutOfMemory;
            new_list.append(value_copy) catch return SearchMemDBError.OutOfMemory;
            const key_copy = self.allocator.dupe(u8, map_key) catch return SearchMemDBError.OutOfMemory;
            map.put(key_copy, new_list) catch return SearchMemDBError.OutOfMemory;
        }
    }
    
    // ClearFilters clears all filters and resets the query state
    pub fn clearFilters(self: *SearchMemDB) SearchMemDBError!void {
        // Free existing filter results keys
        var filter_iterator = self.filter_results.iterator();
        while (filter_iterator.next()) |entry| {
            self.allocator.free(entry.key_ptr.*);
        }
        self.filter_results.clearAndFree();
        
        // Copy all data to filter results
        var data_iterator = self.basic_db.data.iterator();
        while (data_iterator.next()) |entry| {
            const key_copy = self.allocator.dupe(u8, entry.key_ptr.*) catch return SearchMemDBError.OutOfMemory;
            self.filter_results.put(key_copy, entry.value_ptr.*) catch return SearchMemDBError.OutOfMemory;
        }
    }
    
    // SearchKB searches for rows matching the specified knowledge base
    pub fn searchKB(self: *SearchMemDB, knowledge_base: []const u8) SearchMemDBError!StringHashMap(*TreeNode) {
        var new_filter_results = StringHashMap(*TreeNode).init(self.allocator);
        
        if (self.kbs.get(knowledge_base)) |kb_keys| {
            for (kb_keys.items) |key| {
                if (self.filter_results.get(key)) |node| {
                    const key_copy = self.allocator.dupe(u8, key) catch return SearchMemDBError.OutOfMemory;
                    new_filter_results.put(key_copy, node) catch return SearchMemDBError.OutOfMemory;
                }
            }
        }
        
        // Free old filter results and replace
        var old_iterator = self.filter_results.iterator();
        while (old_iterator.next()) |entry| {
            self.allocator.free(entry.key_ptr.*);
        }
        self.filter_results.deinit();
        self.filter_results = new_filter_results;
        
        return self.filter_results;
    }
    
    // SearchLabel searches for rows matching the specified label
    pub fn searchLabel(self: *SearchMemDB, label: []const u8) SearchMemDBError!StringHashMap(*TreeNode) {
        var new_filter_results = StringHashMap(*TreeNode).init(self.allocator);
        
        if (self.labels.get(label)) |label_keys| {
            for (label_keys.items) |key| {
                if (self.filter_results.get(key)) |node| {
                    const key_copy = self.allocator.dupe(u8, key) catch return SearchMemDBError.OutOfMemory;
                    new_filter_results.put(key_copy, node) catch return SearchMemDBError.OutOfMemory;
                }
            }
        }
        
        // Free old filter results and replace
        var old_iterator = self.filter_results.iterator();
        while (old_iterator.next()) |entry| {
            self.allocator.free(entry.key_ptr.*);
        }
        self.filter_results.deinit();
        self.filter_results = new_filter_results;
        
        return self.filter_results;
    }
    
    // SearchName searches for rows matching the specified name
    pub fn searchName(self: *SearchMemDB, name: []const u8) SearchMemDBError!StringHashMap(*TreeNode) {
        var new_filter_results = StringHashMap(*TreeNode).init(self.allocator);
        
        if (self.names.get(name)) |name_keys| {
            for (name_keys.items) |key| {
                if (self.filter_results.get(key)) |node| {
                    const key_copy = self.allocator.dupe(u8, key) catch return SearchMemDBError.OutOfMemory;
                    new_filter_results.put(key_copy, node) catch return SearchMemDBError.OutOfMemory;
                }
            }
        }
        
        // Free old filter results and replace
        var old_iterator = self.filter_results.iterator();
        while (old_iterator.next()) |entry| {
            self.allocator.free(entry.key_ptr.*);
        }
        self.filter_results.deinit();
        self.filter_results = new_filter_results;
        
        return self.filter_results;
    }
    
    // SearchPropertyKey searches for rows that contain the specified property key
    pub fn searchPropertyKey(self: *SearchMemDB, data_key: []const u8) SearchMemDBError!StringHashMap(*TreeNode) {
        var new_filter_results = StringHashMap(*TreeNode).init(self.allocator);
        
        var filter_iterator = self.filter_results.iterator();
        while (filter_iterator.next()) |entry| {
            const key = entry.key_ptr.*;
            if (self.basic_db.data.get(key)) |node| {
                if (node.data == .object) {
                    if (node.data.object.contains(data_key)) {
                        const key_copy = self.allocator.dupe(u8, key) catch return SearchMemDBError.OutOfMemory;
                        new_filter_results.put(key_copy, entry.value_ptr.*) catch return SearchMemDBError.OutOfMemory;
                    }
                }
            }
        }
        
        // Free old filter results and replace
        var old_iterator = self.filter_results.iterator();
        while (old_iterator.next()) |entry| {
            self.allocator.free(entry.key_ptr.*);
        }
        self.filter_results.deinit();
        self.filter_results = new_filter_results;
        
        return self.filter_results;
    }
    
    // SearchPropertyValue searches for rows where the properties JSON field contains the specified key with the specified value
    pub fn searchPropertyValue(self: *SearchMemDB, data_key: []const u8, data_value: json.Value) SearchMemDBError!StringHashMap(*TreeNode) {
        var new_filter_results = StringHashMap(*TreeNode).init(self.allocator);
        
        var filter_iterator = self.filter_results.iterator();
        while (filter_iterator.next()) |entry| {
            const key = entry.key_ptr.*;
            if (self.basic_db.data.get(key)) |node| {
                if (node.data == .object) {
                    if (node.data.object.get(data_key)) |value| {
                        if (self.jsonValuesEqual(value, data_value)) {
                            const key_copy = self.allocator.dupe(u8, key) catch return SearchMemDBError.OutOfMemory;
                            new_filter_results.put(key_copy, entry.value_ptr.*) catch return SearchMemDBError.OutOfMemory;
                        }
                    }
                }
            }
        }
        
        // Free old filter results and replace
        var old_iterator = self.filter_results.iterator();
        while (old_iterator.next()) |entry| {
            self.allocator.free(entry.key_ptr.*);
        }
        self.filter_results.deinit();
        self.filter_results = new_filter_results;
        
        return self.filter_results;
    }
    
    // Helper function to compare JSON values
    fn jsonValuesEqual(self: *SearchMemDB, a: json.Value, b: json.Value) bool {
        _ = self;
        switch (a) {
            .null => return b == .null,
            .bool => |val_a| return b == .bool and val_a == b.bool,
            .integer => |val_a| return b == .integer and val_a == b.integer,
            .float => |val_a| return b == .float and val_a == b.float,
            .string => |val_a| return b == .string and std.mem.eql(u8, val_a, b.string),
            else => return false, // Simplified comparison for objects and arrays
        }
    }
    
    // SearchStartingPath searches for a specific path and all its descendants
    pub fn searchStartingPath(self: *SearchMemDB, starting_path: []const u8) SearchMemDBError!StringHashMap(*TreeNode) {
        var new_filter_results = StringHashMap(*TreeNode).init(self.allocator);
        
        // Add starting path if it exists in filter results
        if (self.filter_results.get(starting_path)) |node| {
            const key_copy = self.allocator.dupe(u8, starting_path) catch return SearchMemDBError.OutOfMemory;
            new_filter_results.put(key_copy, node) catch return SearchMemDBError.OutOfMemory;
        } else {
            // If starting path doesn't exist, clear filter results
            var old_iterator = self.filter_results.iterator();
            while (old_iterator.next()) |entry| {
                self.allocator.free(entry.key_ptr.*);
            }
            self.filter_results.clearAndFree();
            return new_filter_results;
        }
        
        // Get and add descendants
        const descendants = self.basic_db.queryDescendants(starting_path) catch return SearchMemDBError.QueryError;
        defer {
            for (descendants.items) |*desc| {
                desc.deinit(self.allocator);
            }
            descendants.deinit();
        }
        
        for (descendants.items) |item| {
            if (self.filter_results.get(item.path)) |node| {
                const key_copy = self.allocator.dupe(u8, item.path) catch return SearchMemDBError.OutOfMemory;
                new_filter_results.put(key_copy, node) catch return SearchMemDBError.OutOfMemory;
            }
        }
        
        // Free old filter results and replace
        var old_iterator = self.filter_results.iterator();
        while (old_iterator.next()) |entry| {
            self.allocator.free(entry.key_ptr.*);
        }
        self.filter_results.deinit();
        self.filter_results = new_filter_results;
        
        return new_filter_results;
    }
    
    // SearchPath searches for rows matching the specified LTREE path expression using operators
    pub fn searchPath(self: *SearchMemDB, operator: []const u8, starting_path: []const u8) SearchMemDBError!StringHashMap(*TreeNode) {
        // Use the parent class query method
        const search_results = self.basic_db.queryByOperator(operator, starting_path, "");
        defer {
            for (search_results.items) |*result| {
                result.deinit(self.allocator);
            }
            search_results.deinit();
        }
        
        var new_filter_results = StringHashMap(*TreeNode).init(self.allocator);
        for (search_results.items) |item| {
            if (self.filter_results.get(item.path)) |node| {
                const key_copy = self.allocator.dupe(u8, item.path) catch return SearchMemDBError.OutOfMemory;
                new_filter_results.put(key_copy, node) catch return SearchMemDBError.OutOfMemory;
            }
        }
        
        // Free old filter results and replace
        var old_iterator = self.filter_results.iterator();
        while (old_iterator.next()) |entry| {
            self.allocator.free(entry.key_ptr.*);
        }
        self.filter_results.deinit();
        self.filter_results = new_filter_results;
        
        return self.filter_results;
    }
    
    // FindDescriptions extracts descriptions from all data entries
    pub fn findDescriptions(self: *SearchMemDB) SearchMemDBError!StringHashMap([]const u8) {
        var return_values = StringHashMap([]const u8).init(self.allocator);
        
        var data_iterator = self.basic_db.data.iterator();
        while (data_iterator.next()) |entry| {
            const row_key = entry.key_ptr.*;
            const row_data = entry.value_ptr.*;
            
            var description_value: []const u8 = "";
            
            if (row_data.data == .object) {
                if (row_data.data.object.get("description")) |desc| {
                    if (desc == .string) {
                        description_value = desc.string;
                    }
                }
            }
            
            const key_copy = self.allocator.dupe(u8, row_key) catch return SearchMemDBError.OutOfMemory;
            const value_copy = self.allocator.dupe(u8, description_value) catch return SearchMemDBError.OutOfMemory;
            return_values.put(key_copy, value_copy) catch return SearchMemDBError.OutOfMemory;
        }
        
        return return_values;
    }
    
    // GetFilterResults returns the current filter results
    pub fn getFilterResults(self: *SearchMemDB) SearchMemDBError!StringHashMap(*TreeNode) {
        var results = StringHashMap(*TreeNode).init(self.allocator);
        
        var iterator = self.filter_results.iterator();
        while (iterator.next()) |entry| {
            const key_copy = self.allocator.dupe(u8, entry.key_ptr.*) catch return SearchMemDBError.OutOfMemory;
            results.put(key_copy, entry.value_ptr.*) catch return SearchMemDBError.OutOfMemory;
        }
        
        return results;
    }
    
    // GetFilterResultKeys returns just the keys of current filter results
    pub fn getFilterResultKeys(self: *SearchMemDB) SearchMemDBError!ArrayList([]const u8) {
        var keys = ArrayList([]const u8).init(self.allocator);
        
        var iterator = self.filter_results.iterator();
        while (iterator.next()) |entry| {
            const key_copy = self.allocator.dupe(u8, entry.key_ptr.*) catch return SearchMemDBError.OutOfMemory;
            keys.append(key_copy) catch return SearchMemDBError.OutOfMemory;
        }
        
        return keys;
    }
    
    // Getter methods for accessing internal maps
    pub fn getKBs(self: *SearchMemDB) *const StringHashMap(ArrayList([]const u8)) {
        return &self.kbs;
    }
    
    pub fn getLabels(self: *SearchMemDB) *const StringHashMap(ArrayList([]const u8)) {
        return &self.labels;
    }
    
    pub fn getNames(self: *SearchMemDB) *const StringHashMap(ArrayList([]const u8)) {
        return &self.names;
    }
    
    pub fn getDecodedKeys(self: *SearchMemDB) *const StringHashMap(ArrayList([]const u8)) {
        return &self.decoded_keys;
    }
    
    // Delegate methods to BasicConstructDB
    pub fn validatePath(self: *SearchMemDB, path: []const u8) bool {
        return self.basic_db.validatePath(path);
    }
    
    pub fn store(self: *SearchMemDB, path: []const u8, data: json.Value, created_at: ?[]const u8, updated_at: ?[]const u8) !void {
        return self.basic_db.store(path, data, created_at, updated_at);
    }
    
    pub fn get(self: *SearchMemDB, path: []const u8) !?json.Value {
        return self.basic_db.get(path);
    }
    
    pub fn exists(self: *SearchMemDB, path: []const u8) bool {
        return self.basic_db.exists(path);
    }
    
    pub fn getStats(self: *SearchMemDB) TreeStats {
        return self.basic_db.getStats();
    }
};

// Test function
test "SearchMemDB basic functionality" {
    const testing = std.testing;
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();
    
    var smdb = try SearchMemDB.init(allocator, "localhost", 5432, "test", "user", "pass", "test_table");
    defer smdb.deinit();
    
    // Test that it initializes properly
    try testing.expect(smdb.filter_results.count() == 0); // No data loaded from DB in test
    
    // Test getter methods
    const kbs = smdb.getKBs();
    const labels = smdb.getLabels();
    const names = smdb.getNames();
    const decoded_keys = smdb.getDecodedKeys();
    
    try testing.expect(kbs.count() == 0);
    try testing.expect(labels.count() == 0);
    try testing.expect(names.count() == 0);
    try testing.expect(decoded_keys.count() == 0);
}

