// main.zig - Test driver for the math module

const std = @import("std");
const math = @import("src/math/math.zig");

pub fn main() !void {
    const stdout = std.io.getStdOut().writer();
    
    // Test the add function
    const sum = math.add(10, 20);
    try stdout.print("10 + 20 = {}\n", .{sum});
    
    // Test the multiply function
    const product = math.multiply(5, 6);
    try stdout.print("5 * 6 = {}\n", .{product});
}
