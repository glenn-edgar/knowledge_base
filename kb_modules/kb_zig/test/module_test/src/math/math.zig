const std = @import("std");

pub fn add(a: i32, b: i32) i32 {
    return a + b;
}

pub fn multiply(a: i32, b: i32) i32 {
    return a * b;
}

// Unit tests for the module
test "math operations" {
    const expect = std.testing.expect;
    try expect(add(2, 3) == 5);
    try expect(multiply(4, 5) == 20);
}

