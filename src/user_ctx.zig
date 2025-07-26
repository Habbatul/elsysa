const std = @import("std");

pub const User = struct {
    conn: std.net.Server.Connection,
    store: *std.StringHashMap(*Entry),
    storeMutex: *std.Thread.Mutex,
    allocator: std.mem.Allocator,
};

pub const Entry = struct {
    key: []const u8,
    flags: []const u8,
    bytes: usize,
    value: []const u8,
};