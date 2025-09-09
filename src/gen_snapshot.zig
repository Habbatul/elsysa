const std = @import("std");
const main = @import("main.zig");
const Allocator = std.mem.Allocator;

const fileName = "server_snapshot.bin";

pub fn saveSnapshot(store: *const std.StringHashMap(*main.Entry), storeMutex: *std.Thread.RwLock) !void {
    std.log.info("Memulai proses penyimpanan snapshot...", .{});

    storeMutex.lockShared();
    defer storeMutex.unlockShared();

    const file = try std.fs.cwd().createFile(fileName, .{ .truncate = true });
    defer file.close();

    const mapCount = store.count();

    var countBytes: [8]u8 = undefined;
    std.mem.writeInt(u64, &countBytes, mapCount, .little);
    try file.writeAll(&countBytes);

    var iter = store.iterator();

    while (iter.next()) |mapEntry| {
        const entry = mapEntry.value_ptr.*.*;

        var lineBytes: [8]u8 = undefined;
        var bytesBuffer: [8]u8 = undefined;

        std.mem.writeInt(u64, &lineBytes, entry.key.len, .little);
        try file.writeAll(&lineBytes);
        try file.writeAll(entry.key);

        std.mem.writeInt(u64, &lineBytes, entry.flags.len, .little);
        try file.writeAll(&lineBytes);
        try file.writeAll(entry.flags);

        std.mem.writeInt(u64, &bytesBuffer, entry.bytes, .little);
        try file.writeAll(&bytesBuffer);

        std.mem.writeInt(u64, &lineBytes, entry.value.len, .little);
        try file.writeAll(&lineBytes);
        try file.writeAll(entry.value);
    }

    std.log.info("snapshot berhasil disimpan, {d} item ditulis.", .{mapCount});
}

pub fn loadSnapshot(allocator: Allocator, store: *std.StringHashMap(*main.Entry)) !void {
    std.log.info("coba load snapshot dari '{s}'...", .{fileName});

    const file = std.fs.cwd().openFile(fileName, .{}) catch |err| {
        if (err == error.FileNotFound) {
            std.log.warn("file snapshot ga adaa, mulai dengan store kosong.", .{});
            return;
        }
        return err;
    };
    defer file.close();

    var countBytes: [8]u8 = undefined;
    _ = try file.readAll(&countBytes);
    const mapCount = std.mem.readInt(u64, &countBytes, .little);
    std.log.info("file snapshot ditemukan, load {d} item...", .{mapCount});

    for (0..mapCount) |_| {
        var lineBytes: [8]u8 = undefined;
        var bytesBuffer: [8]u8 = undefined;

        _ = try file.readAll(&lineBytes);
        const keyLen = std.mem.readInt(u64, &lineBytes, .little);
        const key = try allocator.alloc(u8, @intCast(keyLen));
        _ = try file.readAll(key);

        _ = try file.readAll(&lineBytes);
        const flagLen = std.mem.readInt(u64, &lineBytes, .little);
        const flags = try allocator.alloc(u8, @intCast(flagLen));
        _ = try file.readAll(flags);

        _ = try file.readAll(&bytesBuffer);
        const bytes = std.mem.readInt(u64, &bytesBuffer, .little);

        _ = try file.readAll(&lineBytes);
        const valueLen = std.mem.readInt(u64, &lineBytes, .little);
        const value = try allocator.alloc(u8, @intCast(valueLen));
        _ = try file.readAll(value);

        const newEntry = try allocator.create(main.Entry);
        newEntry.* = .{
            .key = key,
            .flags = flags,
            .bytes = @intCast(bytes),
            .value = value,
        };

        try store.put(key, newEntry);
    }

    std.log.info("snapshot berhasil diload dengan {d} item.", .{store.count()});
}
