const std = @import("std");
const coro = @import("coro");

const ExpiryManager = @import("expiry.zig").ExpiryManager;

const User = @import("user_ctx.zig").User;
const Entry = @import("user_ctx.zig").Entry;

const Header = struct {
    command: []const u8,
    key: []const u8,
    flags: []const u8,
    exptime: i64,
    bytes: usize,

    pub fn parse(line: []const u8) !Header {
        var iter = std.mem.splitScalar(u8, line, ' ');
        const c = iter.next() orelse return error.InvalidHeader;
        const k = iter.next() orelse return error.InvalidHeader;
        const f = iter.next() orelse "0";
        const e_str = iter.next() orelse "0";
        const b_str = iter.next() orelse "0";
        const e = std.fmt.parseInt(i64, e_str, 10) catch return error.InvalidHeader;
        const b = std.fmt.parseInt(usize, b_str, 10) catch return error.InvalidHeader;
        return Header{
            .command = c,
            .key = k,
            .flags = f,
            .exptime = e,
            .bytes = b
        };
    }
};

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    const allocator = gpa.allocator();

    //init everything
    var store = std.StringHashMap(*Entry).init(allocator);
    var mutex: std.Thread.Mutex = .{};

    //init tcp listener
    var addr = try std.net.Address.parseIp("0.0.0.0", 6060);
    var server = try addr.listen(.{});
    std.debug.print("🔴 Listening on 0.0.0.0:6060\n", .{});

    //lib coro buat ganti thread.spawn
    var scheduler = try coro.Scheduler.init(gpa.allocator(), .{});
    var pool = try coro.ThreadPool.init(gpa.allocator(), .{});

    defer {
        scheduler.deinit();
        pool.deinit();

        server.deinit();
        store.deinit();
        std.debug.assert(gpa.deinit() == .ok);
    }

    const expiryManager = try ExpiryManager.init(allocator, &store, &mutex);
    try expiryManager.spawnWorker();

    while (true) {
        const user = User{
            .conn = server.accept() catch |err| {
                std.debug.print("Error: {}\n", .{err});
                break;
            },
            .store = &store,
            .storeMutex = &mutex,
            .allocator = allocator,
        };

        // _ = try std.Thread.spawn(.{}, handler, .{user, expiryManager});
        // _ = try scheduler.spawn(handler, .{user, expiryManager}, .{});
        _ = try pool.spawnForCompletion(&scheduler, handler, .{user, expiryManager});
    }
    try scheduler.run(.wait);
}

fn handler(user: User, expManager: *ExpiryManager) !void {
    defer {
        user.conn.stream.close();
    }
    // const reader = user.conn.stream.reader();
    const writer = user.conn.stream.writer();
    var bufio = std.io.bufferedReader(user.conn.stream.reader());
    var reader = bufio.reader();
    var buf: [4096]u8 = undefined;

    while (true) {
        const readData = try reader.readUntilDelimiterOrEof(&buf, '\n');
        const dataResult = readData orelse {
            try writer.print("-ERR empty data json\r\n", .{});
            break;
        };
        const line = normalizeLine(dataResult);

        const header = try Header.parse(line);

        //pakek reference karena nanti akses iterator.next() ki ngubah data
        if (std.mem.eql(u8, header.command, "SET")) {
            _ = handleSet(header, reader, user, writer, expManager) catch break;
        } else if (std.mem.eql(u8, header.command, "GET")) {
            _ = handleGet(header, user, writer) catch break;
        } else if (std.mem.eql(u8, header.command, "DEL")) {
            _ = handleDel(header, user, writer) catch break;
        } else {
            writer.print("-ERR command not available\r\n", .{}) catch break;
        }
    }
}

fn normalizeLine(line: []const u8) []const u8 {
    var start: usize = 0;
    var end: usize = line.len;

    if (end >= 2 and line[end - 2] == '\r' and line[end - 1] == '\n') {
        end -= 2;
    } else if (end > 0 and (line[end - 1] == '\r' or line[end - 1] == '\n')) {
        end -= 1;
    }
    while (start < end and (line[start] == '\r' or line[start] == '\n')) {
        start += 1;
    }

    return line[start..end];
}

fn handleSet(
    header: Header,
    reader: anytype,
    user: User,
    writer: std.net.Stream.Writer,
    expManager: *ExpiryManager,
) !void {
    var valueBuf = try user.allocator.alloc(u8, header.bytes + 2);
    defer user.allocator.free(valueBuf);
    var start: usize = 0;
    while (start < header.bytes + 2) {
        const n = try reader.read(valueBuf[start..]);
        if (n == 0) {
            try writer.print("-ERR unexpected EOF\r\n", .{});
            return;
        }
        start += n;
    }
    if (!(valueBuf[header.bytes] == '\r' and valueBuf[header.bytes + 1] == '\n')) {
        try writer.print("-ERR value must end with \\r\\n\r\n", .{});
        return;
    }

    // std.debug.print("Set Key: {s}\r\n", .{header.key});

    const value = valueBuf[0 .. header.bytes];

    const keyCpy = try user.allocator.dupe(u8, header.key);
    const valCpy = try user.allocator.dupe(u8, value);
    const flagCpy = try user.allocator.dupe(u8, header.flags);

    const entry = try user.allocator.create(Entry);
    entry.* = Entry{
        .key = keyCpy,
        .flags = flagCpy,
        .bytes = header.bytes,
        .value = valCpy
    };

    user.storeMutex.lock();
    _ = user.store.put(keyCpy, entry) catch {
        user.storeMutex.unlock();
        user.allocator.free(keyCpy);
        user.allocator.free(flagCpy);
        user.allocator.free(valCpy);
        user.allocator.destroy(entry);
        return error.FailedToPut;
    };
    user.storeMutex.unlock();

    if (header.exptime != 0) {
        const expireAt = std.time.timestamp() + header.exptime;
        try expManager.registerExpiry(header.key, expireAt);
    }

    try writer.print("+OK\r\n", .{});
}

fn handleGet(
    header: Header,
    user: User,
    writer: std.net.Stream.Writer,
) !void {
    const key = header.key;

    user.storeMutex.lock();
    const result = user.store.get(key);
    user.storeMutex.unlock();

    if (result) |val| {
        try writer.print("{s} {s} {d}\r\n", .{ val.key, val.flags, val.bytes });
        try writer.writeAll(val.value);
        try writer.print("\r\n", .{});
    } else {
        try writer.print("$-1\r\n", .{});
    }
}

fn handleDel(
    header: Header,
    user: User,
    writer: std.net.Stream.Writer,
) !void {
    const key = header.key;

    user.storeMutex.lock();
    const isDeleted = user.store.fetchRemove(key);
    user.storeMutex.unlock();

    if (isDeleted) |val| {
        user.allocator.free(val.value.key);
        user.allocator.free(val.value.flags);
        user.allocator.free(val.value.value);
        user.allocator.destroy(val.value);
        // std.debug.print("{s} {s}", .{val.key, val.key});
        try writer.print(":1\r\n", .{});
    } else {
        try writer.print(":0\r\n", .{});
    }
}
