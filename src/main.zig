const std = @import("std");

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
            .bytes = b,
        };
    }
};

pub fn main() !void {
    var gpa: std.heap.GeneralPurposeAllocator(.{ .thread_safe = true }) = .{};
    defer std.debug.assert(gpa.deinit() == .ok);
    const allocator = gpa.allocator();

    var store = std.StringHashMap(*Entry).init(allocator);
    var mutex: std.Thread.Mutex = .{};

    var addr = try std.net.Address.parseIp("0.0.0.0", 6060);
    var server = try addr.listen(.{ .kernel_backlog = 1024 });
    std.debug.print("🔴 Listening on 0.0.0.0:6060\n", .{});

    defer {
        server.deinit();
        store.deinit();
    }

    const threadCount = try std.Thread.getCpuCount();
    var pool: std.Thread.Pool = undefined;
    try pool.init(.{
        .allocator = allocator,
        .n_jobs = threadCount,
    });

    defer pool.deinit();

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

        try pool.spawn(handlerCannotErr, .{user});
    }
}

fn handlerCannotErr(user: User) void {
    var arena = std.heap.ArenaAllocator.init(user.allocator);
    defer arena.deinit();
    const arenaAlloc = arena.allocator();

    handler(user, arenaAlloc) catch |err| {
        std.log.debug("ada error di handler: {s}\n", .{@errorName(err)});
    };
}

fn handler(user: User, arenaAlloc: std.mem.Allocator) !void {
    defer {
        user.conn.stream.close();
    }
    const writer = user.conn.stream.writer();
    var bufio = std.io.bufferedReader(user.conn.stream.reader());
    const reader = bufio.reader();
    // var buf: [4096]u8 = undefined;

    while (true) {
        // const readData = try reader.readUntilDelimiterOrEof(&buf, '\n');
        const readData = try readLineSIMD(&bufio);
        const dataResult = readData orelse {
            try writer.print("-ERR empty data json\r\n", .{});
            break;
        };
        const line = normalizeLine(dataResult);

        const header = try Header.parse(line);

        if (std.mem.eql(u8, header.command, "SET")) {
            _ = handleSet(header, reader, user, writer, arenaAlloc) catch break;
        } else if (std.mem.eql(u8, header.command, "GET")) {
            _ = handleGet(header, user, writer) catch break;
        } else if (std.mem.eql(u8, header.command, "DEL")) {
            _ = handleDel(header, user, writer) catch break;
        } else {
            writer.print("-ERR command not available\r\n", .{}) catch break;
        }
    }
}

pub fn readLineSIMD(buffered: anytype) !?[]u8 {
    const target:u8 = '\n';
    const chunkSize:usize = 16;
    const Vec = @Vector(chunkSize, u8);

    while (true){
        const start = buffered.start;
        const end = buffered.end;

        if (start == end){
            const n = try buffered.unbuffered_reader.read(buffered.buf[0..]);
            if (n == 0) return null;
            buffered.start = 0;
            buffered.end = n;
            continue;
        }

        const sliceToProcess = buffered.buf[start..end];
        const targetVec: Vec = @splat(target);

        var i:usize = 0;
        while (i + chunkSize <= sliceToProcess.len) : (i += chunkSize) {
            // const vec = std.mem.bytesToValue(Vec, sliceToProcess[i..i+chunkSize]);
            // const mask = vec == targetVec;
            const vec: *Vec = @alignCast(@ptrCast(sliceToProcess[i..i+chunkSize])); 
            const mask = vec.* == targetVec;
            const intMask:u16 = @bitCast(mask);


            if (intMask != 0) {
                const newLineOffsite = @ctz(intMask);
                const posInSlice = i+newLineOffsite;

                buffered.start += posInSlice+1;
                return sliceToProcess[0..posInSlice];
            }
        }

        var j = i;
        while (j < sliceToProcess.len) : (j += 1) {
            if (sliceToProcess[j] == target) {
                buffered.start += j+1;
                return sliceToProcess[0..j];
            }
        }

        const remainingSlice = sliceToProcess[i..];
        @memcpy(buffered.buf[0..remainingSlice.len], remainingSlice);
        buffered.start = 0;
        buffered.end = remainingSlice.len;

        const n = try buffered.unbuffered_reader.read(buffered.buf[remainingSlice.len..]);
        if (n == 0) {
            if (buffered.end == 0) return null;
            const line = buffered.buf[0..buffered.end];
            buffered.start = buffered.end;
            return line;
        }
        buffered.end += n;

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
    arenaAlloc: std.mem.Allocator,
) !void {
    var valueBuf = try arenaAlloc.alloc(u8, header.bytes + 2);

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

    const value = valueBuf[0..header.bytes];

    user.storeMutex.lock();
    defer user.storeMutex.unlock();

    const keyCpy = try user.allocator.dupe(u8, header.key);
    const valCpy = try user.allocator.dupe(u8, value);
    const flagCpy = try user.allocator.dupe(u8, header.flags);

    const entry = try user.allocator.create(Entry);
    entry.* = Entry{
        .key = keyCpy,
        .flags = flagCpy,
        .bytes = header.bytes,
        .value = valCpy,
    };


    const removedEntry = user.store.fetchRemove(header.key);

    _ = user.store.put(keyCpy, entry) catch {
        user.storeMutex.unlock();
        user.allocator.free(keyCpy);
        user.allocator.free(flagCpy);
        user.allocator.free(valCpy);
        user.allocator.destroy(entry);
        return error.FailedToPut;
    };
      
    if (removedEntry) |oldEntry| {
        user.allocator.free(oldEntry.value.key);
        user.allocator.free(oldEntry.value.flags);
        user.allocator.free(oldEntry.value.value);
        user.allocator.destroy(oldEntry.value);
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
        try writer.print(":1\r\n", .{});
    } else {
        try writer.print(":0\r\n", .{});
    }
}
