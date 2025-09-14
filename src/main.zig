
const std = @import("std");

pub const User = struct {
    conn: std.net.Server.Connection,
    store: *std.StringHashMap(*Entry),
    storeMutex: *std.Thread.RwLock,
    allocator: std.mem.Allocator,
};

pub const Entry = struct {
    key: []const u8,
    flags: []const u8,
    bytes: usize,
    value: []const u8,
};

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

const snapshot = @import("gen_snapshot.zig");

pub fn main() !void {
    // var gpa:std.heap.GeneralPurposeAllocator(.{.thread_safe = true, .MutexType = std.Thread.Mutex,}) = .init;
    // const allocator = gpa.allocator();

    const allocator = std.heap.raw_c_allocator;
    var store = std.StringHashMap(*Entry).init(allocator);
    var mutex: std.Thread.RwLock = .{};

    var addr = try std.net.Address.parseIp("0.0.0.0", 6060);
    var server = try addr.listen(.{.kernel_backlog = 1024});
    std.debug.print("🔴 Listening on 0.0.0.0:6060\n", .{});

    const threadCount = try std.Thread.getCpuCount();
    var pool : std.Thread.Pool = undefined;
    try pool.init(.{
        .allocator = allocator,
        .n_jobs = threadCount,
    });

    defer {
        server.deinit();
        store.deinit();
        pool.deinit();
        // if (gpa.deinit() == .leak){}
    }

    snapshot.loadSnapshot(allocator, &store) catch |err| {
        std.log.err("gagal memuat snapshot: {}", .{err});
        return err;
    };

    const SNAPSHOT_INTERVAL_SECONDS: i64 = 30; //sekitar 30 menit
    var lasSnapshotTimestamp = std.time.timestamp();
    
    while (true) {
        var pollFds = [_] std.posix.pollfd{
            .{
                .fd = server.stream.handle,
                .events = std.posix.POLL.IN,
                .revents = 0,
            },
        };       

        const numEvents = std.posix.poll(&pollFds, 1000) catch |err| {
            std.log.err("ada error bro: {}", .{err});
            continue;
        };

        if (numEvents > 0) {
            //cuma jangan mati ae ketika ada error
            const user = User{
                .conn = server.accept() catch |err| {
                    std.debug.print("Error: {}\n", .{err});
                    continue;
                },
                .store = &store,
                .storeMutex = &mutex,
                .allocator = allocator,
            };

            try pool.spawn(handlerCannotErr, .{user});

        } else {
            const currentTimestamp = std.time.timestamp();
            if (currentTimestamp - lasSnapshotTimestamp >= SNAPSHOT_INTERVAL_SECONDS) {
                snapshot.saveSnapshot(&store, &mutex) catch |err| {
                    std.log.err("Gagal simpan snapshot: {}", .{err});
                };

                lasSnapshotTimestamp = currentTimestamp;
            }
        }

    }
}

fn handlerCannotErr(user: User) void {
    var arena = std.heap.ArenaAllocator.init(user.allocator);
    defer arena.deinit();
    const arenaAllocator = arena.allocator();

    const userPtr = arenaAllocator.create(User) catch |err| {
        std.log.err("ada error di alloc user struct {}\n", .{err});
        return;
    };

    userPtr.* = user;


    handler(userPtr.*, arenaAllocator) catch  {
        // std.log.debug("ada error di handler: {s}\n", .{@errorName(err)});
    };
}

fn handler(user: User, arenaAlloc: std.mem.Allocator) !void {
    defer user.conn.stream.close();

    // const readBuffer = try arenaAlloc.alloc(u8, 8192);
    // const writeBuffer = try arenaAlloc.alloc(u8, 8192);

    const READ_BUF_SIZE = 64 * 1024;
    var readBuffer:[READ_BUF_SIZE]u8 = undefined;
    const WRITE_BUF_SIZE = 64 * 1024;
    var writeBuffer:[WRITE_BUF_SIZE]u8 = undefined;

    var streamReader = user.conn.stream.reader(&readBuffer);
    const reader = &streamReader.interface_state;

    var streamWriter = user.conn.stream.writer(&writeBuffer);
    const writer = &streamWriter.interface;

    mainLoop:while (true) {

        const rawLine = reader.takeDelimiterExclusive('\n') catch |err| switch (err) {
            error.EndOfStream => break:mainLoop,
            else => {
                std.log.err("gatay la{}\n", .{err});
                return err;
            },
        };
            
        const line = normalizeLine(rawLine);
        const header = Header.parse(line) catch {
            try writer.print("-ERR invalid command format\r\n", .{});
            try writer.flush();
            continue;
        };
        
        if (std.mem.eql(u8, header.command, "SET")) {
            _ = handleSet(header, reader, user, writer, arenaAlloc) catch |err| {
                std.log.err("{}\n", .{err});
                break;
            };
        } else if (std.mem.eql(u8, header.command, "GET")) {
            _ = handleGet(header, user, writer) catch |err| {
                std.log.err("{}\n", .{err});
                break;
            };
        } else if (std.mem.eql(u8, header.command, "DEL")) {
            _ = handleDel(header, user, writer) catch |err| {
                std.log.err("{}\n", .{err});
                break;
            };
        } else {
            std.log.err("gatau lah command nya : {s}\n",.{header.command});
            try writer.print("-ERR command not available\r\n", .{});
            try writer.flush(); 
        }

        try writer.flush();
    } 

}

fn normalizeLine(line: []const u8) []const u8 {
    return std.mem.trim(u8, line, "\r\n ");
}

fn handleSet(
    header: Header,
    reader: *std.Io.Reader,
    user: User,
    writer: *std.Io.Writer,
    arenaAlloc: std.mem.Allocator,
) !void {
    var valueBuf = try arenaAlloc.alloc(u8, header.bytes + 2);
    try reader.*.readSliceAll(valueBuf);
    // const valueBuf = try reader.*.readAlloc(arenaAlloc, header.bytes+2 );

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
        .value = valCpy
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

    // try writer.print("+OK\r\n", .{});
    try writer.*.writeAll("+OK\r\n");
}

fn handleGet(
    header: Header,
    user: User,
    writer: *std.Io.Writer,
) !void {
    const key = header.key;

    user.storeMutex.lockShared();
    const result = user.store.get(key);
    user.storeMutex.unlockShared();

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
    writer: *std.Io.Writer,
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
