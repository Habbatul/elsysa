const std = @import("std");

const User:type = struct {
    conn: std.net.Server.Connection,
    store: *std.StringHashMap([]const u8),
    storeMutex: *std.Thread.Mutex,
    allocator: std.mem.Allocator,
};

pub fn main () !void{
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    const allocator = gpa.allocator();

    //init everything
    var store = std.StringHashMap([]const u8).init(allocator);
    var mutex : std.Thread.Mutex = .{};

    //init tcp listener
    var addr = try std.net.Address.parseIp("0.0.0.0", 6060);
    var server = try addr.listen(.{});
    std.debug.print("🔴 Listening on 0.0.0.0:6060\n", .{});

    defer{
        server.deinit();
        store.deinit();
        std.debug.assert(gpa.deinit() == .ok);
    }

    while (true) {
        const user = User{
            .conn = server.accept() catch |err|  {
                std.debug.print("Error: {}\n", .{err});
                break;
            },
            .store = &store,
            .storeMutex = &mutex,
            .allocator = allocator,
        };
        _ = try std.Thread.spawn(.{}, handler, .{user});
    }
}

fn handler(user: User) !void{
    defer {
        user.conn.stream.close();
    }
    const reader = user.conn.stream.reader();
    const writer = user.conn.stream.writer();
    var buf: [4096]u8 = undefined;

    while (true) {
        const readData = try reader.readUntilDelimiterOrEof(&buf, '\n');
        const dataResult = readData orelse {
            try writer.print("-ERR empty data json\n", .{});
            break;
        };

        //bentuk perintah nantinya adalah "SET key json\n"
        //ambil kata kedua yaitu perintah SET, GET DEL
        const line = normalizeLine(dataResult);
        var iterator = std.mem.splitScalar(u8, line, ' ');
        const command = iterator.next() orelse continue;

        //pakek reference karena nanti akses iterator.next() ki ngubah data
        if (std.mem.eql(u8, command, "SET")){
            _ = handleSet(&iterator, user, writer) catch break;
        } else if (std.mem.eql(u8, command, "GET")){
            _ = handleGet(&iterator, user, writer) catch break;
        } else if (std.mem.eql(u8, command, "DEL")){
            _= handleDel(&iterator, user, writer) catch break;
        }else {
            writer.print("-ERR command not available\n", .{}) catch break;
        }

    }
}

///Hilangkan karakter \r atau \n yang ada diakhir agar sesuai ketika di split
///Contoh : "makanan ikan ada tiga\r\n" akan menjadi "makanan ikan ada tiga\r"
// fn normalizeLineSimple(line: []const u8) []const u8 {
//     if (line.len > 0 and (line[line.len - 1] == '\r' or line[line.len - 1] == '\n')) {
//         return line[0 .. line.len - 1];
//     }
//     return line;
// }


fn normalizeLine(line: []const u8) []const u8 {
    var start: usize = 0;
    var end: usize = line.len;

    //untuk unix, mac, linux \r, \n, \r\n
    if (end >= 2 and line[end-2] == '\r' and line[end-1] == '\n'){
        end -= 2;
    }else if (end>0 and (line[end-1] == '\r' or line[end-1] == '\n')){
        end -=1;
    }

    //menghilangkan semua \r atau \n bila ditemukan diawal bila ada
    while (start<end and (line[start] == '\r' or line[start] == '\n')) {
        start +=1;
    }

    return line[start..end];
}

//parameter e baca file e, ketok kok nanti tipe return e opo, terus jadikan parameter
fn handleSet(iterator : *std.mem.SplitIterator(u8, .scalar), user: User, writer : std.net.Stream.Writer) !void{

    const key = iterator.next() orelse {
        try writer.print("-ERR there's no key found\n", .{});
        return;
    };
    std.debug.print("Set Key: {s}\n", .{key});

    const valueBytes = iterator.rest();
    if (valueBytes.len == 0){
        try writer.print("-ERR there's no key found\n", .{});
        return;
    }

    const keyCpy = try user.allocator.dupe(u8, key);
    const valueBytesCpy = try user.allocator.dupe(u8, valueBytes);

    user.storeMutex.lock();
    try user.store.put(keyCpy, valueBytesCpy);
    user.storeMutex.unlock();

    try writer.print("+OK\n", .{});
}

fn handleGet(iterator : *std.mem.SplitIterator(u8, .scalar), user: User, writer : std.net.Stream.Writer) !void{
    const key = iterator.next() orelse {
        try writer.print("-ERR there's no key found\n", .{});
        return;
    };

    user.storeMutex.lock();
    const result = user.store.get(key);
    user.storeMutex.unlock();

    if (result) |val| {
        try writer.writeAll(val);
        try writer.writeAll("\n");
    }else{
        try writer.print("$-1\n", .{});
    }
}

fn handleDel(iterator : *std.mem.SplitIterator(u8, .scalar), user: User, writer : std.net.Stream.Writer) !void{
    const key = iterator.next() orelse {
        try writer.print("-ERR there's no key found\n", .{});
        return;
    };

    user.storeMutex.lock();
    const isDeleted = user.store.fetchRemove(key);
    user.storeMutex.unlock();

    if(isDeleted)|val|{
        user.allocator.free(val.key);
        user.allocator.free(val.value);
        // std.debug.print("{s} {s}", .{val.key, val.key});
        try writer.print(":1\n", .{});
    }else{
        try writer.print(":0\n", .{});
    }
}
