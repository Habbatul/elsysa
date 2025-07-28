const std = @import("std");
const math = std.math;

const Entry = @import("user_ctx.zig").Entry;

pub const ExpiryEntry = struct {
    key: []const u8,
    expireAt: i64,
};

pub fn compare(_: void, a: ExpiryEntry, b: ExpiryEntry) math.Order {
    return math.order(a.expireAt, b.expireAt);
}

pub const ExpiryManager = struct {
    allocator: std.mem.Allocator,
    queue: std.PriorityQueue(ExpiryEntry, void, compare),
    store: *std.StringHashMap(*Entry),
    storeMutex: *std.Thread.Mutex,
    queueMutex: std.Thread.Mutex,

    pub fn init(
        allocator: std.mem.Allocator,
        store: *std.StringHashMap(*Entry),
        storeMutex: *std.Thread.Mutex,
    ) !*ExpiryManager {
        const self = try allocator.create(ExpiryManager);
        self.* = ExpiryManager{
            .allocator = allocator,
            .queue = std.PriorityQueue(ExpiryEntry, void, compare).init(allocator, {}),
            .store = store,
            .storeMutex = storeMutex,
            .queueMutex = std.Thread.Mutex{},
        };
        return self;
    }

    pub fn registerExpiry(self: *ExpiryManager, key: []const u8, expireAt: i64) !void {
        const key_copy = try self.allocator.dupe(u8, key);
        self.queueMutex.lock();
        defer self.queueMutex.unlock();
        try self.queue.add(ExpiryEntry{ .key = key_copy, .expireAt = expireAt });
    }

    pub fn spawnWorker(self: *ExpiryManager) !void {
        _ = try std.Thread.spawn(.{}, expiryLoop, .{self});
    }

    fn expiryLoop(self: *ExpiryManager) void {
        while (true) {
            std.time.sleep(2 * std.time.ns_per_s);

            while (self.queue.peek()) |expEntry| {
                const now = std.time.timestamp();
                if (expEntry.expireAt > now) break;
                _ = self.queue.remove();

                self.storeMutex.lock();
                const removed = self.store.fetchRemove(expEntry.key);
                self.storeMutex.unlock();

                if (removed) |val| {
                    self.allocator.free(val.value.key);
                    self.allocator.free(val.value.flags);
                    self.allocator.free(val.value.value);
                    self.allocator.destroy(val.value);
                }

                self.allocator.free(expEntry.key);
            }
        }
    }
};
