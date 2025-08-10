const DistrictCode = u16;

const Sector = struct {
    district: DistrictCode,
    code: u8,
    units: []RawUnit,
};

const lat_min = 49.8;
const lon_min = -8.2;
const lat_res = 0.001;
const lon_res = 0.001;

const known_max_lat_index = 50200;
const known_max_lon_index = 9960;

// code has less grouping, give it priority
const delta0_max = 1024;
const delta1_max = 64;
const delta2_max = 64;

const RawUnit = struct {
    code: u16,
    lat_index: u32,
    lon_index: u32,
};

const Postcode = struct {
    district: DistrictCode,
    sector: u8,
    unit: u16,
};

const PostcodeLine = struct {
    postcode: Postcode,
    longitude: f32,
    latitude: f32,
    lineLength: usize,
};

const DistrictCodeMap = std.StringArrayHashMap(DistrictCode);

fn parseLine(district_map: *DistrictCodeMap, line: [*]const u8, bytes_remaining: usize) !PostcodeLine {
    if (bytes_remaining < 400) {
        return error.EOF;
    }

    var i: usize = 1;
    i += 4;
    const district_slice = line[1..i];
    const sector: u8 = line[i] - '0';
    i += 1;
    const unit: u16 = @as(u16, line[i] - 'A') * @as(u16, line[i + 1] - 'A');
    i += 2;

    var commaCounter: usize = 0;
    while (true) {
        if (i + 50 >= bytes_remaining) return error.NotEnoughBytes;
        if (commaCounter == 41) break;
        if (line[i] == ',') commaCounter += 1;
        i += 1;
    }

    const latitude_start = i;
    while (line[i] != ',') : (i += 1) {}
    const latitude_slice = line[latitude_start..i];

    i += 1;

    const longitude_start = i;
    while (line[i] != ',') : (i += 1) {}
    const longitude_slice = line[longitude_start..i];

    while (line[i] != '\n') : (i += 1) {}

    const district_entry = try district_map.getOrPutValue(district_slice, @intCast(district_map.count()));

    return PostcodeLine{
        .postcode = Postcode{
            .district = district_entry.value_ptr.*,
            .sector = sector,
            .unit = unit,
        },
        .latitude = try std.fmt.parseFloat(f32, latitude_slice),
        .longitude = try std.fmt.parseFloat(f32, longitude_slice),
        .lineLength = i + 1,
    };
}

fn createSector(allocator: std.mem.Allocator, district: DistrictCode, code: u8, units: []const RawUnit) !Sector {
    var sector = Sector{
        .district = district,
        .code = code,
        .units = undefined,
    };

    const sorted = try allocator.alloc(RawUnit, units.len);
    var storedCount: usize = 1;

    var remaining = try std.ArrayList(RawUnit).initCapacity(allocator, units.len - 1);
    defer remaining.deinit();
    remaining.expandToCapacity();
    @memcpy(remaining.items, units[1..]);

    var current = units[0];
    sorted[0] = current;

    const MAX_U64: u64 = (1 << 64) - 1;

    while (remaining.items.len > 0) {
        var min_encoded = MAX_U64;
        var best_index: usize = 0;

        for (remaining.items, 0..) |candidate, i| {
            const encoded = encodeDelta(current, candidate) orelse MAX_U64;

            if (encoded < min_encoded) {
                min_encoded = encoded;
                best_index = i;
            }
        }

        current = remaining.orderedRemove(best_index);
        sorted[storedCount] = current;
        storedCount += 1;
    }

    sector.units = sorted;

    return sector;
}

fn parseToSectors(allocator: std.mem.Allocator, district_map: *DistrictCodeMap, csv_contents: []const u8) ![]Sector {
    var bytesRemaining = csv_contents.len;
    var index: usize = 0;
    var lines: usize = 0;
    defer std.debug.print("[*] Parsed {d} postcodes\n", .{lines});

    var sectors = std.ArrayList(Sector).init(allocator);

    var lastDistrict: DistrictCode = 0;
    var lastSector: u8 = 0;

    var units: std.ArrayList(RawUnit) = std.ArrayList(RawUnit).init(allocator);
    defer units.deinit();

    var max_lat_index: u16 = 0;
    var max_lon_index: u16 = 0;

    while (true) {
        const line = parseLine(district_map, csv_contents[index..].ptr, bytesRemaining) catch |err| switch (err) {
            error.EOF => {
                const sector = try createSector(allocator, lastDistrict, lastSector, units.items);
                try sectors.append(sector);
                break;
            },
            else => return err,
        };

        if (line.postcode.district != lastDistrict or line.postcode.sector != lastSector) {
            const sector = try createSector(allocator, lastDistrict, lastSector, units.items);
            try sectors.append(sector);
            units.clearRetainingCapacity();
        }

        const lat_index: u16 = @intFromFloat(@round((line.latitude - lat_min) / lat_res));
        const lon_index: u16 = @intFromFloat(@round((line.longitude - lon_min) / lon_res));

        if (lat_index > max_lat_index)
            max_lat_index = lat_index;
        if (lon_index > max_lon_index)
            max_lon_index = lon_index;

        try units.append(RawUnit{
            .code = line.postcode.unit,
            .lat_index = lat_index,
            .lon_index = lon_index,
        });

        lastSector = line.postcode.sector;
        lastDistrict = line.postcode.district;

        bytesRemaining -|= line.lineLength;
        index += line.lineLength;
        lines += 1;
    }

    std.debug.print("[*] Index bounds: Lat {d} Lon {d}\n", .{ max_lat_index, max_lon_index });
    if (max_lat_index > known_max_lat_index) {
        std.debug.panic("[!] Lat index {d} is higher than known max index {d}\n", .{ max_lat_index, known_max_lat_index });
    }
    if (max_lon_index > known_max_lon_index) {
        std.debug.panic("[!] Lon index {d} is higher than known max index {d}\n", .{ max_lon_index, known_max_lon_index });
    }

    return try sectors.toOwnedSlice();
}

const DistrictCodePair = struct {
    code: []const u8,
    index: DistrictCode,
};

fn zigzag(n: i32) u32 {
    return @intCast((n << 1) ^ (n >> 31));
}

fn sortDistrictCodesFn(_: usize, lhs: DistrictCodePair, rhs: DistrictCodePair) bool {
    return lhs.index < rhs.index;
}

fn unitToBytes(unit: RawUnit) [5]u8 {
    const coordinates: u29 = @intCast(unit.lon_index * known_max_lat_index + unit.lat_index);
    const unit_code: u40 = (@as(u40, unit.code) << 29) | coordinates;
    return .{
        @intCast((unit_code >> 32) & 0xFF),
        @intCast((unit_code >> 24) & 0xFF),
        @intCast((unit_code >> 16) & 0xFF),
        @intCast((unit_code >> 8) & 0xFF),
        @intCast(unit_code & 0xFF),
    };
}

fn encodeDelta(lastUnit: RawUnit, unit: RawUnit) ?u64 {
    const delta = [3]u32{
        zigzag(@as(i32, lastUnit.code) - @as(i32, unit.code)),
        zigzag(@as(i32, @intCast(lastUnit.lat_index)) - @as(i32, @intCast(unit.lat_index))),
        zigzag(@as(i32, @intCast(lastUnit.lon_index)) - @as(i32, @intCast(unit.lon_index))),
    };

    return if (delta[0] < delta0_max and delta[1] < delta1_max and delta[1] < delta2_max)
        delta[2] * (delta1_max + 1) * (delta0_max + 1) + delta[1] * (delta0_max + 1) + delta[0]
    else
        null;
}

fn createBundle(allocator: std.mem.Allocator, district_map: DistrictCodeMap, sectors: []const Sector) !std.ArrayList(u8) {
    var districtMapIterator = district_map.iterator();

    const district_buffer = try allocator.alloc(DistrictCodePair, district_map.count());
    defer allocator.free(district_buffer);

    var didx: usize = 0;
    while (districtMapIterator.next()) |entry| : (didx += 1) {
        district_buffer[didx] = DistrictCodePair{
            .code = entry.key_ptr.*,
            .index = entry.value_ptr.*,
        };
    }

    std.sort.block(DistrictCodePair, district_buffer, @as(usize, 0), sortDistrictCodesFn);

    const owned_sectors = try allocator.dupe(Sector, sectors);
    defer allocator.free(owned_sectors);

    var bundle = std.ArrayList(u8).init(allocator);

    try bundle.appendSlice(&std.mem.toBytes(@as(u16, @intCast(district_buffer.len))));
    for (district_buffer) |district| {
        try bundle.appendSlice(district.code);
    }

    var lastUnit: ?RawUnit = null;
    var units: usize = 0;
    for (owned_sectors) |sector| {
        // ~3200 districts max (u12) + 10 sectors (u4)
        const sector_code: u16 = (sector.district << 12) | sector.code;
        try bundle.appendSlice(&std.mem.toBytes(sector_code));

        try bundle.appendSlice(&std.mem.toBytes(@as(u16, @intCast(sector.units.len))));
        for (sector.units) |unit| {
            if (lastUnit) |lu| {
                if (encodeDelta(lu, unit)) |delta| {
                    var n: u64 = delta;

                    while (n != 0) {
                        const byte: u8 = @as(u8, @truncate(n)) & 0x7F;
                        n >>= 7;
                        try bundle.append(byte | 0x80);
                    }

                    units += 1;
                    lastUnit = unit;
                    continue;
                }
            }

            try bundle.appendSlice(&unitToBytes(unit));

            units += 1;
            lastUnit = unit;
        }
    }

    std.debug.print("[*] Packed {d} postcodes to {d} KiB\n", .{ units, bundle.items.len / 1024 });

    return bundle;
}

fn deinitSectors(allocator: std.mem.Allocator, sectors: []const Sector) void {
    for (sectors) |sector| {
        allocator.free(sector.units);
    }
    allocator.free(sectors);
}

pub fn main() !void {
    const file_path = "ONSPD_MAY_2025_UK.csv";

    const allocator = std.heap.smp_allocator;

    std.debug.print("[*] Started\n", .{});

    // read entire file, 2 GiB allocation limit
    const file_data = try std.fs.cwd().readFileAlloc(allocator, file_path, 2 * 1024 * 1024 * 1024);
    defer allocator.free(file_data);

    std.debug.print("[*] Loaded CSV\n", .{});

    var districts_map = DistrictCodeMap.init(allocator);
    defer districts_map.deinit();

    const sectors = try parseToSectors(allocator, &districts_map, file_data);
    defer deinitSectors(allocator, sectors);

    const bundle = try createBundle(allocator, districts_map, sectors);
    defer bundle.deinit();

    std.debug.print("[*] Writing bundle...\n", .{});

    const bundle_file = try std.fs.cwd().createFile("out.bin", .{});
    defer bundle_file.close();

    try bundle_file.writeAll(bundle.items);

    std.debug.print("[*] Completed tasks\n", .{});
}

const std = @import("std");
