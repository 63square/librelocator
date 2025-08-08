const DistrictCode = u16;

const Sector = struct {
    district: DistrictCode,
    code: u8,
    units: []RawUnit,
};

const lat_min = 49.9;
const lon_min = -8.6;
const lat_res = 0.001;
const lon_res = 0.001;

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

/// Everything owned by caller, make sure to clean up
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

    while (true) {
        const line = parseLine(district_map, csv_contents[index..].ptr, bytesRemaining) catch |err| switch (err) {
            error.EOF => {
                const sector = Sector{
                    .district = lastDistrict,
                    .code = lastSector,
                    .units = try allocator.alloc(RawUnit, units.items.len),
                };

                @memcpy(sector.units, units.items);
                try sectors.append(sector);
                break;
            },
            else => return err,
        };

        if (line.postcode.district != lastDistrict or line.postcode.sector != lastSector) {
            const sector = Sector{
                .district = lastDistrict,
                .code = lastSector,
                .units = try allocator.alloc(RawUnit, units.items.len),
            };
            @memcpy(sector.units, units.items);
            try sectors.append(sector);
            units.clearRetainingCapacity();
        }

        try units.append(RawUnit{
            .code = line.postcode.unit,
            .lat_index = @intFromFloat((line.latitude - lat_min) / lat_res),
            .lon_index = @intFromFloat((line.longitude - lon_min) / lon_res),
        });

        lastSector = line.postcode.sector;
        lastDistrict = line.postcode.district;

        bytesRemaining -|= line.lineLength;
        index += line.lineLength;
        lines += 1;
    }

    return try sectors.toOwnedSlice();
}

const DistrictCodePair = struct {
    code: []const u8,
    index: DistrictCode,
};

fn sortDistrictCodesFn(_: usize, lhs: DistrictCodePair, rhs: DistrictCodePair) bool {
    return lhs.index < rhs.index;
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

    var bundle = std.ArrayList(u8).init(allocator);

    try bundle.appendSlice(&std.mem.toBytes(@as(u16, @intCast(district_buffer.len))));
    for (district_buffer) |district| {
        try bundle.appendSlice(district.code);
    }

    var units: usize = 0;
    for (sectors) |sector| {
        // ~3200 districts max (u12) + 10 sectors (u4)
        const sector_code: u16 = (sector.district << 12) | sector.code;
        try bundle.appendSlice(&std.mem.toBytes(sector_code));

        try bundle.appendSlice(&std.mem.toBytes(@as(u16, @intCast(sector.units.len))));
        for (sector.units) |unit| {
            const coordinates: u28 = @intCast((unit.lat_index << 14) | unit.lon_index);
            const unit_code: u40 = (@as(u40, unit.code) << 28) | coordinates;
            const unit_bytes: [5]u8 = .{
                @intCast((unit_code >> 32) & 0xFF),
                @intCast((unit_code >> 24) & 0xFF),
                @intCast((unit_code >> 16) & 0xFF),
                @intCast((unit_code >> 8) & 0xFF),
                @intCast(unit_code & 0xFF),
            };
            try bundle.appendSlice(&unit_bytes);
            units += 1;
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
