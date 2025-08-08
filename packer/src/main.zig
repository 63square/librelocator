const DistrictCode = u16;
const District = struct { index: DistrictCode, sectors: [10]Sector };

const Sector = ?[]Unit;

const Unit = struct {
    code: u16,
    longitude: f16,
    latitude: f16,
};

const Postcode = struct {
    district: DistrictCode,
    sector: u8,
    unit: u16,
};

const PostcodeLine = struct {
    postcode: Postcode,
    longitude: f16,
    latitude: f16,
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
        .latitude = try std.fmt.parseFloat(f16, latitude_slice),
        .longitude = try std.fmt.parseFloat(f16, longitude_slice),
        .lineLength = i + 1,
    };
}

/// Everything owned by caller, make sure to clean up
fn parseToDistricts(allocator: std.mem.Allocator, district_map: *DistrictCodeMap, csv_contents: []const u8) ![]District {
    var bytesRemaining = csv_contents.len;
    var index: usize = 0;
    var lines: usize = 0;
    defer std.debug.print("[*] Parsed {d} postcodes\n", .{lines});

    var districts = std.ArrayList(District).init(allocator);

    var lastDistrict: DistrictCode = 0;
    var lastSector: u8 = 0;

    var district: District = undefined;
    var sector: std.ArrayList(Unit) = std.ArrayList(Unit).init(allocator);
    defer sector.deinit();

    while (true) {
        const line = parseLine(district_map, csv_contents[index..].ptr, bytesRemaining) catch |err| switch (err) {
            error.EOF => {
                district.sectors[lastSector] = try allocator.alloc(Unit, sector.items.len);
                @memcpy(district.sectors[lastSector].?, sector.items);
                try districts.append(district);
                break;
            },
            else => return err,
        };

        if (line.postcode.district == lastDistrict) {
            if (line.postcode.sector != lastSector) {
                district.sectors[lastSector] = try allocator.alloc(Unit, sector.items.len);
                @memcpy(district.sectors[lastSector].?, sector.items);
                sector.clearRetainingCapacity();
            }
        } else {
            if (sector.items.len > 1) {
                district.sectors[lastSector] = try allocator.alloc(Unit, sector.items.len);
                @memcpy(district.sectors[lastSector].?, sector.items);
                sector.clearRetainingCapacity();
            } else {
                district.sectors[lastSector] = null;
            }

            try districts.append(district);

            district = District{
                .index = line.postcode.district,
                .sectors = undefined,
            };
            @memset(&district.sectors, null);
        }

        try sector.append(Unit{
            .code = line.postcode.unit,
            .latitude = line.latitude,
            .longitude = line.longitude,
        });

        lastSector = line.postcode.sector;
        lastDistrict = line.postcode.district;

        bytesRemaining -|= line.lineLength;
        index += line.lineLength;
        lines += 1;
    }

    return try districts.toOwnedSlice();
}

const DistrictCodePair = struct {
    code: []const u8,
    index: DistrictCode,
};

fn sortDistrictCodesFn(_: usize, lhs: DistrictCodePair, rhs: DistrictCodePair) bool {
    return lhs.index < rhs.index;
}

fn createBundle(allocator: std.mem.Allocator, district_map: DistrictCodeMap, districts: []const District) !std.ArrayList(u8) {
    var mapIterator = district_map.iterator();

    const district_buffer = try allocator.alloc(DistrictCodePair, district_map.count());
    defer allocator.free(district_buffer);

    var idx: usize = 0;
    while (mapIterator.next()) |entry| : (idx += 1) {
        district_buffer[idx] = DistrictCodePair{
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

    for (districts) |district| {
        try bundle.appendSlice(&std.mem.toBytes(district.index));
        for (district.sectors) |sector_n| {
            if (sector_n) |sector| {
                try bundle.appendSlice(&std.mem.toBytes(@as(u16, @intCast(sector.len))));
                for (sector) |unit| {
                    try bundle.appendSlice(&std.mem.toBytes(unit.code));
                    try bundle.appendSlice(&std.mem.toBytes(unit.latitude));
                    try bundle.appendSlice(&std.mem.toBytes(unit.longitude));
                }
            } else {
                try bundle.appendSlice("\x00\x00");
            }
        }
    }

    return bundle;
}

fn deinitDistricts(allocator: std.mem.Allocator, districts: []const District) void {
    for (districts) |district| {
        for (district.sectors) |sector| {
            if (sector) |s| {
                allocator.free(s);
            }
        }
    }
    allocator.free(districts);
}

pub fn main() !void {
    const file_path = "ONSPD_MAY_2025_UK.csv";

    const allocator = std.heap.smp_allocator;

    std.debug.print("[*] Started\n", .{});
    defer std.debug.print("[*] Completed tasks\n", .{});

    // read entire file, 2 GiB allocation limit
    const file_data = try std.fs.cwd().readFileAlloc(allocator, file_path, 2 * 1024 * 1024 * 1024);
    defer allocator.free(file_data);

    std.debug.print("[*] Loaded CSV\n", .{});

    var districts_map = DistrictCodeMap.init(allocator);
    defer districts_map.deinit();

    const districts = try parseToDistricts(allocator, &districts_map, file_data);
    defer deinitDistricts(allocator, districts);

    const bundle = try createBundle(allocator, districts_map, districts);
    defer bundle.deinit();

    std.debug.print("[*] Created bundle\n", .{});

    const stdout = std.io.getStdOut();
    try stdout.writeAll(bundle.items);
}

const std = @import("std");
