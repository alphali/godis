package core

import "math"

const GEO_STEP_MAX = 26 /* 26*2 = 52 bits. */

/* Limits from EPSG:900913 / EPSG:3785 / OSGEO:41001 */
const GEO_LAT_MIN = -85.05112878
const GEO_LAT_MAX = 85.05112878
const GEO_LONG_MIN = -180
const GEO_LONG_MAX = 180
const D_R = (math.Pi / 180.0)

/// @brief Earth's quatratic mean radius for WGS-84
const EARTH_RADIUS_IN_METERS = 6372797.560856

const MERCATOR_MAX = 20037726.37
const MERCATOR_MIN = -20037726.37

type GeoHashFix52Bits = uint64
type GeoHashVarBits = uint64

type GeoHashBits struct {
	bits uint64
	step uint8
}

type GeoHashRange struct {
	min float64
	max float64
}

type GeoHashArea struct {
	hash      GeoHashBits
	longitude GeoHashRange
	latitude  GeoHashRange
}

type GeoHashRadius struct {
	hash      GeoHashBits
	area      GeoHashArea
	neighbors GeoHashNeighbors
}
type GeoHashNeighbors struct {
	north      GeoHashBits
	east       GeoHashBits
	west       GeoHashBits
	south      GeoHashBits
	north_east GeoHashBits
	south_east GeoHashBits
	north_west GeoHashBits
	south_west GeoHashBits
}

type geoArray struct {
	array   []*geoPoint
	buckets uint
	used    uint
}

type geoPoint struct {
	longitude float64
	latitude  float64
	dist      float64
	score     float64
	member    string
}

func deg_rad(ang float64) float64 {
	return ang * D_R
}
func rad_deg(ang float64) float64 {
	return ang / D_R
}

func geohashEncodeWGS84(longitude float64, latitude float64, step uint8, hash *GeoHashBits) int {
	return geohashEncodeType(longitude, latitude, step, hash)
}

func geohashEncodeType(longitude float64, latitude float64, step uint8, hash *GeoHashBits) int {
	r := [2]GeoHashRange{}
	geohashGetCoordRange(&r[0], &r[1])
	return geohashEncode(&r[0], &r[1], longitude, latitude, step, hash)
}

/* These are constraints from EPSG:900913 / EPSG:3785 / OSGEO:41001 */
/* We can't geocode at the north/south pole. */
func geohashGetCoordRange(long_range *GeoHashRange, lat_range *GeoHashRange) {
	long_range.max = GEO_LONG_MAX
	long_range.min = GEO_LONG_MIN
	lat_range.max = GEO_LAT_MAX
	lat_range.min = GEO_LAT_MIN
}

func geohashEncode(long_range *GeoHashRange, lat_range *GeoHashRange, longitude float64, latitude float64, step uint8,
	hash *GeoHashBits) int {
	/* Check basic arguments sanity. */

	/* Return an error when trying to index outside the supported
	 * constraints. */
	if longitude > 180 || longitude < -180 ||
		latitude > 85.05112878 || latitude < -85.05112878 {
		return 0
	}

	hash.bits = 0
	hash.step = step

	if latitude < lat_range.min || latitude > lat_range.max ||
		longitude < long_range.min || longitude > long_range.max {
		return 0
	}

	var lat_offset float64
	var long_offset float64
	lat_offset =
		(latitude - lat_range.min) / (lat_range.max - lat_range.min)
	long_offset =
		(longitude - long_range.min) / (long_range.max - long_range.min)

	/* convert to fixed point based on the step size */
	mask := 1 << step
	lat_offset = lat_offset * float64(mask)
	long_offset = long_offset * float64(mask)
	hash.bits = interleave64(int32(lat_offset), int32(long_offset))
	return 1
}

/*
lat 放在偶数位，lng放在奇数位
*/
func interleave64(latOffset int32, lngOffset int32) uint64 {
	B := []uint64{0x5555555555555555, 0x3333333333333333,
		0x0F0F0F0F0F0F0F0F, 0x00FF00FF00FF00FF,
		0x0000FFFF0000FFFF}
	S := []uint8{1, 2, 4, 8, 16}
	x := uint64(latOffset)
	y := uint64(lngOffset)
	x = (x | (x << S[4])) & B[4]
	y = (y | (y << S[4])) & B[4]
	x = (x | (x << S[3])) & B[3]
	y = (y | (y << S[3])) & B[3]
	x = (x | (x << S[2])) & B[2]
	y = (y | (y << S[2])) & B[2]
	x = (x | (x << S[1])) & B[1]
	y = (y | (y << S[1])) & B[1]
	x = (x | (x << S[0])) & B[0]
	y = (y | (y << S[0])) & B[0]
	return x | (y << 1)
}

func deinterleave64(interleaved uint64) uint64 {
	B := []uint64{0x5555555555555555, 0x3333333333333333,
		0x0F0F0F0F0F0F0F0F, 0x00FF00FF00FF00FF,
		0x0000FFFF0000FFFF, 0x00000000FFFFFFFF}

	S := []uint8{0, 1, 2, 4, 8, 16}
	x := interleaved
	y := interleaved >> 1
	x = (x | (x >> S[0])) & B[0]
	y = (y | (y >> S[0])) & B[0]

	x = (x | (x >> S[1])) & B[1]
	y = (y | (y >> S[1])) & B[1]

	x = (x | (x >> S[2])) & B[2]
	y = (y | (y >> S[2])) & B[2]

	x = (x | (x >> S[3])) & B[3]
	y = (y | (y >> S[3])) & B[3]

	x = (x | (x >> S[4])) & B[4]
	y = (y | (y >> S[4])) & B[4]

	x = (x | (x >> S[5])) & B[5]
	y = (y | (y >> S[5])) & B[5]

	return x | (y << 32)
}

func geohashAlign52Bits(hash GeoHashBits) uint64 {
	bits := hash.bits
	bits <<= (52 - hash.step*2)
	return bits
}

func decodeGeohash(bits float64, xy *[2]float64) bool {
	hash := GeoHashBits{bits: uint64(bits), step: GEO_STEP_MAX}
	return geohashDecodeToLongLatWGS84(hash, xy)
}
func geohashDecodeToLongLatWGS84(hash GeoHashBits, xy *[2]float64) bool {
	return geohashDecodeToLongLatType(hash, xy)
}

func geohashDecodeToLongLatType(hash GeoHashBits, xy *[2]float64) bool {
	area := new(GeoHashArea)
	if xy == nil || !geohashDecodeType(hash, area) {
		return false
	}
	return geohashDecodeAreaToLongLat(area, xy)
}

func geohashDecodeType(hash GeoHashBits, area *GeoHashArea) bool {
	r := [2]GeoHashRange{}
	geohashGetCoordRange(&r[0], &r[1])
	return geohashDecode(r[0], r[1], hash, area)
}

func geohashDecodeWGS84(hash GeoHashBits, area *GeoHashArea) bool {
	return geohashDecodeType(hash, area)
}

func geohashDecodeAreaToLongLat(area *GeoHashArea, xy *[2]float64) bool {
	if xy == nil {
		return false
	}
	xy[0] = (area.longitude.min + area.longitude.max) / 2
	xy[1] = (area.latitude.min + area.latitude.max) / 2
	return true

}

func hashIsZero(hash GeoHashBits) bool {
	return hash.bits == 0 && hash.step == 0
}

func rangeIsZero(r GeoHashRange) bool {
	return r.max == 0 && r.min == 0
}

func geohashDecode(long_range GeoHashRange, lat_range GeoHashRange, hash GeoHashBits, area *GeoHashArea) bool {
	if hashIsZero(hash) || area == nil || rangeIsZero(lat_range) || rangeIsZero(long_range) {
		return false
	}

	area.hash = hash
	step := hash.step
	hash_sep := deinterleave64(hash.bits)

	lat_scale := lat_range.max - lat_range.min
	long_scale := long_range.max - long_range.min

	ilato := uint32(hash_sep)
	ilono := uint32(hash_sep >> 32)

	area.latitude.min = lat_range.min + (float64(ilato)*1.0/float64(uint64(1)<<step))*lat_scale
	area.latitude.max = lat_range.min + ((float64(ilato)+1)*1.0/float64(uint64(1)<<step))*lat_scale
	area.longitude.min = long_range.min + (float64(ilono)*1.0/float64(uint64(1)<<step))*long_scale
	area.longitude.max = long_range.min + ((float64(ilono)+1)*1.0/float64(uint64(1)<<step))*long_scale

	return true
}

func geohashGetDistance(lon1d float64, lat1d float64, lon2d float64, lat2d float64) float64 {
	var lat1r, lon1r, lat2r, lon2r, u, v float64
	lat1r = deg_rad(lat1d)
	lon1r = deg_rad(lon1d)
	lat2r = deg_rad(lat2d)
	lon2r = deg_rad(lon2d)
	u = math.Sin((lat2r - lat1r) / 2)
	v = math.Sin((lon2r - lon1r) / 2)
	return 2.0 * EARTH_RADIUS_IN_METERS *
		math.Asin(math.Sqrt(u*u+math.Cos(lat1r)*math.Cos(lat2r)*v*v))
}

func geohashGetAreasByRadiusWGS84(longitude float64, latitude float64, radius_meters float64) GeoHashRadius {
	return geohashGetAreasByRadius(longitude, latitude, radius_meters)
}

func geohashGetAreasByRadius(longitude float64, latitude float64, radius_meters float64) GeoHashRadius {
	var long_range, lat_range GeoHashRange
	var radius GeoHashRadius
	var hash GeoHashBits
	var neighbors GeoHashNeighbors
	var area GeoHashArea
	var min_lon, max_lon, min_lat, max_lat float64
	var bounds [4]float64
	var steps int

	geohashBoundingBox(longitude, latitude, radius_meters, &bounds)
	min_lon = bounds[0]
	min_lat = bounds[1]
	max_lon = bounds[2]
	max_lat = bounds[3]

	steps = int(geohashEstimateStepsByRadius(radius_meters, latitude))

	geohashGetCoordRange(&long_range, &lat_range)                                      //获取经纬度范围 南北极无法geocode
	geohashEncode(&long_range, &lat_range, longitude, latitude, (uint8(steps)), &hash) //geohash
	geohashNeighbors(&hash, &neighbors)                                                //计算其余8个框的geohash
	geohashDecode(long_range, lat_range, hash, &area)

	decrease_step := 0
	{
		var north, south, east, west GeoHashArea

		geohashDecode(long_range, lat_range, neighbors.north, &north)
		geohashDecode(long_range, lat_range, neighbors.south, &south)
		geohashDecode(long_range, lat_range, neighbors.east, &east)
		geohashDecode(long_range, lat_range, neighbors.west, &west)

		if geohashGetDistance(longitude, latitude, longitude, north.latitude.max) < radius_meters {
			decrease_step = 1
		}
		if geohashGetDistance(longitude, latitude, longitude, south.latitude.min) < radius_meters {
			decrease_step = 1
		}
		if geohashGetDistance(longitude, latitude, east.longitude.max, latitude) < radius_meters {
			decrease_step = 1
		}
		if geohashGetDistance(longitude, latitude, west.longitude.min, latitude) < radius_meters {
			decrease_step = 1
		}
	}
	if steps > 1 && decrease_step > 0 {
		steps--
		geohashEncode(&long_range, &lat_range, longitude, latitude, uint8(steps), &hash)
		geohashNeighbors(&hash, &neighbors)
		geohashDecode(long_range, lat_range, hash, &area)
	}
	/* Exclude the search areas that are useless. */
	if steps >= 2 {
		if area.latitude.min < min_lat {
			GZERO(&neighbors.south)
			GZERO(&neighbors.south_west)
			GZERO(&neighbors.south_east)
		}
		if area.latitude.max > max_lat {
			GZERO(&neighbors.north)
			GZERO(&neighbors.north_east)
			GZERO(&neighbors.north_west)
		}
		if area.longitude.min < min_lon {
			GZERO(&neighbors.west)
			GZERO(&neighbors.south_west)
			GZERO(&neighbors.north_west)
		}
		if area.longitude.max > max_lon {
			GZERO(&neighbors.east)
			GZERO(&neighbors.south_east)
			GZERO(&neighbors.north_east)
		}
	}
	radius.hash = hash
	radius.neighbors = neighbors
	radius.area = area
	return radius
}
func GZERO(s *GeoHashBits) {
	s.bits = 0
	s.step = 0
}

//计算经度、纬度为中心的搜索区域的边界框
func geohashBoundingBox(longitude float64, latitude float64, radius_meters float64, bounds *[4]float64) bool {
	if bounds == nil {
		return false
	}
	bounds[0] = longitude - rad_deg(radius_meters/EARTH_RADIUS_IN_METERS/math.Cos(deg_rad(latitude)))
	bounds[2] = longitude + rad_deg(radius_meters/EARTH_RADIUS_IN_METERS/math.Cos(deg_rad(latitude)))
	bounds[1] = latitude - rad_deg(radius_meters/EARTH_RADIUS_IN_METERS)
	bounds[3] = latitude + rad_deg(radius_meters/EARTH_RADIUS_IN_METERS)
	return true
}

//计算bits 位的精度
func geohashEstimateStepsByRadius(range_meters float64, lat float64) uint8 {
	if range_meters == 0 {
		return 26
	}
	step := uint8(1)
	for range_meters < MERCATOR_MAX {
		range_meters *= 2
		step++
	}

	step -= 2
	if lat > 66 || lat < -66 {
		step--
		if lat > 80 || lat < -80 {
			step--
		}
	}

	/* Frame to valid range. */
	if step < 1 {
		step = 1
	}
	if step > 26 {
		step = 26
	}
	return step
}

//计算其余8个框的geohash
func geohashNeighbors(hash *GeoHashBits, neighbors *GeoHashNeighbors) {
	neighbors.east = *hash
	neighbors.west = *hash
	neighbors.north = *hash
	neighbors.south = *hash
	neighbors.south_east = *hash
	neighbors.south_west = *hash
	neighbors.north_east = *hash
	neighbors.north_west = *hash //8个方位的hash赋值

	Geohash_move_x(&neighbors.east, 1)
	Geohash_move_y(&neighbors.east, 0)

	Geohash_move_x(&neighbors.west, -1)
	Geohash_move_y(&neighbors.west, 0)

	Geohash_move_x(&neighbors.south, 0)
	Geohash_move_y(&neighbors.south, -1)

	Geohash_move_x(&neighbors.north, 0)
	Geohash_move_y(&neighbors.north, 1)

	Geohash_move_x(&neighbors.north_west, -1)
	Geohash_move_y(&neighbors.north_west, 1)

	Geohash_move_x(&neighbors.north_east, 1)
	Geohash_move_y(&neighbors.north_east, 1)

	Geohash_move_x(&neighbors.south_east, 1)
	Geohash_move_y(&neighbors.south_east, -1)

	Geohash_move_x(&neighbors.south_west, -1)
	Geohash_move_y(&neighbors.south_west, -1)
}

func Geohash_move_x(hash *GeoHashBits, d int8) {
	if d == 0 {
		return
	}

	x := hash.bits & 0xaaaaaaaaaaaaaaaa
	y := hash.bits & 0x5555555555555555

	zz := uint64(0x5555555555555555 >> (64 - hash.step*2))
	if d > 0 {
		x = x + (zz + 1)
	} else {
		x = x | zz
		x = x - (zz + 1)
	}
	x &= (0xaaaaaaaaaaaaaaaa >> (64 - hash.step*2))
	hash.bits = (x | y)
}

func Geohash_move_y(hash *GeoHashBits, d int8) {
	if d == 0 {
		return
	}

	x := hash.bits & 0xaaaaaaaaaaaaaaaa
	y := hash.bits & 0x5555555555555555

	zz := uint64(0xaaaaaaaaaaaaaaaa >> (64 - hash.step*2))
	if d > 0 {
		y = y + (zz + 1)
	} else {
		y = y | zz
		y = y - (zz + 1)
	}
	y &= (0x5555555555555555 >> (64 - hash.step*2))
	hash.bits = (x | y)
}

func geohashGetDistanceIfInRadius(x1 float64, y1 float64, x2 float64, y2 float64, radius float64, distance *float64) bool {
	*distance = geohashGetDistance(x1, y1, x2, y2)
	if *distance > radius {
		return false
	}
	return true
}

func geohashGetDistanceIfInRadiusWGS84(x1 float64, y1 float64, x2 float64, y2 float64, radius float64, distance *float64) bool {
	return geohashGetDistanceIfInRadius(x1, y1, x2, y2, radius, distance)
}
