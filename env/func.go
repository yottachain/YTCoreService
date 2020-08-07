package env

func BytesToId(bs []byte) int64 {
	vbi := int64(bs[0] & 0xFF)
	vbi = vbi<<8 | int64(bs[1]&0xFF)
	vbi = vbi<<8 | int64(bs[2]&0xFF)
	vbi = vbi<<8 | int64(bs[3]&0xFF)
	vbi = vbi<<8 | int64(bs[4]&0xFF)
	vbi = vbi<<8 | int64(bs[5]&0xFF)
	vbi = vbi<<8 | int64(bs[6]&0xFF)
	vbi = vbi<<8 | int64(bs[7]&0xFF)
	return vbi
}

func IdToBytes(id int64) []byte {
	return []byte{
		uint8(id >> 56),
		uint8(id >> 48),
		uint8(id >> 40),
		uint8(id >> 32),
		uint8(id >> 24),
		uint8(id >> 16),
		uint8(id >> 8),
		uint8(id)}
}

func IsExistInArray(id int32, array []int32) bool {
	for _, arr := range array {
		if id == arr {
			return true
		}
	}
	return false
}
