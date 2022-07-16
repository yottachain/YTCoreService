package codec

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
)

type Parameters struct {
	Width      int32
	Polynomial int64
	ReflectIn  bool
	ReflectOut bool
	Init       int64
	FinalXor   int64
}

var CRC16 Parameters = Parameters{16, 0x8005, true, true, 0x0000, 0x0}

func reflect(in int64, count int32) int64 {
	ret := in
	var idx int32 = 0
	for {
		if idx >= count {
			break
		}
		srcbit := 1 << idx
		dstbit := 1 << (count - idx - 1)
		if (in & int64(srcbit)) != 0 {
			ret |= int64(dstbit)
		} else {
			ret = ret & (^int64(dstbit))
		}
		idx++
	}
	return ret
}

func CheckSumString(data []byte) string {
	bs := CheckSumBytes(data)
	return hex.EncodeToString(bs)
}

func CheckSumBytes(data []byte) []byte {
	crc := CheckSum(data)
	bytebuf := bytes.NewBuffer([]byte{})
	binary.Write(bytebuf, binary.BigEndian, crc)
	return bytebuf.Bytes()
}

func CheckSum(data []byte) int16 {
	curValue := CRC16.Init
	topBit := 1 << (CRC16.Width - 1)
	mask := (topBit << 1) - 1
	num := len(data)
	for i := 0; i < num; i++ {
		curByte := (int64(data[i])) & 0x00FF
		if CRC16.ReflectIn {
			curByte = reflect(curByte, 8)
		}
		for j := 0x80; j != 0; j >>= 1 {
			bit := curValue & int64(topBit)
			curValue <<= 1
			if (curByte & int64(j)) != 0 {
				bit ^= int64(topBit)
			}
			if bit != 0 {
				curValue ^= CRC16.Polynomial
			}
		}
	}
	if CRC16.ReflectOut {
		curValue = reflect(curValue, CRC16.Width)
	}
	curValue = curValue ^ CRC16.FinalXor
	return int16(curValue & int64(mask))
}
