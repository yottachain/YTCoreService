package env

import (
	"encoding/binary"
	"math/rand"
	"runtime"
	"strings"
	"time"

	"github.com/sirupsen/logrus"
)

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

func TracePanic() {
	if r := recover(); r != nil {
		TraceError()
	}
}

func TraceError() {
	stack := make([]byte, 4096)
	length := runtime.Stack(stack, true)
	ss := string(stack[0:length])
	ls := strings.Split(ss, "\n")
	for _, s := range ls {
		logrus.Error("[Unkown]" + s + "\n")
	}
}

func MakeRandData(size int64) []byte {
	rand.Seed(time.Now().UnixNano())
	loop := size / 8
	buf := make([]byte, loop*8)
	for ii := int64(0); ii < loop; ii++ {
		binary.BigEndian.PutUint64(buf[ii*8:(ii+1)*8], rand.Uint64())
	}
	return buf
}
