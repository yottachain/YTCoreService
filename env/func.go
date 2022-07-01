package env

import (
	"encoding/binary"
	"math/rand"
	"net"
	"runtime"
	"strings"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/yottachain/YTHost/client"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

func ZeroLenFileID() primitive.ObjectID {
	id := primitive.NewObjectID()
	id[4] = 0
	id[5] = 0
	id[6] = 0
	id[7] = 0
	id[8] = 0
	id[9] = 0
	id[10] = 0
	id[11] = 0
	return id
}

func IsZeroLenFileID(id primitive.ObjectID) bool {
	for ii := 4; ii < 12; ii++ {
		if id[ii] != 0 {
			return false
		}
	}
	return true
}

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

func BytesToInt32(bs []byte) int32 {
	vbi := int32(bs[0] & 0xFF)
	vbi = vbi<<8 | int32(bs[1]&0xFF)
	vbi = vbi<<8 | int32(bs[2]&0xFF)
	vbi = vbi<<8 | int32(bs[3]&0xFF)
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

func Int32ToBytes(id int32) []byte {
	return []byte{
		uint8(id >> 24),
		uint8(id >> 16),
		uint8(id >> 8),
		uint8(id)}
}

func IsExistInArray(id int32, array []int32) bool {
	if array == nil {
		return false
	}
	for _, arr := range array {
		if id == arr {
			return true
		}
	}
	return false
}

func TracePanic(prefix string) {
	if r := recover(); r != nil {
		TraceError(prefix)
	}
}

func P2PConfig(config *Config) {
	client.GlobalClientOption.ConnectTimeout = config.GetRangeInt("P2PHOST_CONNECTTIMEOUT", 1000, 60000, 5000)
	client.GlobalClientOption.QueueSize = config.GetRangeInt("P2PHOST_QUEUESIZE", 1, 10, 1)
	client.GlobalClientOption.QueueTimeout = config.GetRangeInt("P2PHOST_QUEUETIMEOUT", 1000, 60000, 3000)
	client.GlobalClientOption.WriteTimeout = config.GetRangeInt("P2PHOST_WRITETIMEOUT", 1000, 60000, 7000)
	client.GlobalClientOption.ReadTimeout = config.GetRangeInt("P2PHOST_READTIMEOUT", 1000, 180000, 20000)
	client.GlobalClientOption.IdleTimeout = config.GetRangeInt("P2PHOST_IDLETIMEOUT", 60000, 3600000, 180000)
	client.GlobalClientOption.MuteTimeout = config.GetRangeInt("P2PHOST_MUTETIMEOUT", client.GlobalClientOption.WriteTimeout, client.GlobalClientOption.IdleTimeout, client.GlobalClientOption.WriteTimeout*3)
}

func TraceError(prefix string) {
	stack := make([]byte, 2048)
	length := runtime.Stack(stack, true)
	ss := string(stack[0:length])
	ls := strings.Split(ss, "\n")
	for _, s := range ls {
		logrus.Error(prefix + s + "\n")
	}
}

func TraceErrors(prefix string) string {
	stack := make([]byte, 2048)
	length := runtime.Stack(stack, true)
	ss := string(stack[0:length])
	ls := strings.Split(ss, "\n")
	for _, s := range ls {
		logrus.Error(prefix + s + "\n")
	}
	return ss
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

func GetFreePort() (int, error) {
	addr, err := net.ResolveTCPAddr("tcp", "localhost:0")
	if err != nil {
		return 0, err
	}
	l, err := net.ListenTCP("tcp", addr)
	if err != nil {
		return 0, err
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port, nil
}
