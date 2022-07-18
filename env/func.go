package env

import (
	"crypto/md5"
	"math/big"
	"net"

	"github.com/aurawing/eos-go/btcsuite/btcutil/base58"
	mnet "github.com/multiformats/go-multiaddr-net"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

func NetID() (string, error) {
	md5Digest := md5.New()
	maddrs, err := mnet.InterfaceMultiaddrs()
	if err != nil {
		return "", err
	}
	for _, ma := range maddrs {
		if mnet.IsIPLoopback(ma) {
			continue
		}
		md5Digest.Write(ma.Bytes())
	}
	id := md5Digest.Sum(nil)
	return base58.Encode(id), nil
}

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

func CalCycleFee(usedpace int64) uint64 {
	uspace := big.NewInt(usedpace)
	unitCycleCost := big.NewInt(int64(UnitCycleCost))
	unitSpace := big.NewInt(int64(UnitSpace))
	bigcost := big.NewInt(0)
	bigcost = bigcost.Mul(uspace, unitCycleCost)
	bigcost = bigcost.Div(bigcost, unitSpace)
	return uint64(bigcost.Int64())
}

func CalFirstFee(usedpace int64) uint64 {
	uspace := big.NewInt(usedpace)
	unitFirstCost := big.NewInt(int64(UnitFirstCost))
	unitSpace := big.NewInt(int64(UnitSpace))
	bigcost := big.NewInt(0)
	bigcost = bigcost.Mul(uspace, unitFirstCost)
	bigcost = bigcost.Div(bigcost, unitSpace)
	return uint64(bigcost.Int64())
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

func Int32ToBytes(id int32) []byte {
	return []byte{
		uint8(id >> 24),
		uint8(id >> 16),
		uint8(id >> 8),
		uint8(id)}
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
