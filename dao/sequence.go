package dao

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/yottachain/YTCoreService/env"
	"github.com/yottachain/YTCoreService/net"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

var USERID_SEQ *uint32 = new(uint32)

var BLKID_SEQ *int32 = new(int32)

func InitUserID_seq() {
	source := NewBaseSource()
	filter := bson.M{"_id": bson.M{"$mod": []interface{}{net.GetSuperNodeCount(), env.SuperNodeID}}}
	var result = struct {
		ID uint32 `bson:"_id"`
	}{}
	opt := options.FindOne().SetProjection(bson.M{"_id": 1}).SetSort(bson.M{"_id": -1})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err := source.GetUserColl().FindOne(ctx, filter, opt).Decode(&result)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			atomic.StoreUint32(USERID_SEQ, uint32(env.SuperNodeID))
			env.Log.Infof("User sequence init value:%d\n", env.SuperNodeID)
		} else {
			env.Log.Panicf("InitUserID_seq err:%s\n", err)
		}
	}
	atomic.StoreUint32(USERID_SEQ, result.ID)
	env.Log.Infof("User sequence init value:%d\n", result.ID)
	atomic.StoreInt32(BLKID_SEQ, 0)
}

func GenerateUserID() uint32 {
	return atomic.AddUint32(USERID_SEQ, uint32(net.GetSuperNodeCount()))
}

func GetSequence(inc int) int32 {
	id := atomic.AddInt32(BLKID_SEQ, int32(inc))
	bs := make([]byte, 4)
	if env.IsBackup == 0 {
		bs[0] = uint8(env.SuperNodeID)
	} else {
		bs[0] = uint8(env.SuperNodeID + net.GetSuperNodeCount())
	}
	bs[1] = uint8(id >> 16)
	bs[2] = uint8(id >> 8)
	bs[3] = uint8(id)
	vbi := int32(bs[0] & 0xFF)
	vbi = vbi<<8 | int32(bs[1]&0xFF)
	vbi = vbi<<8 | int32(bs[2]&0xFF)
	vbi = vbi<<8 | int32(bs[3]&0xFF)
	return vbi
}

func GenerateZeroID(timestamp int64) int64 {
	high := (time.Now().Unix() & 0x000000ffffffff) << 32
	low := int64(0) & 0x00000000ffffffff
	return high | low
}

func GenerateBlockID(shardCount int) int64 {
	h := time.Now().Unix()
	l := int64(GetSequence(shardCount) - int32(shardCount))
	high := (h & 0x000000ffffffff) << 32
	low := l & 0x00000000ffffffff
	return high | low
}
