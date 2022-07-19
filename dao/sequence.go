package dao

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/yottachain/YTCoreService/env"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

var USERID_SEQ *uint32 = new(uint32)
var BLKID_SEQ *int32 = new(int32)
var SHDID_SEQ *int32 = new(int32)
var SNID int = 0

func initSequence() {
	for {
		err := init_SNID()
		if err == nil {
			logrus.Infof("[InitSequence]Init SNID:%d\n", SNID)
			break
		} else {
			logrus.Infof("[InitSequence]Init SNID ERR:%s\n", err)
			time.Sleep(time.Second * 5)
		}
	}
	init_SEQ()
}

func init_SNID() error {
	snid, err := env.NetID()
	if err != nil {
		logrus.Panicf("[InitSequence]NetID Err:%s\n", err)
	}
	source := NewBaseSource()
	var result = struct {
		ID   string `bson:"_id"`
		SNID int    `bson:"snid"`
	}{}
	err = source.GetSuperNodesColl().FindOne(context.Background(), bson.M{"_id": snid}).Decode(&result)
	if err != nil {
		if err != mongo.ErrNoDocuments {
			return err
		}
	} else {
		SNID = result.SNID
		return nil
	}
	newid := 1
	opt := options.Find().SetSort(bson.M{"snid": 1})
	cur, err := source.GetSuperNodesColl().Find(context.Background(), bson.M{}, opt)
	defer func() {
		if cur != nil {
			cur.Close(context.Background())
		}
	}()
	if err != nil {
		return err
	}
	for cur.Next(context.Background()) {
		err = cur.Decode(&result)
		if err != nil {
			return err
		}
		if newid != result.SNID {
			break
		} else {
			newid++
		}
	}
	result.ID = snid
	result.SNID = newid
	_, err = source.GetSuperNodesColl().InsertOne(context.Background(), result)
	if err != nil {
		return err
	} else {
		SNID = result.SNID
		return nil
	}
}

func init_SEQ() {
	source := NewBaseSource()
	var result = struct {
		ID uint32 `bson:"_id"`
	}{}
	opt := options.FindOne().SetProjection(bson.M{"_id": 1}).SetSort(bson.M{"_id": -1})
	err := source.GetUserColl().FindOne(context.Background(), bson.M{}, opt).Decode(&result)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			atomic.StoreUint32(USERID_SEQ, 0)
			logrus.Infof("[InitSequence]User sequence init value:%d\n", 0)
		} else {
			logrus.Panicf("[InitSequence]Err:%s\n", err)
		}
	} else {
		atomic.StoreUint32(USERID_SEQ, result.ID)
		logrus.Infof("[InitSequence]User sequence init value:%d\n", result.ID)
	}
	atomic.StoreInt32(BLKID_SEQ, 0)
	atomic.StoreInt32(SHDID_SEQ, 0)
}

func sequence(seq *int32, inc int) int32 {
	id := atomic.AddInt32(seq, int32(inc))
	h := int32(SNID)
	high := (h & 0x00ffffff) << 24
	low := id & 0x00ffffff
	return high | low
}

func GenerateUserID() uint32 {
	return atomic.AddUint32(USERID_SEQ, 1)
}

func GenerateShardID(shardCount int) int64 {
	h := time.Now().Unix()
	l := int64(sequence(SHDID_SEQ, shardCount) - int32(shardCount))
	high := (h & 0x00000000ffffffff) << 32
	low := l & 0x00000000ffffffff
	return high | low
}

func GenerateBlockID(shardCount int) int64 {
	h := time.Now().Unix()
	l := int64(sequence(BLKID_SEQ, shardCount) - int32(shardCount))
	high := (h & 0x00000000ffffffff) << 32
	low := l & 0x00000000ffffffff
	return high | low
}

func GenerateZeroID(timestamp int64) int64 {
	return (timestamp & 0x00000000ffffffff) << 32
}
