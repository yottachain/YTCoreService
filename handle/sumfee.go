package handle

import (
	"sync"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/yottachain/YTCoreService/dao"
	"github.com/yottachain/YTCoreService/net"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

func IterateUser() {
	lastId := 0
	limit := 1000
	for {
		if !net.IsActive() {
			time.Sleep(time.Duration(30) * time.Second)
			continue
		}
		_, err := dao.ListUsers(lastId, limit, bson.M{"_id": 1, "nextCycle": 1, "username": 1})
		if err != nil {

		}
	}
}

var StartVNU = primitive.NilObjectID

const BLKID_LIMIT = 1000

type UserObjectSum struct {
	sync.RWMutex
	UserID    int32
	FirstVNU  primitive.ObjectID
	UsedSpace int64
}

func (me *UserObjectSum) IterateObjects() {
	logrus.Infof("Start sum fee,UserID:%d\n", me.UserID)
	limit := net.GetSuperNodeCount() * BLKID_LIMIT
	firstId := StartVNU
	m := make(map[int32][]int64)
	for {
		ls, id, err := dao.ListObjects(uint32(me.UserID), firstId, GetStopVNU(), limit)
		if err != nil {
			time.Sleep(time.Duration(30) * time.Second)
			continue
		}
		for _, bs := range ls {
			supid := int32(bs[8])
			vbi := GetVBI(bs)
			ids, ok := m[supid]
			if ok {
				if len(ids) >= BLKID_LIMIT {
					//fasong
					m[supid] = []int64{vbi}
				} else {
					m[supid] = append(ids, vbi)
				}
			} else {
				m[supid] = []int64{vbi}
			}
		}
		firstId = id
		if firstId == primitive.NilObjectID {
			break
		}
	}
	//
}

func GetStopVNU() primitive.ObjectID {
	t := time.Now().Add(time.Duration(-90) * time.Hour * 24)
	return primitive.NewObjectIDFromTimestamp(t)
}

func GetVBI(bs []byte) int64 {
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
