package handle

import (
	"time"

	"github.com/sirupsen/logrus"
	"github.com/yottachain/YTCoreService/dao"
	"github.com/yottachain/YTCoreService/env"
	"github.com/yottachain/YTCoreService/net"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

func StartGC() {
	if !env.GC {
		return
	}
	time.Sleep(time.Duration(10 * time.Minute))
	if !net.IsActive() {
		time.Sleep(time.Duration(30) * time.Second)
		return
	}
	ListUser()
}

func ListUser() {
	defer env.TracePanic("[DelUsedSpace]")
	var lastId int32 = 0
	limit := 100
	logrus.Infof("[DelUsedSpace]Start iterate user...\n")
	for {
		us, err := dao.ListUsers(lastId, limit, bson.M{"_id": 1})
		if err != nil {
			time.Sleep(time.Duration(30) * time.Second)
			continue
		}
		if len(us) == 0 {
			break
		} else {
			for _, user := range us {
				lastId = user.UserID
				IterateObjects(user.UserID)
			}
		}
	}
	logrus.Infof("[DelUsedSpace]Iterate user OK!\n")
}

func IterateObjects(uid int32) {
	logrus.Infof("[DelUsedSpace]Start ls objects,UserID:%d\n", uid)
	firstId := primitive.NilObjectID
	for {
		vnus, err := dao.ListObjectsForDel(uint32(uid), firstId, 1000)
		if err != nil {
			time.Sleep(time.Duration(30) * time.Second)
			continue
		}
		for _, vnu := range vnus {
			if time.Now().Unix()-vnu.Timestamp().Unix() >= 60*5 {
				_, found := Upload_CACHE.Get(vnu.Hex())
				if !found {
					DelBlocks(uid, vnu, false)
				}
			}
			firstId = vnu
		}
		if firstId == primitive.NilObjectID {
			break
		}
	}
}