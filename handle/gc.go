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
	time.Sleep(time.Duration(1 * time.Minute))
	for {
		if !net.IsActive() {
			time.Sleep(time.Duration(30) * time.Second)
			continue
		}
		ListUser()
		time.Sleep(time.Duration(60) * time.Second)
	}
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
	time.Sleep(time.Duration(2) * time.Hour)
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
			DelBlocks(uid, vnu, false)
			firstId = vnu
		}
		if firstId == primitive.NilObjectID {
			break
		}
	}
}
