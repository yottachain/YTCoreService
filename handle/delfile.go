package handle

import (
	"sync"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/sirupsen/logrus"
	"github.com/yottachain/YTCoreService/dao"
	"github.com/yottachain/YTCoreService/env"
	"github.com/yottachain/YTCoreService/net"
	"github.com/yottachain/YTCoreService/pkt"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

func StartGC() {
	time.Sleep(time.Duration((env.SuperNodeID+1)*30) * time.Minute)
	for {
		if !net.IsActive() {
			time.Sleep(time.Duration(30) * time.Second)
			continue
		}
		time.Sleep(time.Duration(180) * time.Second)
		ListUser()
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
	time.Sleep(time.Duration(24) * time.Hour)
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
			DelObject(uid, vnu)
			firstId = vnu
		}
		if firstId == primitive.NilObjectID {
			break
		}
	}
}

func DelObject(uid int32, vnu primitive.ObjectID) {
	for {
		_, err := dao.DelObject(uid, vnu)
		if err != nil {
			time.Sleep(time.Duration(30) * time.Second)
		}
		//
	}

}

func (h *AuthHandler) DeleteBlock(snid int32, vibs []int64, usedSpace *int64, wg *sync.WaitGroup) {
	defer wg.Done()
	req := &pkt.AuthBlockLinkReq{VBIS: vibs}
	var longmsg proto.Message
	sn := net.GetSuperNode(int(snid))
	if sn.ID == int32(env.SuperNodeID) {
		handler := &AuthBlockLinkHandler{pkey: sn.PubKey, m: req}
		msg := handler.Handle()
		if _, ok := msg.(*pkt.ErrorMessage); ok {
			*usedSpace = -1
			return
		} else {
			longmsg = msg
		}
	} else {
		msg, err := net.RequestSN(req, sn, "", 0, false)
		if err != nil {
			*usedSpace = -1
			return
		} else {
			longmsg = msg
		}
	}
	if resp, ok := longmsg.(*pkt.LongResp); ok {
		*usedSpace = resp.Value
	} else {
		*usedSpace = -1
	}

}
