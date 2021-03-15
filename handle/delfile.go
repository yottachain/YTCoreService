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
	req := &pkt.DeleteBlockReq{VBIS: vibs}
	sn := net.GetSuperNode(int(snid))
	if sn.ID == int32(env.SuperNodeID) {
		handler := &DeleteBlockHandler{pkey: sn.PubKey, m: req}
		handler.Handle()
	} else {
		_, err := net.RequestSN(req, sn, "", 0, false)
		if err != nil {

		}
	}
}

type DeleteBlockHandler struct {
	pkey string
	m    *pkt.DeleteBlockReq
}

func (h *DeleteBlockHandler) SetMessage(pubkey string, msg proto.Message) (*pkt.ErrorMessage, *int32, *int32) {
	h.pkey = pubkey
	req, ok := msg.(*pkt.DeleteBlockReq)
	if ok {
		h.m = req
		if h.m.VBIS == nil || len(h.m.VBIS) == 0 {
			return pkt.NewErrorMsg(pkt.INVALID_ARGS, "Invalid request:Null value"), nil, nil
		}
		return nil, SUMFEE_ROUTINE_NUM, nil
	} else {
		return pkt.NewErrorMsg(pkt.INVALID_ARGS, "Invalid request"), nil, nil
	}
}

func (h *DeleteBlockHandler) Handle() proto.Message {
	_, err := net.AuthSuperNode(h.pkey)
	if err != nil {
		logrus.Errorf("[DeleteBlockHandler]AuthSuper ERR:%s\n", err)
		return pkt.NewErrorMsg(pkt.INVALID_NODE_ID, err.Error())
	}
	return &pkt.LongResp{Value: 0}
}
