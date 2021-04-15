package handle

import (
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/mr-tron/base58"
	"github.com/sirupsen/logrus"
	"github.com/yottachain/YTCoreService/dao"
	"github.com/yottachain/YTCoreService/env"
	"github.com/yottachain/YTCoreService/net"
	"github.com/yottachain/YTCoreService/pkt"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

var DEL_BLK_CH chan int

func InitDELPool() {
	DEL_BLK_CH = make(chan int, env.MAX_DELBLK_ROUTINE)
	for ii := 0; ii < int(env.MAX_DELBLK_ROUTINE); ii++ {
		DEL_BLK_CH <- 1
	}
}

func StartDoDelete() {
	InitDELPool()
	time.Sleep(time.Duration(10) * time.Second)
	for {
		IterateDELLog()
		time.Sleep(time.Duration(10) * time.Second)
	}
}

func IterateDELLog() {
	for {
		log := dao.FindOneDelLOG()
		if log == nil {
			return
		}
		DelBlocks(log.UID, log.VNU, true)
	}
}

const VBI_COUNT_LIMIT = 10

func DelBlocks(uid int32, vnu primitive.ObjectID, decSpace bool) {
	for {
		meta, err := dao.DelOrUpObject(uid, vnu, decSpace)
		if err != nil {
			time.Sleep(time.Duration(30) * time.Second)
			continue
		} else {
			if meta != nil {
				vbigroup := make(map[int32][]int64)
				for _, refbs := range meta.BlockList {
					refer := pkt.NewRefer(refbs)
					if refer == nil {
						logrus.Errorf("[DeleteOBJ][%d]Refer data err\n", uid)
						continue
					}
					ids := vbigroup[int32(refer.SuperID)]
					ids = append(ids, refer.VBI)
					if len(ids) >= VBI_COUNT_LIMIT {
						delete(vbigroup, int32(refer.SuperID))
						<-DEL_BLK_CH
						go deleteBlocks(int32(refer.SuperID), ids)
					} else {
						vbigroup[int32(refer.SuperID)] = ids
					}
				}
				for K, V := range vbigroup {
					if len(V) > 0 {
						<-DEL_BLK_CH
						go deleteBlocks(K, V)
					}
				}
			}
			break
		}
	}
}

func deleteBlocks(snid int32, vibs []int64) {
	defer func() { DEL_BLK_CH <- 1 }()
	req := &pkt.DeleteBlockReq{VBIS: vibs}
	sn := net.GetSuperNode(int(snid))
	var errmsg *pkt.ErrorMessage = nil
	if sn.ID == int32(env.SuperNodeID) {
		handler := &DeleteBlockHandler{pkey: sn.PubKey, m: req}
		msg := handler.Handle()
		if err, ok := msg.(*pkt.ErrorMessage); ok {
			errmsg = err
		}
	} else {
		_, err := net.RequestSN(req, sn, "", 0, false)
		if err != nil {
			errmsg = err
		}
	}
	if errmsg != nil {
		logrus.Errorf("[DeleteOBJ][%d]Delete blocks err:%s\n", pkt.ToError(errmsg))
		time.Sleep(time.Duration(60*3) * time.Second)
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
		return nil, DELBLK_ROUTINE_NUM, nil
	} else {
		return pkt.NewErrorMsg(pkt.INVALID_ARGS, "Invalid request"), nil, nil
	}
}

func (h *DeleteBlockHandler) WriteLOG(shds []*dao.ShardMeta) error {
	if shds != nil {
		for _, shd := range shds {
			log, err := GetNodeLog(shd.NodeId)
			if err != nil {
				logrus.Errorf("[DeleteBlock]GetNodeLog ERR:%s\n", err)
				return err
			}
			err = log.WriteLog(base58.Encode(shd.VHF))
			if err != nil {
				logrus.Errorf("[DeleteBlock]WriteLog %d ERR:%s\n", shd.NodeId, err)
				return err
			}
		}
	}
	return nil
}

func (h *DeleteBlockHandler) Handle() proto.Message {
	_, err := net.AuthSuperNode(h.pkey)
	if err != nil {
		logrus.Errorf("[DeleteBlock]AuthSuper ERR:%s\n", err)
		return pkt.NewErrorMsg(pkt.INVALID_NODE_ID, err.Error())
	}
	var delerr error = nil
	for _, vbi := range h.m.VBIS {
		shds, er := dao.DelOrUpBLK(vbi)
		if er != nil {
			delerr = er
		} else {
			er = h.WriteLOG(shds)
			if er != nil {
				delerr = er
			}
		}
	}
	if delerr == nil {
		return &pkt.VoidResp{}
	} else {
		return pkt.NewErrorMsg(pkt.SERVER_ERROR, delerr.Error())
	}
}