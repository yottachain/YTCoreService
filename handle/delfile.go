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
	DEL_BLK_CH = make(chan int, env.MAX_DELBLK_ROUTINE/3)
	for ii := 0; ii < int(env.MAX_DELBLK_ROUTINE/3); ii++ {
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
		DelBlocks(log.UID, log.VNU, true, false)
	}
}

const VBI_COUNT_LIMIT = 10

func DelBlocks(uid int32, vnu primitive.ObjectID, decSpace bool, del bool) {
	for {
		meta, err := dao.DelOrUpObject(uid, vnu, decSpace, del)
		if err != nil {
			time.Sleep(time.Duration(30) * time.Second)
			continue
		} else {
			if meta != nil {
				logrus.Infof("[DeleteOBJ][%d]Deleting object %s,block count %d...\n", uid, vnu.Hex(), len(meta.BlockList))
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
	startTime := time.Now()
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
		logrus.Errorf("[DeleteOBJ][%d]Delete blocks err:%s\n", snid, pkt.ToError(errmsg))
		time.Sleep(time.Duration(90) * time.Second)
	} else {
		logrus.Infof("[DeleteOBJ][%d]Delete %d blocks,take times %d ms\n", snid, len(vibs), time.Now().Sub(startTime).Milliseconds())
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
			if shd.NodeId2 > 0 {
				log, err = GetNodeLog(shd.NodeId2)
				if err != nil {
					logrus.Errorf("[DeleteBlock]GetNodeLog ERR:%s\n", err)
					return err
				}
				err = log.WriteLog(base58.Encode(shd.VHF))
				if err != nil {
					logrus.Errorf("[DeleteBlock]WriteLog %d ERR:%s\n", shd.NodeId2, err)
					return err
				}
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
	var dbtime, alltime int64
	var delerr error = nil
	for _, vbi := range h.m.VBIS {
		startTime := time.Now()
		shds, er := dao.DelOrUpBLK(vbi)
		if er != nil {
			delerr = er
		} else {
			dbtime = dbtime + time.Now().Sub(startTime).Milliseconds()
			er = h.WriteLOG(shds)
			if er != nil {
				delerr = er
			}
			alltime = alltime + time.Now().Sub(startTime).Milliseconds()
		}
	}
	logrus.Infof("[DeleteBlock]Delete %d blocks,take times %d/%d ms\n", len(h.m.VBIS), dbtime, alltime)
	if delerr == nil {
		return &pkt.VoidResp{}
	} else {
		return pkt.NewErrorMsg(pkt.SERVER_ERROR, delerr.Error())
	}
}
