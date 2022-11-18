package handle

import (
	"context"
	"fmt"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/yottachain/YTCoreService/dao"
	"github.com/yottachain/YTCoreService/env"
	"github.com/yottachain/YTCoreService/pkt"
	ytrebuilder "github.com/yottachain/yotta-rebuilder"
	"github.com/yottachain/yotta-rebuilder/pbrebuilder"
	"google.golang.org/protobuf/proto"
)

var REBUILDER_SERVICE *ytrebuilder.RebuilderClient

func initRebuildService() {
	if env.REBUILD_ADDR != "" {
		var err error
		REBUILDER_SERVICE, err = ytrebuilder.NewClient(env.REBUILD_ADDR)
		if err != nil {
			logrus.Errorf("[DN]Init Rebuild service err:%s\n", err)
		} else {
			logrus.Infof("[DN]Init Rebuild service:%s\n", env.REBUILD_ADDR)
		}
	}
}

type TaskOpResultListHandler struct {
	pkey string
	m    *pkt.TaskOpResultList
}

func (h *TaskOpResultListHandler) SetMessage(pubkey string, msg proto.Message) (*pkt.ErrorMessage, *int32, *int32) {
	h.pkey = pubkey
	req, ok := msg.(*pkt.TaskOpResultList)
	if ok {
		h.m = req
		return nil, STAT_ROUTINE_NUM, nil
	} else {
		return pkt.NewErrorMsg(pkt.INVALID_ARGS, "Invalid request"), nil, nil
	}
}

func (h *TaskOpResultListHandler) Handle() proto.Message {
	newid, err := GetNodeId(h.pkey)
	if err != nil {
		emsg := fmt.Sprintf("[DNRebuidRep]Invalid node pubkey:%s,ERR:%s\n", h.pkey, err.Error())
		logrus.Errorf(emsg)
		return pkt.NewErrorMsg(pkt.INVALID_NODE_ID, emsg)
	}
	if h.m.NodeId != newid {
		logrus.Warnf("[DNRebuidRep]Node unequal:%d!=%d.\n", newid, h.m.NodeId)
		newid = h.m.NodeId
	}

	if h.m.SrcNodeID == 0 {
		logrus.Warnf("[DNRebuidRep]SrcNodeID id:0")
		//return pkt.NewErrorMsg(pkt.INVALID_NODE_ID, "Invalid SrcNodeID id:0")
	}
	if h.m.Id == nil || len(h.m.Id) == 0 || h.m.RES == nil || len(h.m.RES) == 0 {
		logrus.Errorf("[DNRebuidRep][%d]Rebuild task OpResultList is empty.\n", newid)
		return pkt.NewErrorMsg(pkt.INVALID_ARGS, "RES NULL")
	}
	if REBUILDER_SERVICE == nil {
		logrus.Errorf("[DNRebuidRep][%d]Rebuild server Not started.\n", newid)
		return &pkt.MultiTaskOpResultRes{ErrCode: 2, SuccNum: 0}
	}
	okList := []int64{}
	for index, idbs := range h.m.Id {
		if h.m.RES[index] == 0 {
			id := env.BytesToId(idbs)
			okList = append(okList, id)
		}
	}
	metas, del, err := dao.GetShardNodes(okList, h.m.SrcNodeID)
	if err != nil {
		return &pkt.MultiTaskOpResultRes{ErrCode: 2, SuccNum: int32(len(metas))}
	}
	delsize := len(del)
	if delsize > 0 {
		bkid := dao.GenerateShardID(delsize)
		for index, id := range del {
			dao.SaveShardBakup(bkid+int64(index), id, h.m.SrcNodeID)
		}
	}
	if time.Now().Unix() < h.m.ExpiredTime {
		err := SaveRep(newid, metas)
		if err != nil {
			return &pkt.MultiTaskOpResultRes{ErrCode: 2, SuccNum: int32(len(metas))}
		}
		startTime := time.Now()
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*time.Duration(env.P2P_ReadTimeout))
		defer cancel()
		req := &pbrebuilder.MultiTaskOpResult{Id: h.m.Id, RES: h.m.RES, NodeID: newid, ExpiredTime: h.m.ExpiredTime, SrcNodeID: h.m.SrcNodeID}
		err = REBUILDER_SERVICE.UpdateTaskStatus(ctx, req)
		if err != nil {
			logrus.Errorf("[DNRebuidRep][%d]Update rebuild TaskStatus,count:%d/%d,ERR:%s,take times %d ms\n",
				newid, len(metas), len(h.m.Id), err, time.Since(startTime).Milliseconds())
			return &pkt.MultiTaskOpResultRes{ErrCode: 2, SuccNum: int32(len(metas))}
		} else {
			logrus.Infof("[DNRebuidRep][%d]Update rebuild TaskStatus OK,count:%d/%d,take times %d ms\n",
				newid, len(metas), len(h.m.Id), time.Since(startTime).Milliseconds())
			return &pkt.MultiTaskOpResultRes{ErrCode: 0, SuccNum: int32(len(metas))}
		}
	} else {
		logrus.Warnf("[DNRebuidRep][%d]ExpiredTime:%d<%d.\n", newid, h.m.ExpiredTime, time.Now().Unix())
		return &pkt.MultiTaskOpResultRes{ErrCode: 1, SuccNum: int32(len(metas))}
	}
}

func SaveRep(newid int32, metas []*dao.ShardMeta) error {
	size := len(metas)
	if size == 0 {
		return nil
	}
	rebuildmeta := []*dao.ShardRebuidMeta{}
	vbi := dao.GenerateShardID(size)
	vbi2 := dao.GenerateShardID(size)
	for index, m := range metas {
		if m.NodeId != -1 {
			rm := &dao.ShardRebuidMeta{ID: vbi + int64(index)}
			rm.VFI = m.VFI
			rm.NewNodeId = newid
			rm.OldNodeId = m.NodeId
			rebuildmeta = append(rebuildmeta, rm)
		}
		if m.NodeId2 != -1 {
			rm := &dao.ShardRebuidMeta{ID: vbi2 + int64(index)}
			rm.VFI = m.VFI
			rm.NewNodeId = newid
			rm.OldNodeId = m.NodeId2
			rebuildmeta = append(rebuildmeta, rm)
		}
	}
	count := make(map[int32]int16)
	for _, res := range rebuildmeta {
		num, ok := count[res.NewNodeId]
		if ok {
			count[res.NewNodeId] = num + 1
		} else {
			count[res.NewNodeId] = 1
		}
		if res.OldNodeId != 0 {
			num, ok = count[res.OldNodeId]
			if ok {
				count[res.OldNodeId] = num - 1
			} else {
				count[res.OldNodeId] = -1
			}
		}
	}
	startTime := time.Now()
	err := dao.UpdateShardMeta(metas, newid)
	if err != nil {
		logrus.Errorf("[DNRebuidRep][%d]UpdateShardMeta ERR:%s,count %d,take times %d ms\n",
			newid, err, size, time.Since(startTime).Milliseconds())
		return err
	} else {
		logrus.Infof("[DNRebuidRep][%d]UpdateShardMeta OK,count %d,take times %d ms\n",
			newid, size, time.Since(startTime).Milliseconds())
	}
	bs := dao.ToBytes(count)
	err = dao.SaveNodeShardCount(vbi, bs)
	if err != nil {
		return err
	}
	startTime = time.Now()
	err = dao.SaveShardRebuildMetas(rebuildmeta)
	if err != nil {
		logrus.Errorf("[DNRebuidRep][%d]Save Rebuild TaskOpResult ERR:%s,count %d,take times %d ms\n",
			newid, err, size, time.Since(startTime).Milliseconds())
		return err
	} else {
		logrus.Infof("[DNRebuidRep][%d]Save Rebuild TaskOpResult OK, count:%d,take times %d ms\n",
			newid, size, time.Since(startTime).Milliseconds())
	}
	return nil
}
