package handle

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/sirupsen/logrus"
	"github.com/yottachain/YTCoreService/dao"
	"github.com/yottachain/YTCoreService/env"
	"github.com/yottachain/YTCoreService/net"
	"github.com/yottachain/YTCoreService/pkt"
	"github.com/yottachain/YTDNMgmt"
	ytrebuilder "github.com/yottachain/yotta-rebuilder"
	"github.com/yottachain/yotta-rebuilder/pbrebuilder"
)

var REBUILDER_SERVICE *ytrebuilder.RebuilderClient

func InitRebuildService() {
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

func SendRebuildTask(node *YTDNMgmt.Node) {
	if REBUILDER_SERVICE != nil {
		if atomic.LoadInt32(AYNC_ROUTINE_NUM) > env.MAX_AYNC_ROUTINE {
			logrus.Errorf("[SendRebuildTask]ERR:Too many routines.\n")
			return
		}
		go ExecSendRebuildTask(node)
	}
}

func ExecSendRebuildTask(n *YTDNMgmt.Node) {
	atomic.AddInt32(AYNC_ROUTINE_NUM, 1)
	defer atomic.AddInt32(AYNC_ROUTINE_NUM, -1)
	defer env.TracePanic("[SendRebuildTask]")
	startTime := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*time.Duration(env.Writetimeout))
	defer cancel()
	ls, err := REBUILDER_SERVICE.GetRebuildTasks(ctx, n.ID)
	stime := time.Now().Sub(startTime).Milliseconds()
	logrus.Infof("[GetRebuildTasks]OK,take times %d ms\n", stime)
	if err != nil {
		logrus.Errorf("[GetRebuildTasks]ERR:%s,take times %d ms\n", err, stime)
		return
	}
	node := &net.Node{Id: n.ID, Nodeid: n.NodeID, Pubkey: n.PubKey, Addrs: n.Addrs}
	req := &pkt.TaskList{Tasklist: ls.Tasklist, ExpiredTime: ls.ExpiredTime, SrcNodeID: ls.SrcNodeID, ExpiredTimeGap: ls.ExpiredTimeGap}
	_, e := net.RequestDN(req, node, "")
	if err != nil {
		logrus.Errorf("[SendRebuildTask][%d]Send rebuild task ERR:%d--%s\n", node.Id, e.Code, e.Msg)
	} else {
		logrus.Infof("[SendRebuildTask][%d]Send rebuild task OK,count %d\n", node.Id, len(ls.Tasklist))
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
	if h.m.Id == nil || len(h.m.Id) == 0 || h.m.RES == nil || len(h.m.RES) == 0 {
		logrus.Errorf("[DNRebuidRep][%d]Rebuild task OpResultList is empty.\n", newid)
		return &pkt.VoidResp{}
	}
	if REBUILDER_SERVICE == nil {
		logrus.Errorf("[DNRebuidRep][%d]Rebuild server Not started.\n", newid)
		return &pkt.VoidResp{}
	}
	okList := []int64{}
	for index, idbs := range h.m.Id {
		if h.m.RES[index] == 0 {
			id := env.BytesToId(idbs)
			okList = append(okList, id)
		}
	}
	metas, err := dao.GetShardNodes(okList)
	if err != nil {
		return &pkt.VoidResp{}
	}
	if time.Now().Unix() < h.m.ExpiredTime {
		err := SaveRep(newid, metas)
		if err != nil {
			return &pkt.VoidResp{}
		}
		startTime := time.Now()
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*time.Duration(env.Writetimeout))
		defer cancel()
		req := &pbrebuilder.MultiTaskOpResult{Id: h.m.Id, RES: h.m.RES, NodeID: newid, ExpiredTime: h.m.ExpiredTime, SrcNodeID: h.m.SrcNodeID}
		err = REBUILDER_SERVICE.UpdateTaskStatus(ctx, req)
		if err != nil {
			logrus.Errorf("[DNRebuidRep][%d]Update rebuild TaskStatus,count:%d/%d,ERR:%s,take times %d ms\n",
				newid, len(metas), len(h.m.Id), err, time.Now().Sub(startTime).Milliseconds())
		} else {
			logrus.Infof("[DNRebuidRep][%d]Update rebuild TaskStatus OK,count:%d/%d,take times %d ms\n",
				newid, len(metas), len(h.m.Id), time.Now().Sub(startTime).Milliseconds())
		}
	} else {
		logrus.Warnf("[DNRebuidRep]ExpiredTime:%d<%d.\n", h.m.ExpiredTime, time.Now().Unix())
	}
	return &pkt.VoidResp{}
}

func SaveRep(newid int32, metas []*dao.ShardRebuidMeta) error {
	size := len(metas)
	if size == 0 {
		return nil
	}
	vbi := dao.GenerateShardID(size)
	for index, m := range metas {
		m.ID = vbi + int64(index)
		m.NewNodeId = newid
	}
	count := make(map[int32]int16)
	upmetas := make(map[int64]int32)
	for _, res := range metas {
		num, ok := count[res.NewNodeId]
		if ok {
			count[res.NewNodeId] = num + 1
		} else {
			count[res.NewNodeId] = 1
		}
		num, ok = count[res.OldNodeId]
		if ok {
			count[res.OldNodeId] = num - 1
		} else {
			count[res.OldNodeId] = -1
		}
		upmetas[res.VFI] = res.NewNodeId
	}
	startTime := time.Now()
	err := dao.UpdateShardMeta(upmetas)
	if err != nil {
		logrus.Errorf("[DNRebuidRep][%d]UpdateShardMeta ERR:%s,count %d,take times %d ms\n",
			newid, err, size, time.Now().Sub(startTime).Milliseconds())
		return err
	} else {
		logrus.Infof("[DNRebuidRep][%d]UpdateShardMeta OK,count %d,take times %d ms\n",
			newid, size, time.Now().Sub(startTime).Milliseconds())
	}
	bs := dao.ToBytes(count)
	err = dao.SaveNodeShardCount(vbi, bs)
	if err != nil {
		return err
	}
	startTime = time.Now()
	err = dao.SaveShardRebuildMetas(metas)
	if err != nil {
		logrus.Errorf("[DNRebuidRep][%d]Save Rebuild TaskOpResult ERR:%s,count %d,take times %d ms\n",
			newid, err, size, time.Now().Sub(startTime).Milliseconds())
		return err
	} else {
		logrus.Infof("[DNRebuidRep][%d]Save Rebuild TaskOpResult OK, count:%d,take times %d ms\n",
			newid, size, time.Now().Sub(startTime).Milliseconds())
	}
	return nil
}
