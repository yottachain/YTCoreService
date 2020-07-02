package handle

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aurawing/eos-go/btcsuite/btcutil/base58"
	"github.com/golang/protobuf/proto"
	"github.com/patrickmn/go-cache"
	"github.com/yottachain/YTCoreService/dao"
	"github.com/yottachain/YTCoreService/env"
	"github.com/yottachain/YTCoreService/net"
	"github.com/yottachain/YTCoreService/pkt"
	"github.com/yottachain/YTDNMgmt"
	ytanalysis "github.com/yottachain/yotta-analysis"
	ytrebuilder "github.com/yottachain/yotta-rebuilder"
	"github.com/yottachain/yotta-rebuilder/pbrebuilder"
)

var NODE_CACHE = cache.New(60*time.Minute, 60*time.Minute)

func GetNodeId(key string) (int32, error) {
	v, found := NODE_CACHE.Get(key)
	if !found {
		id, err := net.NodeMgr.GetNodeIDByPubKey(key)
		if err != nil {
			return 0, err
		} else {
			NODE_CACHE.Set(key, id, cache.DefaultExpiration)
			return id, nil
		}
	}
	return v.(int32), nil
}

type StatusRepHandler struct {
	pkey string
	m    *pkt.StatusRepReq
}

func (h *StatusRepHandler) SetMessage(pubkey string, msg proto.Message) (*pkt.ErrorMessage, *int32) {
	h.pkey = pubkey
	req, ok := msg.(*pkt.StatusRepReq)
	if ok {
		h.m = req
		if h.m.Addrs == nil || len(h.m.Addrs) == 0 {
			return pkt.NewErrorMsg(pkt.INVALID_ARGS, "Invalid request:Null value"), nil
		}
		return nil, STAT_ROUTINE_NUM
	} else {
		return pkt.NewErrorMsg(pkt.INVALID_ARGS, "Invalid request"), nil
	}
}

func (h *StatusRepHandler) Handle() proto.Message {
	id, err := GetNodeId(h.pkey)
	if err != nil {
		emsg := fmt.Sprintf("Invalid node pubkey:%s,ID,%d,ERR:%s\n", h.pkey, h.m.Id, err.Error())
		env.Log.Errorf(emsg)
		return pkt.NewErrorMsg(pkt.INVALID_NODE_ID, emsg)
	}
	if id != int32(h.m.Id) {
		emsg := fmt.Sprintf("StatusRep Nodeid ERR:%d!=%d\n", id, h.m.Id)
		env.Log.Errorf(emsg)
		return pkt.NewErrorMsg(pkt.INVALID_NODE_ID, emsg)
	}
	relay := 0
	if h.m.Relay {
		relay = 1
	}
	addrs := h.m.Addrs
	node := &YTDNMgmt.Node{
		ID:            h.m.Id,
		CPU:           h.m.Cpu,
		Memory:        h.m.Memory,
		Bandwidth:     h.m.Bandwidth,
		MaxDataSpace:  h.m.MaxDataSpace,
		AssignedSpace: h.m.AssignedSpace,
		UsedSpace:     h.m.UsedSpace,
		Addrs:         addrs,
		Relay:         int32(relay),
		Version:       h.m.Version,
		Rebuilding:    h.m.Rebuilding,
		RealSpace:     h.m.RealSpace,
		Tx:            h.m.Tx,
		Rx:            h.m.Rx,
		Ext:           h.m.Other,
		Timestamp:     time.Now().Unix(),
	}
	startTime := time.Now()
	newnode, err := net.NodeMgr.UpdateNodeStatus(node)
	if err != nil {
		emsg := fmt.Sprintf("UpdateNodeStatus ERR:%s,ID:%d,take times %d ms\n", err.Error(), h.m.Id, time.Now().Sub(startTime).Milliseconds())
		env.Log.Errorf(emsg)
		return pkt.NewErrorMsg(pkt.INVALID_NODE_ID, emsg)
	}
	productiveSpace := newnode.ProductiveSpace
	relayUrl := ""
	if newnode.Addrs != nil && len(newnode.Addrs) > 0 {
		relayUrl = newnode.Addrs[0]
	}
	statusRepResp := &pkt.StatusRepResp{ProductiveSpace: uint64(productiveSpace), RelayUrl: relayUrl}
	newnode.Addrs = addrs
	//NodeStatSync(newnode)
	SendSpotCheck(newnode)
	SendRebuildTask(newnode)
	env.Log.Debugf("StatusRep Node:%d,take times %d ms\n", h.m.Id, time.Now().Sub(startTime).Milliseconds())
	return statusRepResp
}

var SPOT_NODE_LIST = struct {
	sync.RWMutex
	nodes [env.SPOTCHECKNUM]*YTDNMgmt.Node
	index int
}{index: 0}

var SPOTCHECK_SERVICE *ytanalysis.AnalysisClient

func InitSpotCheckService() {
	if env.SPOTCHECK_ADDR != "" {
		var err error
		SPOTCHECK_SERVICE, err = ytanalysis.NewClient(env.SPOTCHECK_ADDR)
		if err != nil {
			env.Log.Errorf("Init SpotCheck service err:%s\n", err)
		} else {
			env.Log.Infof("Init SpotCheck service:%s\n", env.SPOTCHECK_ADDR)
		}
	}
}

func SendSpotCheck(node *YTDNMgmt.Node) {
	if SPOTCHECK_SERVICE != nil {
		SPOT_NODE_LIST.Lock()
		pos := SPOT_NODE_LIST.index + 1
		if pos >= env.SPOTCHECKNUM {
			SPOT_NODE_LIST.index = 0
		} else {
			SPOT_NODE_LIST.index = pos
		}
		SPOT_NODE_LIST.nodes[SPOT_NODE_LIST.index] = node
		SPOT_NODE_LIST.Unlock()
		if atomic.LoadInt32(AYNC_ROUTINE_NUM) > env.MAX_AYNC_ROUTINE {
			env.Log.Errorf("Exec SpotCheck ERR:Too many routines.\n")
			return
		}
		go ExecSendSpotCheck()
	}
}

func ExecSendSpotCheck() {
	atomic.AddInt32(AYNC_ROUTINE_NUM, 1)
	defer atomic.AddInt32(AYNC_ROUTINE_NUM, -1)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*time.Duration(net.Writetimeout))
	defer cancel()
	ischeck, err := SPOTCHECK_SERVICE.IsNodeSelected(ctx)
	if err != nil {
		env.Log.Errorf("IsNodeSelected ERR:%s\n", err)
		return
	}
	if ischeck {
		ctx2, cancel2 := context.WithTimeout(context.Background(), time.Second*time.Duration(net.Writetimeout))
		defer cancel2()
		list, err := SPOTCHECK_SERVICE.GetSpotCheckList(ctx2)
		if err != nil {
			env.Log.Errorf("GetSpotCheckList ERR:%s\n", err)
			return
		}
		num := len(list.TaskList)
		req := &pkt.SpotCheckTaskList{TaskId: list.TaskID.Hex(), Snid: int32(env.SuperNodeID)}
		req.TaskList = make([]*pkt.SpotCheckTask, num)
		for ii := 0; ii < num; ii++ {
			t := list.TaskList[ii]
			req.TaskList[ii] = &pkt.SpotCheckTask{Id: t.ID, NodeId: t.NodeID, Addr: t.Addr}
			vni := base58.Decode(t.VNI)
			size := len(vni)
			if size > 16 {
				vni = vni[size-16:]
			}
			req.TaskList[ii].VHF = vni
		}
		nodes := []*net.Node{}
		SPOT_NODE_LIST.RLock()
		for _, n := range SPOT_NODE_LIST.nodes {
			if n != nil {
				node := &net.Node{Id: uint32(n.ID), Nodeid: n.NodeID, Pubkey: n.PubKey, Addrs: n.Addrs}
				nodes = append(nodes, node)
			}
		}
		SPOT_NODE_LIST.RUnlock()
		for _, n := range nodes {
			_, err := net.RequestDN(req, n, "")
			if err != nil {
				env.Log.Errorf("[%d]Send spotcheck task [%s] ERR:%d--%s\n", n.Id, req.TaskId, err.Code, err.Msg)
			} else {
				env.Log.Infof("[%d]Send spotcheck task [%s] OK,count %d\n", n.Id, req.TaskId, num)
			}
		}
	}
}

type SpotCheckRepHandler struct {
	pkey string
	m    *pkt.SpotCheckStatus
}

func (h *SpotCheckRepHandler) SetMessage(pubkey string, msg proto.Message) (*pkt.ErrorMessage, *int32) {
	h.pkey = pubkey
	req, ok := msg.(*pkt.SpotCheckStatus)
	if ok {
		h.m = req
		return nil, STAT_ROUTINE_NUM
	} else {
		return pkt.NewErrorMsg(pkt.INVALID_ARGS, "Invalid request"), nil
	}
}

func (h *SpotCheckRepHandler) Handle() proto.Message {
	myid, err := GetNodeId(h.pkey)
	if err != nil {
		emsg := fmt.Sprintf("Invalid node pubkey:%s,ERR:%s\n", h.pkey, err.Error())
		env.Log.Errorf(emsg)
		return pkt.NewErrorMsg(pkt.INVALID_NODE_ID, emsg)
	}
	if h.m.InvalidNodeList == nil || len(h.m.InvalidNodeList) == 0 {
		env.Log.Infof("[%d]Exec spotcheck results,TaskID:[%s],Not err.\n", myid, h.m.TaskId)
	} else {
		for _, res := range h.m.InvalidNodeList {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*time.Duration(net.Writetimeout))
			defer cancel()
			err := SPOTCHECK_SERVICE.UpdateTaskStatus(ctx, h.m.TaskId, int32(res))
			if err != nil {
				env.Log.Errorf("[%d]Exec spotcheck results,TaskID:[%s],Node [%d] mistake,UpdateTaskStatus ERR:%s\n", myid, h.m.TaskId, res, err)
			} else {
				env.Log.Infof("[%d]Exec spotcheck results,TaskID:[%s],Node [%d] mistake\n", myid, h.m.TaskId, res)
			}
		}
	}
	return &pkt.VoidResp{}
}

var REBUILDER_SERVICE *ytrebuilder.RebuilderClient

func InitRebuildService() {
	if env.REBUILD_ADDR != "" {
		var err error
		REBUILDER_SERVICE, err = ytrebuilder.NewClient(env.REBUILD_ADDR)
		if err != nil {
			env.Log.Errorf("Init Rebuild service err:%s\n", err)
		} else {
			env.Log.Infof("Init Rebuild service:%s\n", env.REBUILD_ADDR)
		}
	}
}

func SendRebuildTask(node *YTDNMgmt.Node) {
	if REBUILDER_SERVICE != nil {
		if atomic.LoadInt32(AYNC_ROUTINE_NUM) > env.MAX_AYNC_ROUTINE {
			env.Log.Errorf("Exec SendRebuildTask ERR:Too many routines.\n")
			return
		}
		go ExecSendRebuildTask(node)
	}
}

func ExecSendRebuildTask(n *YTDNMgmt.Node) {
	atomic.AddInt32(AYNC_ROUTINE_NUM, 1)
	defer atomic.AddInt32(AYNC_ROUTINE_NUM, -1)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*time.Duration(net.Writetimeout))
	defer cancel()
	ls, err := REBUILDER_SERVICE.GetRebuildTasks(ctx)
	if err != nil {
		env.Log.Errorf("GetRebuildTasks ERR:%s\n", err)
	}
	node := &net.Node{Id: uint32(n.ID), Nodeid: n.NodeID, Pubkey: n.PubKey, Addrs: n.Addrs}
	req := &pkt.TaskList{Tasklist: ls.Tasklist}
	_, e := net.RequestDN(req, node, "")
	if err != nil {
		env.Log.Errorf("[%d]Send rebuild task ERR:%d--%s\n", node.Id, e.Code, e.Msg)
	} else {
		env.Log.Infof("[%d]Send rebuild task OK,count \n", node.Id, len(ls.Tasklist))
	}
}

type TaskOpResultListHandler struct {
	pkey string
	m    *pkt.TaskOpResultList
}

func (h *TaskOpResultListHandler) SetMessage(pubkey string, msg proto.Message) (*pkt.ErrorMessage, *int32) {
	h.pkey = pubkey
	req, ok := msg.(*pkt.TaskOpResultList)
	if ok {
		h.m = req
		return nil, STAT_ROUTINE_NUM
	} else {
		return pkt.NewErrorMsg(pkt.INVALID_ARGS, "Invalid request"), nil
	}
}

func (h *TaskOpResultListHandler) Handle() proto.Message {
	newid, err := GetNodeId(h.pkey)
	if err != nil {
		emsg := fmt.Sprintf("Invalid node pubkey:%s,ERR:%s\n", h.pkey, err.Error())
		env.Log.Errorf(emsg)
		return pkt.NewErrorMsg(pkt.INVALID_NODE_ID, emsg)
	}
	if h.m.Id == nil || len(h.m.Id) == 0 || h.m.RES == nil || len(h.m.RES) == 0 {
		env.Log.Errorf("[%d]Rebuild task OpResultList is empty.\n", newid)
		return &pkt.VoidResp{}
	}
	if REBUILDER_SERVICE == nil {
		env.Log.Errorf("[%d]Rebuild server Not started.\n", newid)
		return &pkt.VoidResp{}
	}
	okList := []int64{}
	for index, idbs := range h.m.Id {
		if h.m.RES[index] == 0 {
			id := dao.BytesToId(idbs)
			okList = append(okList, id)
		}
	}
	metas, err := dao.GetShardNodes(okList)
	if err != nil {
		return &pkt.VoidResp{}
	}
	size := len(metas)
	vbi := dao.GenerateShardID(size)
	for index, m := range metas {
		m.ID = vbi + int64(index)
		m.NewNodeId = newid
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*time.Duration(net.Writetimeout))
	defer cancel()
	req := &pbrebuilder.MultiTaskOpResult{Id: h.m.Id, RES: h.m.RES}
	err = REBUILDER_SERVICE.UpdateTaskStatus(ctx, req)
	if err != nil {
		env.Log.Errorf("[%d]Update rebuid TaskStatus, count=%d,ERR:%s\n", newid, len(h.m.Id), err)
	} else {
		env.Log.Infof("[%d]Update rebuid TaskStatus OK,count=%d\n", newid, len(h.m.Id))
	}
	err = dao.SaveShardRebuildMetas(metas)
	if err != nil {
		env.Log.Errorf("[%d]Save Rebuild TaskOpResult ERR:%s\n", newid, err)
	} else {
		env.Log.Infof("[%d]Save Rebuild TaskOpResult ok, count:%d\n", newid, size)
	}
	return &pkt.VoidResp{}
}
