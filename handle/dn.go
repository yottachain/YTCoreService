package handle

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/eoscanada/eos-go/btcsuite/btcutil/base58"
	"github.com/golang/protobuf/proto"
	"github.com/patrickmn/go-cache"
	"github.com/yottachain/YTCoreService/env"
	"github.com/yottachain/YTCoreService/net"
	"github.com/yottachain/YTCoreService/pkt"
	"github.com/yottachain/YTDNMgmt"
	ytanalysis "github.com/yottachain/yotta-analysis"
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

func (h *StatusRepHandler) SetPubkey(pubkey string) {
	h.pkey = pubkey
}

func (h *StatusRepHandler) SetMessage(msg proto.Message) *pkt.ErrorMessage {
	req, ok := msg.(*pkt.StatusRepReq)
	if ok {
		h.m = req
		if h.m.Addrs == nil || len(h.m.Addrs) == 0 {
			return pkt.NewErrorMsg(pkt.INVALID_ARGS, "Invalid request:Null value")
		}
		return nil
	} else {
		return pkt.NewErrorMsg(pkt.INVALID_ARGS, "Invalid request")
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
	node.Addrs = addrs
	NodeStatSync(node)
	SendSpotCheck(node)
	env.Log.Debugf("StatusRep Node:%d,take times %d ms\n", h.m.Id, time.Now().Sub(startTime).Milliseconds())
	return statusRepResp
}

var NODE_MAP = struct {
	sync.RWMutex
	nodes map[int32]*YTDNMgmt.Node
}{nodes: make(map[int32]*YTDNMgmt.Node)}

func NodeStatSync(node *YTDNMgmt.Node) {
	NODE_MAP.Lock()
	NODE_MAP.nodes[node.ID] = node
	NODE_MAP.Unlock()
}

func DoNodeStatSyncLoop() {
	for {
		time.Sleep(time.Duration(30) * time.Second)
		if net.IsActive() {
			DoNodeStatSync()
		}
		time.Sleep(time.Duration(30) * time.Second)
	}
}

func DoNodeStatSync() {
	ns := []*pkt.NodeSyncReq_Node{}
	ids := []int32{}
	NODE_MAP.RLock()
	for k, v := range NODE_MAP.nodes {
		if time.Now().Unix()-v.Timestamp < 60*4 {
			n := &pkt.NodeSyncReq_Node{
				Id:            &v.ID,
				Cpu:           &v.CPU,
				Memory:        &v.Memory,
				Bandwidth:     &v.Bandwidth,
				MaxDataSpace:  &v.MaxDataSpace,
				AssignedSpace: &v.AssignedSpace,
				UsedSpace:     &v.UsedSpace,
				Addrs:         v.Addrs,
				Relay:         &v.Relay,
				Version:       &v.Version,
				Rebuilding:    &v.Rebuilding,
				RealSpace:     &v.RealSpace,
				Tx:            &v.Tx,
				Rx:            &v.Rx,
				Other:         &v.Ext,
				Timestamp:     &v.Timestamp,
			}
			ns = append(ns, n)
		} else {
			ids = append(ids, k)
		}
	}
	NODE_MAP.RUnlock()
	if len(ids) > 0 {
		NODE_MAP.Lock()
		for _, id := range ids {
			delete(NODE_MAP.nodes, id)
		}
		NODE_MAP.Unlock()
	}
	if len(ns) > 0 {
		nodeSyncReq := &pkt.NodeSyncReq{Node: ns}
		_, err := SyncRequest(nodeSyncReq, env.SuperNodeID, 3)
		if err != nil {
			env.Log.Errorf("Sync Node STAT,ERR:%s\n", err.Error())
		} else {
			env.Log.Debugf("Sync Node STAT,count:%d\n", len(ns))
		}
	}
}

type NodeSyncHandler struct {
	pkey string
	m    *pkt.NodeSyncReq
}

func (h *NodeSyncHandler) SetPubkey(pubkey string) {
	h.pkey = pubkey
}

func (h *NodeSyncHandler) SetMessage(msg proto.Message) *pkt.ErrorMessage {
	req, ok := msg.(*pkt.NodeSyncReq)
	if ok {
		h.m = req
		if h.m.Node == nil || len(h.m.Node) == 0 {
			return pkt.NewErrorMsg(pkt.INVALID_ARGS, "Invalid request:Null value")
		}
		return nil
	} else {
		return pkt.NewErrorMsg(pkt.INVALID_ARGS, "Invalid request")
	}
}

func (h *NodeSyncHandler) Handle() proto.Message {
	sn, err := net.AuthSuperNode(h.pkey)
	if err != nil {
		env.Log.Errorf("%s\n", err)
		return pkt.NewErrorMsg(pkt.INVALID_NODE_ID, err.Error())
	}
	defer func() {
		if r := recover(); r != nil {
			env.Log.Tracef("NodeSyncHandler ERR:%s\n", r)
		}
	}()
	startTime := time.Now()
	for _, n := range h.m.Node {
		node := &YTDNMgmt.Node{
			ID:            *n.Id,
			CPU:           *n.Cpu,
			Memory:        *n.Memory,
			Bandwidth:     *n.Bandwidth,
			MaxDataSpace:  *n.MaxDataSpace,
			AssignedSpace: *n.AssignedSpace,
			UsedSpace:     *n.UsedSpace,
			Addrs:         n.Addrs,
			Relay:         *n.Relay,
			Version:       *n.Version,
			Rebuilding:    *n.Rebuilding,
			RealSpace:     *n.RealSpace,
			Tx:            *n.Tx,
			Rx:            *n.Rx,
			Ext:           *n.Other,
			Timestamp:     *n.Timestamp,
		}
		err := net.NodeMgr.SyncNode(node)
		if err != nil {
			env.Log.Errorf("SyncNode ERR:%s,ID:%d\n", err.Error(), *n.Id)
		}
	}
	env.Log.Debugf("Sync Node STAT,count:%d,from sn %d,take times %d ms.\n", len(h.m.Node), sn.ID, time.Now().Sub(startTime).Milliseconds())
	return &pkt.VoidResp{}
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
		if atomic.LoadInt32(ROUTINE_SIZE) > MAX_ROUTINE_SIZE {
			env.Log.Errorf("Exec SpotCheck ERR:Too many routines.\n")
			return
		}
		atomic.AddInt32(ROUTINE_SIZE, 1)
		defer atomic.AddInt32(ROUTINE_SIZE, -1)
		go ExecSendSpotCheck()
	}
}

func ExecSendSpotCheck() {
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
			_, err := net.RequestDN(req, n, req.TaskId)
			if err != nil {
				env.Log.Errorf("Send task ERR:%d--%s\n", err.Code, err.Msg)
			} else {
				env.Log.Infof("Send task OK.")
			}
		}
	}
}

type SpotCheckRepHandler struct {
	pkey string
	m    *pkt.SpotCheckStatus
}

func (h *SpotCheckRepHandler) SetPubkey(pubkey string) {
	h.pkey = pubkey
}

func (h *SpotCheckRepHandler) SetMessage(msg proto.Message) *pkt.ErrorMessage {
	req, ok := msg.(*pkt.SpotCheckStatus)
	if ok {
		h.m = req
		return nil
	} else {
		return pkt.NewErrorMsg(pkt.INVALID_ARGS, "Invalid request")
	}
}

func (h *SpotCheckRepHandler) Handle() proto.Message {
	_, err := GetNodeId(h.pkey)
	if err != nil {
		emsg := fmt.Sprintf("Invalid node pubkey:%s,ERR:%s\n", h.pkey, err.Error())
		env.Log.Errorf(emsg)
		return pkt.NewErrorMsg(pkt.INVALID_NODE_ID, emsg)
	}
	if h.m.InvalidNodeList == nil || len(h.m.InvalidNodeList) == 0 {
		env.Log.Infof("SpotCheckTaskStatus:%s,invalidNodeList is empty.\n", h.m.TaskId)
	} else {
		for _, res := range h.m.InvalidNodeList {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*time.Duration(net.Writetimeout))
			defer cancel()
			err := SPOTCHECK_SERVICE.UpdateTaskStatus(ctx, h.m.TaskId, int32(res))
			if err != nil {
				env.Log.Errorf("UpdateTaskStatus TaskID=%s,InvalidNode=%d,ERR:%s\n", h.m.TaskId, res, err)
			} else {
				env.Log.Infof("UpdateTaskStatus OK,TaskID=%s,InvalidNode=%d\n", h.m.TaskId, res)
			}
		}
	}
	return &pkt.VoidResp{}
}
