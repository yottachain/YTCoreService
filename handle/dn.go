package handle

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/patrickmn/go-cache"
	"github.com/yottachain/YTDNMgmt"
	"github.com/yottachain/YTCoreService/env"
	"github.com/yottachain/YTCoreService/net"
	"github.com/yottachain/YTCoreService/pkt"
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

func SendSpotCheck(node *YTDNMgmt.Node) {
	SPOT_NODE_LIST.Lock()
	pos := SPOT_NODE_LIST.index + 1
	if pos >= env.SPOTCHECKNUM {
		SPOT_NODE_LIST.index = 0
	} else {
		SPOT_NODE_LIST.index = pos
	}
	SPOT_NODE_LIST.nodes[SPOT_NODE_LIST.index] = node
	SPOT_NODE_LIST.Unlock()
}

func ExecSendSpotCheck() error {
	if atomic.LoadInt32(ROUTINE_SIZE) > MAX_ROUTINE_SIZE {
		return errors.New("Too many routines.")
	}
	atomic.AddInt32(ROUTINE_SIZE, 1)
	defer atomic.AddInt32(ROUTINE_SIZE, -1)
	//	net.NodeMgr.SpotcheckSelected()
	return nil
}
