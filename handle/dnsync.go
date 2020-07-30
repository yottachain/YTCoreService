package handle

import (
	"sync"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/sirupsen/logrus"
	"github.com/yottachain/YTCoreService/env"
	"github.com/yottachain/YTCoreService/net"
	"github.com/yottachain/YTCoreService/pkt"
	"github.com/yottachain/YTDNMgmt"
)

var NODE_MAP = struct {
	sync.RWMutex
	nodes map[int32]*YTDNMgmt.Node
}{nodes: make(map[int32]*YTDNMgmt.Node)}

func NodeStatSync(node *YTDNMgmt.Node) {
	NODE_MAP.Lock()
	NODE_MAP.nodes[node.ID] = node
	NODE_MAP.Unlock()
}

func StartSyncNodes() {
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
			logrus.Errorf("[NodeStatSync]Sync Node STAT,ERR:%s\n", err.Error())
		} else {
			logrus.Debugf("[NodeStatSync]Sync Node STAT,count:%d\n", len(ns))
		}
	}
}

type NodeSyncHandler struct {
	pkey string
	m    *pkt.NodeSyncReq
}

func (h *NodeSyncHandler) SetMessage(pubkey string, msg proto.Message) (*pkt.ErrorMessage, *int32, *int32) {
	h.pkey = pubkey
	req, ok := msg.(*pkt.NodeSyncReq)
	if ok {
		h.m = req
		if h.m.Node == nil || len(h.m.Node) == 0 {
			return pkt.NewErrorMsg(pkt.INVALID_ARGS, "Invalid request:Null value"), nil, nil
		}
		return nil, SYNC_ROUTINE_NUM, nil
	} else {
		return pkt.NewErrorMsg(pkt.INVALID_ARGS, "Invalid request"), nil, nil
	}
}

func (h *NodeSyncHandler) Handle() proto.Message {
	sn, err := net.AuthSuperNode(h.pkey)
	if err != nil {
		logrus.Errorf("[NodeStatSync]AuthSuper ERR:%s\n", err)
		return pkt.NewErrorMsg(pkt.INVALID_NODE_ID, err.Error())
	}
	defer func() {
		if r := recover(); r != nil {
			logrus.Errorf("[NodeStatSync]ERR:%s\n", r)
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
			logrus.Errorf("[NodeStatSync]ERR:%s,ID:%d\n", err.Error(), *n.Id)
		}
	}
	logrus.Debugf("[NodeStatSync]Count:%d,from sn %d,take times %d ms.\n", len(h.m.Node), sn.ID, time.Now().Sub(startTime).Milliseconds())
	return &pkt.VoidResp{}
}
