package handle

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aurawing/eos-go/btcsuite/btcutil/base58"
	"github.com/golang/protobuf/proto"
	"github.com/patrickmn/go-cache"
	"github.com/sirupsen/logrus"
	"github.com/yottachain/YTCoreService/dao"
	"github.com/yottachain/YTCoreService/env"
	"github.com/yottachain/YTCoreService/net"
	"github.com/yottachain/YTCoreService/pkt"
	"github.com/yottachain/YTDNMgmt"
	ytanalysis "github.com/yottachain/yotta-analysis"
	ytrebuilder "github.com/yottachain/yotta-rebuilder"
	"github.com/yottachain/yotta-rebuilder/pbrebuilder"
)

var NODE_CACHE_BY_PUBKEY = cache.New(10*time.Minute, 5*time.Minute)
var NODE_CACHE_BY_ID = cache.New(10*time.Minute, 5*time.Minute)

func GetNodeId(key string) (int32, error) {
	v, found := NODE_CACHE_BY_PUBKEY.Get(key)
	if !found {
		node, err := net.NodeMgr.GetNodeByPubKey(key)
		if err != nil {
			return 0, err
		} else {
			n := &net.Node{Id: node.ID, Nodeid: node.NodeID, Pubkey: node.PubKey, Addrs: node.Addrs}
			NODE_CACHE_BY_PUBKEY.Set(key, n, cache.DefaultExpiration)
			NODE_CACHE_BY_ID.Set(strconv.Itoa(int(n.Id)), n, cache.DefaultExpiration)
			return node.ID, nil
		}
	}
	return int32(v.(*net.Node).Id), nil
}

func GetNode(id int32) (*net.Node, error) {
	v, found := NODE_CACHE_BY_ID.Get(strconv.Itoa(int(id)))
	if !found {
		node, err := net.NodeMgr.GetNodes([]int32{id})
		if err != nil || node == nil || len(node) == 0 {
			return nil, err
		} else {
			n := &net.Node{Id: node[0].ID, Nodeid: node[0].NodeID, Pubkey: node[0].PubKey, Addrs: node[0].Addrs}
			NODE_CACHE_BY_PUBKEY.Set(n.Pubkey, n, cache.DefaultExpiration)
			NODE_CACHE_BY_ID.Set(strconv.Itoa(int(n.Id)), n, cache.DefaultExpiration)
			return n, nil
		}
	}
	return v.(*net.Node), nil
}

func GetNodes(ids []int32) ([]*net.Node, error) {
	size := len(ids)
	nodes := make([]*net.Node, size)
	for ii := 0; ii < size; ii++ {
		n, err := GetNode(ids[ii])
		if err != nil {
			return nil, err
		}
		nodes[ii] = n
	}
	return nodes, nil
}

type StatusRepHandler struct {
	pkey string
	m    *pkt.StatusRepReq
}

func (h *StatusRepHandler) SetMessage(pubkey string, msg proto.Message) (*pkt.ErrorMessage, *int32, *int32) {
	h.pkey = pubkey
	req, ok := msg.(*pkt.StatusRepReq)
	if ok {
		h.m = req
		if h.m.Addrs == nil || len(h.m.Addrs) == 0 {
			return pkt.NewErrorMsg(pkt.INVALID_ARGS, "Invalid request:Null value"), nil, nil
		}
		return nil, STAT_ROUTINE_NUM, nil
	} else {
		return pkt.NewErrorMsg(pkt.INVALID_ARGS, "Invalid request"), nil, nil
	}
}

func (h *StatusRepHandler) Handle() proto.Message {
	id, err := GetNodeId(h.pkey)
	if err != nil {
		emsg := fmt.Sprintf("[DNStatusRep]Invalid node pubkey:%s,ID,%d,ERR:%s\n", h.pkey, h.m.Id, err.Error())
		logrus.Errorf(emsg)
		return pkt.NewErrorMsg(pkt.INVALID_NODE_ID, emsg)
	}
	if id != int32(h.m.Id) {
		emsg := fmt.Sprintf("[DNStatusRep]Nodeid ERR:%d!=%d\n", id, h.m.Id)
		logrus.Errorf(emsg)
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
		emsg := fmt.Sprintf("[DNStatusRep]ERR:%s,ID:%d,take times %d ms\n", err.Error(), h.m.Id, time.Now().Sub(startTime).Milliseconds())
		logrus.Errorf(emsg)
		return pkt.NewErrorMsg(pkt.INVALID_NODE_ID, emsg)
	}
	productiveSpace := newnode.ProductiveSpace
	relayUrl := ""
	if newnode.Addrs != nil && len(newnode.Addrs) > 0 {
		relayUrl = newnode.Addrs[0]
	}
	statusRepResp := &pkt.StatusRepResp{ProductiveSpace: uint64(productiveSpace), RelayUrl: relayUrl}
	newnode.Addrs = YTDNMgmt.CheckPublicAddrs(node.Addrs, net.NodeMgr.Config.Misc.ExcludeAddrPrefix)
	//NodeStatSync(newnode)
	SendSpotCheck(newnode)
	SendRebuildTask(newnode)
	logrus.Infof("[DNStatusRep]Node:%d,take times %d ms\n", h.m.Id, time.Now().Sub(startTime).Milliseconds())
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
			logrus.Errorf("[DN]Init spotCheck service err:%s\n", err)
		} else {
			logrus.Infof("[DN]Init spotCheck service:%s\n", env.SPOTCHECK_ADDR)
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
			logrus.Errorf("[SendSpotCheck]ERR:Too many routines.\n")
			return
		}
		go ExecSendSpotCheck()
	}
}

func ExecSendSpotCheck() {
	atomic.AddInt32(AYNC_ROUTINE_NUM, 1)
	defer atomic.AddInt32(AYNC_ROUTINE_NUM, -1)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*time.Duration(env.Writetimeout))
	defer cancel()
	defer env.TracePanic()
	ischeck, err := SPOTCHECK_SERVICE.IsNodeSelected(ctx)
	if err != nil {
		logrus.Errorf("[IsNodeSelected]ERR:%s\n", err)
		return
	}
	if ischeck {
		ctx2, cancel2 := context.WithTimeout(context.Background(), time.Second*time.Duration(env.Writetimeout))
		defer cancel2()
		list, err := SPOTCHECK_SERVICE.GetSpotCheckList(ctx2)
		if err != nil {
			logrus.Errorf("[GetSpotCheckList]ERR:%s\n", err)
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
				node := &net.Node{Id: n.ID, Nodeid: n.NodeID, Pubkey: n.PubKey, Addrs: n.Addrs}
				nodes = append(nodes, node)
			}
		}
		SPOT_NODE_LIST.RUnlock()
		for _, n := range nodes {
			_, err := net.RequestDN(req, n, "")
			if err != nil {
				logrus.Errorf("[SendTask][%d]Send spotcheck task [%s] ERR:%d--%s\n", n.Id, req.TaskId, err.Code, err.Msg)
			} else {
				logrus.Infof("[SendTask][%d]Send spotcheck task [%s] OK,count %d\n", n.Id, req.TaskId, num)
			}
		}
	}
}

type SpotCheckRepHandler struct {
	pkey string
	m    *pkt.SpotCheckStatus
}

func (h *SpotCheckRepHandler) SetMessage(pubkey string, msg proto.Message) (*pkt.ErrorMessage, *int32, *int32) {
	h.pkey = pubkey
	req, ok := msg.(*pkt.SpotCheckStatus)
	if ok {
		h.m = req
		return nil, STAT_ROUTINE_NUM, nil
	} else {
		return pkt.NewErrorMsg(pkt.INVALID_ARGS, "Invalid request"), nil, nil
	}
}

func (h *SpotCheckRepHandler) Handle() proto.Message {
	myid, err := GetNodeId(h.pkey)
	if err != nil {
		emsg := fmt.Sprintf("[DNSpotCheckRep]Invalid node pubkey:%s,ERR:%s\n", h.pkey, err.Error())
		logrus.Errorf(emsg)
		return pkt.NewErrorMsg(pkt.INVALID_NODE_ID, emsg)
	}
	if h.m.InvalidNodeList == nil || len(h.m.InvalidNodeList) == 0 {
		logrus.Infof("[DNSpotCheckRep][%d]Exec spotcheck results,TaskID:[%s],Not err.\n", myid, h.m.TaskId)
	} else {
		for _, res := range h.m.InvalidNodeList {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*time.Duration(env.Writetimeout))
			defer cancel()
			err := SPOTCHECK_SERVICE.UpdateTaskStatus(ctx, h.m.TaskId, int32(res))
			if err != nil {
				logrus.Errorf("[DNSpotCheckRep][%d]Exec spotcheck results,TaskID:[%s],Node [%d] ERR,UpdateTaskStatus ERR:%s\n", myid, h.m.TaskId, res, err)
			} else {
				logrus.Infof("[DNSpotCheckRep][%d]Exec spotcheck results,TaskID:[%s],Node [%d] ERR\n", myid, h.m.TaskId, res)
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
		ExecSendRebuildTask(node)
	}
}

func ExecSendRebuildTask(n *YTDNMgmt.Node) {
	atomic.AddInt32(AYNC_ROUTINE_NUM, 1)
	defer atomic.AddInt32(AYNC_ROUTINE_NUM, -1)
	defer env.TracePanic()
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
	req := &pkt.TaskList{Tasklist: ls.Tasklist}
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
	size := len(metas)
	if size > 0 {
		vbi := dao.GenerateShardID(size)
		for index, m := range metas {
			m.ID = vbi + int64(index)
			m.NewNodeId = newid
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*time.Duration(env.Writetimeout))
	defer cancel()
	req := &pbrebuilder.MultiTaskOpResult{Id: h.m.Id, RES: h.m.RES, NodeID: newid}
	err = REBUILDER_SERVICE.UpdateTaskStatus(ctx, req)
	if err != nil {
		logrus.Errorf("[DNRebuidRep][%d]Update rebuild TaskStatus,count:%d/%d,ERR:%s\n", newid, size, len(h.m.Id), err)
	} else {
		logrus.Infof("[DNRebuidRep][%d]Update rebuild TaskStatus OK,count:%d/%d\n", newid, size, len(h.m.Id))
		if size > 0 {
			err = dao.SaveShardRebuildMetas(metas)
			if err != nil {
				logrus.Errorf("[DNRebuidRep][%d]Save Rebuild TaskOpResult ERR:%s\n", newid, err)
			} else {
				logrus.Infof("[DNRebuidRep][%d]Save Rebuild TaskOpResult ok, count:%d\n", newid, size)
			}
		}
	}
	return &pkt.VoidResp{}
}
