package handle

import (
	"context"
	"encoding/base64"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/sirupsen/logrus"
	"github.com/yottachain/YTCoreService/env"
	"github.com/yottachain/YTCoreService/net"
	"github.com/yottachain/YTCoreService/pkt"
	"github.com/yottachain/YTDNMgmt"
	ytanalysis "github.com/yottachain/yotta-analysis"
)

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
	defer env.TracePanic("[SendSpotCheckTask]")
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
			vni, err := base64.StdEncoding.DecodeString(t.VNI)
			if err != nil {
				logrus.Warnf("[GetSpotCheckList]Return ERR VNI:%s\n", t.VNI)
				req.TaskList[ii].VHF = []byte{}
				continue
			}
			size := len(vni)
			if size > 16 {
				vni = vni[size-16:]
			}
			if size < 16 {
				logrus.Warnf("[GetSpotCheckList]Return ERR len(%d) VNI:%s\n", size, t.VNI)
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
			_, err := net.RequestDN(req, n, "", false)
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
		startTime := time.Now()
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
		logrus.Infof("[DNSpotCheckRep]UpdateTaskStatus OK,count %d,take times %d ms\n", len(h.m.InvalidNodeList), time.Now().Sub(startTime).Milliseconds())
	}
	return &pkt.VoidResp{}
}
