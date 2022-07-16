package handle

import (
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/patrickmn/go-cache"
	"github.com/sirupsen/logrus"
	"github.com/yottachain/YTCoreService/net"
	"github.com/yottachain/YTCoreService/pkt"
	"github.com/yottachain/YTDNMgmt"
	"google.golang.org/protobuf/proto"
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
		if err != nil {
			logrus.Errorf("[GetNode]NodeID %d,ERR:%s.\n", id, err)
			return nil, err
		} else {
			if len(node) == 0 {
				return nil, nil
			}
			n := &net.Node{Id: node[0].ID, Nodeid: node[0].NodeID, Pubkey: node[0].PubKey, Addrs: node[0].Addrs}
			NODE_CACHE_BY_PUBKEY.Set(n.Pubkey, n, cache.DefaultExpiration)
			NODE_CACHE_BY_ID.Set(strconv.Itoa(int(n.Id)), n, cache.DefaultExpiration)
			return n, nil
		}
	}
	return v.(*net.Node), nil
}

func GetNodes(ids []int32) ([]*net.Node, error) {
	noexistids := ""
	size := len(ids)
	nodes := make([]*net.Node, size)
	for ii := 0; ii < size; ii++ {
		n, err := GetNode(ids[ii])
		if err != nil {
			return nil, err
		}
		if n == nil {
			if noexistids == "" {
				noexistids = strconv.Itoa(int(ids[ii]))
			} else {
				noexistids = noexistids + "," + strconv.Itoa(int(ids[ii]))
			}
		}
		nodes[ii] = n
	}
	if noexistids != "" {
		return nodes, errors.New("NodeID " + noexistids + " does not exist")
	} else {
		return nodes, nil
	}
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
		ID:             h.m.Id,
		CPU:            h.m.Cpu,
		Memory:         h.m.Memory,
		Bandwidth:      h.m.Bandwidth,
		MaxDataSpace:   h.m.MaxDataSpace,
		AssignedSpace:  h.m.AssignedSpace,
		UsedSpace:      h.m.UsedSpace,
		Addrs:          addrs,
		Relay:          int32(relay),
		Version:        h.m.Version,
		Rebuilding:     h.m.Rebuilding,
		RealSpace:      h.m.RealSpace,
		Tx:             h.m.Tx,
		Rx:             h.m.Rx,
		Ext:            h.m.Other,
		Timestamp:      time.Now().Unix(),
		HashID:         h.m.Hash,
		AllocatedSpace: int64(h.m.AllocSpace),
		AvailableSpace: int64(h.m.AvailableSpace),
	}
	startTime := time.Now()
	var productiveSpace int64
	newnode, err := net.NodeMgr.UpdateNodeStatus(node)
	if err != nil {
		emsg := fmt.Sprintf("[DNStatusRep]ERR:%s,ID:%d,take times %d ms\n", err.Error(), h.m.Id, time.Since(startTime).Milliseconds())
		logrus.Errorf(emsg)
		var e *YTDNMgmt.ReportError
		if errors.As(err, &e) {
			productiveSpace = int64(e.ErrCode)
		} else {
			productiveSpace = -2
		}
		return &pkt.StatusRepResp{ProductiveSpace: productiveSpace, RelayUrl: ""}
	} else {
		productiveSpace = newnode.ProductiveSpace
	}
	relayUrl := ""
	if newnode.Addrs != nil && len(newnode.Addrs) > 0 {
		relayUrl = newnode.Addrs[0]
	}
	statusRepResp := &pkt.StatusRepResp{ProductiveSpace: productiveSpace, RelayUrl: relayUrl}
	newnode.Addrs = YTDNMgmt.CheckPublicAddrs(node.Addrs, net.NodeMgr.Config.Misc.ExcludeAddrPrefix)
	SendSpotCheck(newnode)
	logrus.Infof("[DNStatusRep]Node:%d,take times %d ms\n", h.m.Id, time.Since(startTime).Milliseconds())
	return statusRepResp
}
