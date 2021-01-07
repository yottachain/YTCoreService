package handle

import (
	"bytes"
	"fmt"
	"strconv"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/patrickmn/go-cache"
	"github.com/sirupsen/logrus"
	"github.com/yottachain/YTCoreService/dao"
	"github.com/yottachain/YTCoreService/net"
	"github.com/yottachain/YTCoreService/pkt"
	"github.com/yottachain/YTCrypto"
	"github.com/yottachain/YTDNMgmt"
)

type ListSuperNodeHandler struct {
	pkey string
	m    *pkt.ListSuperNodeReq
}

func (h *ListSuperNodeHandler) SetMessage(pubkey string, msg proto.Message) (*pkt.ErrorMessage, *int32, *int32) {
	h.pkey = pubkey
	req, ok := msg.(*pkt.ListSuperNodeReq)
	if ok {
		h.m = req
		return nil, READ_ROUTINE_NUM, nil
	} else {
		return pkt.NewErrorMsg(pkt.INVALID_ARGS, "Invalid request"), nil, nil
	}
}

func (h *ListSuperNodeHandler) Handle() proto.Message {
	ls := net.GetSuperNodes()
	count := uint32(len(ls))
	snlist := make([]*pkt.ListSuperNodeResp_SuperNodes_SuperNode, count)
	for index, n := range ls {
		pkey := "NA"
		snlist[index] = &pkt.ListSuperNodeResp_SuperNodes_SuperNode{Id: &n.ID, Nodeid: &n.NodeID, Pubkey: &n.PubKey, Privkey: &pkey, Addrs: n.Addrs}
	}
	sns := &pkt.ListSuperNodeResp_SuperNodes{Count: &count, Supernode: snlist}
	resp := &pkt.ListSuperNodeResp{Supernodes: sns}
	return resp
}

var NODELIST_CACHE = cache.New(1*time.Minute, 1*time.Minute)

type PreAllocNodeHandler struct {
	pkey string
	m    *pkt.PreAllocNodeReqV2
	user *dao.User
}

func (h *PreAllocNodeHandler) SetMessage(pubkey string, msg proto.Message) (*pkt.ErrorMessage, *int32, *int32) {
	h.pkey = pubkey
	req, ok := msg.(*pkt.PreAllocNodeReqV2)
	if ok {
		h.m = req
		if h.m.UserId == nil || h.m.SignData == nil || h.m.KeyNumber == nil {
			return pkt.NewErrorMsg(pkt.INVALID_ARGS, "Invalid request:Null value"), nil, nil
		}
		if h.m.Count == nil {
			h.m.Count = new(uint32)
			*h.m.Count = 1000
		} else {
			if *h.m.Count > 1000 {
				*h.m.Count = 1000
			}
			if *h.m.Count < 100 {
				*h.m.Count = 100
			}
		}
		h.user = dao.GetUserCache(int32(*h.m.UserId), int(*h.m.KeyNumber), *h.m.SignData)
		if h.user == nil {
			return pkt.NewError(pkt.INVALID_SIGNATURE), nil, nil
		}
		return nil, READ_ROUTINE_NUM, h.user.Routine
	} else {
		return pkt.NewErrorMsg(pkt.INVALID_ARGS, "Invalid request"), nil, nil
	}
}

func (h *PreAllocNodeHandler) Handle() proto.Message {
	v, found := NODELIST_CACHE.Get(strconv.Itoa(int(h.user.UserID)))
	if found {
		logrus.Debugf("[PreAllocNode]User %d AllocNodes,from cache\n", h.user.UserID)
		return v.(*pkt.PreAllocNodeResp)
	}
	logrus.Infof("[PreAllocNode]User %d AllocNodes,count:%d\n", h.user.UserID, *h.m.Count)
	nodes := []*pkt.PreAllocNodeResp_PreAllocNode{}
	ls, err := net.NodeMgr.AllocNodes(int32(*h.m.Count), h.m.Excludes)
	if err != nil {
		logrus.Errorf("[PreAllocNode]User %d AllocNodes,ERR:%s\n", h.user.UserID, err)
		return pkt.NewErrorMsg(pkt.SERVER_ERROR, err.Error())
	}
	for _, n := range ls {
		if len(nodes) >= int(*h.m.Count) {
			break
		}
		nodes = signNode(nodes, n)
	}
	logrus.Infof("[PreAllocNode]User %d AllocNodes OK,return %d\n", h.user.UserID, len(nodes))
	resp := &pkt.PreAllocNodeResp{Preallocnode: nodes}
	NODELIST_CACHE.SetDefault(strconv.Itoa(int(h.user.UserID)), resp)
	return resp
}

func signNode(nodes []*pkt.PreAllocNodeResp_PreAllocNode, n *YTDNMgmt.Node) []*pkt.PreAllocNodeResp_PreAllocNode {
	exist := false
	for _, node := range nodes {
		if *node.Id == n.ID {
			exist = true
			break
		}
	}
	if !exist {
		node := &pkt.PreAllocNodeResp_PreAllocNode{Id: &n.ID,
			Nodeid: &n.NodeID, Pubkey: &n.PubKey, Addrs: n.Addrs, Timestamp: &n.Timestamp}
		node.Weight = &n.Weight
		bytebuf := bytes.NewBuffer([]byte{})
		for _, s := range n.Addrs {
			bytebuf.WriteString(s)
		}
		bytebuf.WriteString(n.PubKey)
		bytebuf.WriteString(n.NodeID)
		data := fmt.Sprintf("%d%s%d", n.ID, bytebuf.String(), n.Timestamp)
		signdata, err := YTCrypto.Sign(net.GetLocalSuperNode().PrivKey, []byte(data))
		if err != nil {
			logrus.Errorf("[PreAllocNode]SignNode ERR%s\n", err)
		} else {
			node.Sign = &signdata
			return append(nodes, node)
		}
	}
	return nodes
}
