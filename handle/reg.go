package handle

import (
	"bytes"
	"fmt"
	"strconv"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/mr-tron/base58"
	"github.com/patrickmn/go-cache"
	"github.com/sirupsen/logrus"
	"github.com/yottachain/YTCoreService/dao"
	"github.com/yottachain/YTCoreService/env"
	"github.com/yottachain/YTCoreService/net"
	"github.com/yottachain/YTCoreService/pkt"
	"github.com/yottachain/YTCrypto"
	"github.com/yottachain/YTDNMgmt"
)

type ListSuperNodeHandler struct {
	pkey string
	m    *pkt.ListSuperNodeReq
}

func (h *ListSuperNodeHandler) SetMessage(pubkey string, msg proto.Message) (*pkt.ErrorMessage, *int32) {
	h.pkey = pubkey
	req, ok := msg.(*pkt.ListSuperNodeReq)
	if ok {
		h.m = req
		return nil, READ_ROUTINE_NUM
	} else {
		return pkt.NewErrorMsg(pkt.INVALID_ARGS, "Invalid request"), nil
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

var REG_PEER_CACHE = cache.New(5*time.Second, 5*time.Second)

type RegUserHandler struct {
	pkey string
	m    *pkt.RegUserReqV2
}

func (h *RegUserHandler) SetMessage(pubkey string, msg proto.Message) (*pkt.ErrorMessage, *int32) {
	h.pkey = pubkey
	req, ok := msg.(*pkt.RegUserReqV2)
	if ok {
		h.m = req
		if h.m.PubKey == nil || h.m.Username == nil || h.m.VersionId == nil {
			return pkt.NewErrorMsg(pkt.INVALID_ARGS, "Invalid request:Null value"), nil
		}
		return nil, READ_ROUTINE_NUM
	} else {
		return pkt.NewErrorMsg(pkt.INVALID_ARGS, "Invalid request"), nil
	}
}

func (h *RegUserHandler) Handle() proto.Message {
	logrus.Infof("[RegUser]Name:%s\n", *h.m.Username)
	if env.S3Version != "" {
		if *h.m.VersionId == "" || bytes.Compare([]byte(*h.m.VersionId), []byte(env.S3Version)) < 0 {
			errmsg := fmt.Sprintf("[RegUser]Name:%s,ERR:TOO_LOW_VERSION?%s\n", *h.m.Username, *h.m.VersionId)
			logrus.Errorf(errmsg)
			return pkt.NewErrorMsg(pkt.TOO_LOW_VERSION, errmsg)
		}
	}
	_, found := NODELIST_CACHE.Get(h.pkey)
	if found {
		errmsg := fmt.Sprintf("[RegUser]Name:%s,ERR:TOO_FREQUENTLY\n", *h.m.Username)
		logrus.Errorf(errmsg)
		return pkt.NewErrorMsg(pkt.SERVER_ERROR, errmsg)
	}
	sn := net.GetRegSuperNode(*h.m.Username)
	queryUserReqV2 := &pkt.QueryUserReqV2{
		Pubkey:   h.m.PubKey,
		Username: h.m.Username,
		UserId:   new(int32),
	}
	*queryUserReqV2.UserId = -1
	var res proto.Message
	if sn.ID == int32(env.SuperNodeID) {
		handler := &QueryUserHandler{pkey: sn.PubKey, m: queryUserReqV2}
		res = handler.Handle()
		if serr, ok := res.(*pkt.ErrorMessage); ok {
			return serr
		}
	} else {
		var serr *pkt.ErrorMessage
		res, serr = net.RequestSN(queryUserReqV2, sn, "", 0)
		if serr != nil {
			return serr
		}
	}
	queryUserResp, ok := res.(*pkt.QueryUserResp)
	if !ok {
		logrus.Errorf("[RegUser]Return error type.\n")
		return pkt.NewErrorMsg(pkt.SERVER_ERROR, "Error type")
	}
	*queryUserReqV2.UserId = *queryUserResp.UserId
	logrus.Infof("[RegUser][%s] is registered @ SN-%d,userID:%d\n", *h.m.Username, sn.ID, *queryUserResp.UserId)
	syncres, err := SyncRequest(queryUserReqV2, int(sn.ID), 0)
	if err != nil {
		logrus.Errorf("[RegUser]SyncRequest err:%s\n", err)
		return pkt.NewErrorMsg(pkt.SERVER_ERROR, "Error type")
	}
	for _, snresp := range syncres {
		if snresp != nil {
			if snresp.Error() != nil {
				logrus.Errorf("[RegUser]Sync userinfo ERR:%d\n", snresp.Error().Code)
				return snresp.Error()
			}
		}
	}
	newsn := net.GetUserSuperNode(*queryUserResp.UserId)
	if newsn.ID != sn.ID {
		logrus.Errorf("[RegUser]SuperID inconsistency[%d!=%d]\n", newsn.ID, sn.ID)
		return pkt.NewError(pkt.SERVER_ERROR)
	}
	resp := &pkt.RegUserResp{SuperNodeNum: new(uint32),
		SuperNodeID: &sn.NodeID, SuperNodeAddrs: sn.Addrs,
		UserId: new(uint32), KeyNumber: new(uint32),
	}
	*resp.SuperNodeNum = uint32(sn.ID)
	*resp.UserId = uint32(*queryUserResp.UserId)
	*resp.KeyNumber = uint32(*queryUserResp.KeyNumber)
	return resp
}

type QueryUserHandler struct {
	pkey string
	m    *pkt.QueryUserReqV2
}

func (h *QueryUserHandler) SetMessage(pubkey string, msg proto.Message) (*pkt.ErrorMessage, *int32) {
	h.pkey = pubkey
	req, ok := msg.(*pkt.QueryUserReqV2)
	if ok {
		h.m = req
		if h.m.Pubkey == nil || h.m.Username == nil || h.m.UserId == nil {
			return pkt.NewErrorMsg(pkt.INVALID_ARGS, "Invalid request:Null value"), nil
		}
		return nil, WRITE_ROUTINE_NUM
	} else {
		return pkt.NewErrorMsg(pkt.INVALID_ARGS, "Invalid request"), nil
	}
}

func (h *QueryUserHandler) Handle() proto.Message {
	_, err := net.AuthSuperNode(h.pkey)
	if err != nil {
		logrus.Errorf("[QueryUser]AuthSuper ERR:%s\n", err)
		return pkt.NewErrorMsg(pkt.INVALID_NODE_ID, err.Error())
	}
	logrus.Debugf("User '%s' sync request.\n", *h.m.Username)
	if *h.m.UserId == -1 {
		_, err := net.GetBalance(*h.m.Username)
		if err != nil {
			logrus.Errorf("[QueryUser]User '%s' auth ERR:%s\n", *h.m.Username, err)
			return pkt.NewErrorMsg(pkt.SERVER_ERROR, "UserID invalid")
		}
		logrus.Infof("[QueryUser][%s] Certification passed.\n", *h.m.Username)
	}
	KUEp, err := base58.Decode(*h.m.Pubkey)
	keyNumber := 0
	user := dao.GetUserByUsername(*h.m.Username)
	if user != nil {
		if *h.m.UserId != -1 && *h.m.UserId != user.UserID {
			logrus.Errorf("[QueryUser]UserID '%d/%d' invalid,username:%s\n", user.UserID, *h.m.UserId, *h.m.Username)
			return pkt.NewErrorMsg(pkt.SERVER_ERROR, "UserID invalid")
		}
		ii := 0
		exists := false
		for ; ii < len(user.KUEp); ii++ {
			if bytes.Equal(user.KUEp[ii], KUEp) {
				keyNumber = ii
				exists = true
				break
			}
		}
		if !exists {
			err = dao.AddUserKUEp(user.UserID, KUEp)
			if err != nil {
				return pkt.NewErrorMsg(pkt.SERVER_ERROR, "AddUserKUEp ERR")
			}
			user.KUEp = append(user.KUEp, KUEp)
			keyNumber = len(user.KUEp) - 1
		}
	} else {
		if *h.m.UserId == -1 {
			user = &dao.User{UserID: int32(dao.GenerateUserID())}
		} else {
			user = &dao.User{UserID: *h.m.UserId}
		}
		user.KUEp = [][]byte{KUEp}
		user.Username = *h.m.Username
		err = dao.AddUser(user)
		if err != nil {
			return pkt.NewErrorMsg(pkt.SERVER_ERROR, "AddUser ERR")
		}
		keyNumber = 0
	}
	resp := &pkt.QueryUserResp{UserId: &user.UserID, KeyNumber: new(uint32)}
	*resp.KeyNumber = uint32(keyNumber)
	dao.AddUserCache(user.UserID, keyNumber, user)
	return resp
}

var NODELIST_CACHE = cache.New(1*time.Minute, 1*time.Minute)

type PreAllocNodeHandler struct {
	pkey string
	m    *pkt.PreAllocNodeReqV2
	user *dao.User
}

func (h *PreAllocNodeHandler) SetMessage(pubkey string, msg proto.Message) (*pkt.ErrorMessage, *int32) {
	h.pkey = pubkey
	req, ok := msg.(*pkt.PreAllocNodeReqV2)
	if ok {
		h.m = req
		if h.m.UserId == nil || h.m.SignData == nil || h.m.KeyNumber == nil {
			return pkt.NewErrorMsg(pkt.INVALID_ARGS, "Invalid request:Null value"), nil
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
			return pkt.NewError(pkt.INVALID_SIGNATURE), nil
		}
		return nil, READ_ROUTINE_NUM
	} else {
		return pkt.NewErrorMsg(pkt.INVALID_ARGS, "Invalid request"), nil
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
