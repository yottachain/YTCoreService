package handle

import (
	"bytes"
	"fmt"
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
)

var REG_PEER_CACHE = cache.New(5*time.Second, 5*time.Second)

type RegUserV3Handler struct {
	pkey string
	m    *pkt.RegUserReqV3
}

func (h *RegUserV3Handler) SetMessage(pubkey string, msg proto.Message) (*pkt.ErrorMessage, *int32, *int32) {
	h.pkey = pubkey
	lasttime, found := REG_PEER_CACHE.Get(h.pkey)
	if found {
		if time.Now().Unix()-lasttime.(int64) < 5 {
			return pkt.NewErrorMsg(pkt.TOO_MANY_CURSOR, "Too frequently"), nil, nil
		}
	}
	REG_PEER_CACHE.SetDefault(h.pkey, time.Now().Unix())
	req, ok := msg.(*pkt.RegUserReqV3)
	if ok {
		h.m = req
		if h.m.PubKey == nil || h.m.Username == nil || h.m.VersionId == nil || len(h.m.PubKey) == 0 {
			return pkt.NewErrorMsg(pkt.INVALID_ARGS, "Invalid request:Null value"), nil, nil
		}
		return nil, READ_ROUTINE_NUM, nil
	} else {
		return pkt.NewErrorMsg(pkt.INVALID_ARGS, "Invalid request"), nil, nil
	}
}

func (h *RegUserV3Handler) Handle() proto.Message {
	logrus.Infof("[RegUser]Name:%s\n", *h.m.Username)
	if env.S3Version != "" {
		if *h.m.VersionId == "" || bytes.Compare([]byte(*h.m.VersionId), []byte(env.S3Version)) < 0 {
			errmsg := fmt.Sprintf("[RegUser]Name:%s,ERR:TOO_LOW_VERSION?%s\n", *h.m.Username, *h.m.VersionId)
			logrus.Errorf(errmsg)
			return pkt.NewErrorMsg(pkt.TOO_LOW_VERSION, errmsg)
		}
	}
	queryUserResp, sn, err := RegUser(*h.m.Username, h.m.PubKey)
	if err != nil {
		return err
	}
	resp := &pkt.RegUserRespV2{SuperNodeNum: new(uint32),
		SuperNodeID: &sn.NodeID, SuperNodeAddrs: sn.Addrs,
		UserId: new(uint32),
	}
	*resp.SuperNodeNum = uint32(sn.ID)
	*resp.UserId = uint32(*queryUserResp.UserId)
	resp.KeyNumber = make([]int32, len(h.m.PubKey))
	for index, pk := range h.m.PubKey {
		resp.KeyNumber[index] = -1
		KUEp := base58.Decode(pk)
		for ii, pk := range queryUserResp.Pubkey {
			if bytes.Equal(pk, KUEp) {
				resp.KeyNumber[index] = int32(ii)
				break
			}
		}
	}
	return resp
}

type RegUserHandler struct {
	pkey string
	m    *pkt.RegUserReqV2
}

func (h *RegUserHandler) SetMessage(pubkey string, msg proto.Message) (*pkt.ErrorMessage, *int32, *int32) {
	h.pkey = pubkey
	lasttime, found := REG_PEER_CACHE.Get(h.pkey)
	if found {
		if time.Now().Unix()-lasttime.(int64) < 5 {
			return pkt.NewErrorMsg(pkt.TOO_MANY_CURSOR, "Too frequently"), nil, nil
		}
	}
	REG_PEER_CACHE.SetDefault(h.pkey, time.Now().Unix())
	req, ok := msg.(*pkt.RegUserReqV2)
	if ok {
		h.m = req
		if h.m.PubKey == nil || h.m.Username == nil || h.m.VersionId == nil {
			return pkt.NewErrorMsg(pkt.INVALID_ARGS, "Invalid request:Null value"), nil, nil
		}
		return nil, READ_ROUTINE_NUM, nil
	} else {
		return pkt.NewErrorMsg(pkt.INVALID_ARGS, "Invalid request"), nil, nil
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
	queryUserResp, sn, err := RegUser(*h.m.Username, []string{*h.m.PubKey})
	if err != nil {
		return err
	}
	resp := &pkt.RegUserResp{SuperNodeNum: new(uint32),
		SuperNodeID: &sn.NodeID, SuperNodeAddrs: sn.Addrs,
		UserId: new(uint32),
	}
	*resp.SuperNodeNum = uint32(sn.ID)
	*resp.UserId = uint32(*queryUserResp.UserId)
	KUEp := base58.Decode(*h.m.PubKey)
	for ii, pk := range queryUserResp.Pubkey {
		if bytes.Equal(pk, KUEp) {
			resp.KeyNumber = new(uint32)
			*resp.KeyNumber = uint32(ii)
			break
		}
	}
	if resp.KeyNumber == nil {
		logrus.Errorf("[RegUser]Pubkey %s sync err!\n", *h.m.PubKey)
		return pkt.NewError(pkt.SERVER_ERROR)
	}
	return resp
}

func RegUser(username string, pubkeys []string) (*pkt.QueryUserResp, *YTDNMgmt.SuperNode, *pkt.ErrorMessage) {
	sn := net.GetRegSuperNode(username)
	queryUserReqV2 := &pkt.QueryUserReqV2{
		Pubkey:   pubkeys,
		Username: &username,
		UserId:   new(int32),
	}
	var res proto.Message
	if sn.ID == int32(env.SuperNodeID) {
		handler := &QueryUserHandler{pkey: sn.PubKey, m: queryUserReqV2}
		res = handler.Handle()
		if serr, ok := res.(*pkt.ErrorMessage); ok {
			return nil, sn, serr
		}
	} else {
		var serr *pkt.ErrorMessage
		res, serr = net.RequestSN(queryUserReqV2, sn, "", 0, true)
		if serr != nil {
			return nil, sn, serr
		}
	}
	queryUserResp, ok := res.(*pkt.QueryUserResp)
	if !ok {
		logrus.Errorf("[RegUser]Return error type.\n")
		return nil, sn, pkt.NewErrorMsg(pkt.SERVER_ERROR, "Error type")
	}
	logrus.Infof("[RegUser][%s] is registered @ SN-%d,userID:%d\n", username, sn.ID, *queryUserResp.UserId)
	syncUserReq := &pkt.SyncUserReq{
		Pubkey:   queryUserResp.Pubkey,
		Username: &username,
		UserId:   queryUserResp.UserId,
	}
	syncres, err := SyncRequest(syncUserReq, int(sn.ID), 0)
	if err != nil {
		logrus.Errorf("[RegUser]SyncRequest err:%s\n", err)
		return nil, sn, pkt.NewErrorMsg(pkt.SERVER_ERROR, "Error type")
	}
	for _, snresp := range syncres {
		if snresp != nil {
			if snresp.Error() != nil {
				logrus.Errorf("[RegUser]Sync userinfo ERR:%d\n", snresp.Error().Code)
				return nil, sn, snresp.Error()
			}
		}
	}
	newsn := net.GetUserSuperNode(*queryUserResp.UserId)
	if newsn.ID != sn.ID {
		logrus.Errorf("[RegUser]SuperID inconsistency[%d!=%d]\n", newsn.ID, sn.ID)
		return nil, sn, pkt.NewError(pkt.SERVER_ERROR)
	}
	return queryUserResp, sn, nil
}

type QueryUserHandler struct {
	pkey string
	m    *pkt.QueryUserReqV2
}

func (h *QueryUserHandler) SetMessage(pubkey string, msg proto.Message) (*pkt.ErrorMessage, *int32, *int32) {
	h.pkey = pubkey
	req, ok := msg.(*pkt.QueryUserReqV2)
	if ok {
		h.m = req
		if h.m.Pubkey == nil || h.m.Username == nil || h.m.UserId == nil || len(h.m.Pubkey) == 0 {
			return pkt.NewErrorMsg(pkt.INVALID_ARGS, "Invalid request:Null value"), nil, nil
		}
		return nil, SYNC_ROUTINE_NUM, nil
	} else {
		return pkt.NewErrorMsg(pkt.INVALID_ARGS, "Invalid request"), nil, nil
	}
}

func (h *QueryUserHandler) Handle() proto.Message {
	_, err := net.AuthSuperNode(h.pkey)
	if err != nil {
		logrus.Errorf("[QueryUser]AuthSuper ERR:%s\n", err)
		return pkt.NewErrorMsg(pkt.INVALID_NODE_ID, err.Error())
	}
	logrus.Debugf("[QueryUser]User '%s' login request.\n", *h.m.Username)
	pubkeymap := make(map[string]bool)
	pass := false
	for _, pkey := range h.m.Pubkey {
		if net.AuthUserInfo(pkey, *h.m.Username, 3) {
			pubkeymap[pkey] = true
			pass = true
		} else {
			logrus.Warnf("[QueryUser]User %s failed to authenticate public key %s\n", *h.m.Username, pkey)
			pubkeymap[pkey] = false
		}
	}
	if !pass {
		logrus.Errorf("[QueryUser]User '%s' auth failed\n", *h.m.Username)
		return pkt.NewErrorMsg(pkt.SERVER_ERROR, "UserID invalid")
	}
	logrus.Infof("[QueryUser][%s] Certification passed.\n", *h.m.Username)
	user := dao.GetUserByUsername(*h.m.Username)
	if user != nil {
		for k, v := range pubkeymap {
			if v == false {
				continue
			}
			KUEp := base58.Decode(k)
			exist := false
			for _, pk := range user.KUEp {
				if bytes.Equal(pk, KUEp) {
					exist = true
					break
				}
			}
			if exist == false {
				err = dao.AddUserKUEp(user.UserID, KUEp)
				if err != nil {
					return pkt.NewErrorMsg(pkt.SERVER_ERROR, "AddUserKUEp ERR")
				}
				user.KUEp = append(user.KUEp, KUEp)
			}
		}
	} else {
		user = &dao.User{UserID: int32(dao.GenerateUserID())}
		user.KUEp = [][]byte{}
		user.Username = *h.m.Username
		for k, v := range pubkeymap {
			if v == false {
				continue
			}
			KUEp := base58.Decode(k)
			user.KUEp = append(user.KUEp, KUEp)
		}
		err = dao.AddUser(user)
		if err != nil {
			return pkt.NewErrorMsg(pkt.SERVER_ERROR, "AddUser ERR")
		}
	}
	dao.AddUserCache(user.UserID, user)
	resp := &pkt.QueryUserResp{UserId: &user.UserID}
	resp.Pubkey = user.KUEp
	return resp
}

type SyncUserHandler struct {
	pkey string
	m    *pkt.SyncUserReq
}

func (h *SyncUserHandler) SetMessage(pubkey string, msg proto.Message) (*pkt.ErrorMessage, *int32, *int32) {
	h.pkey = pubkey
	req, ok := msg.(*pkt.SyncUserReq)
	if ok {
		h.m = req
		if h.m.Pubkey == nil || h.m.Username == nil || h.m.UserId == nil || len(h.m.Pubkey) == 0 {
			return pkt.NewErrorMsg(pkt.INVALID_ARGS, "Invalid request:Null value"), nil, nil
		}
		return nil, SYNC_ROUTINE_NUM, nil
	} else {
		return pkt.NewErrorMsg(pkt.INVALID_ARGS, "Invalid request"), nil, nil
	}
}

func (h *SyncUserHandler) Handle() proto.Message {
	_, err := net.AuthSuperNode(h.pkey)
	if err != nil {
		logrus.Errorf("[SyncUser]AuthSuper ERR:%s\n", err)
		return pkt.NewErrorMsg(pkt.INVALID_NODE_ID, err.Error())
	}
	logrus.Debugf("[SyncUser]User '%s' sync request.\n", *h.m.Username)
	user := dao.GetUserByUserId(*h.m.UserId)
	if user != nil {
		eq := true
		if len(h.m.Pubkey) != len(user.KUEp) {
			eq = false
		}
		for index, bs := range h.m.Pubkey {
			if !bytes.Equal(user.KUEp[index], bs) {
				eq = false
				break
			}
		}
		if !eq {
			user.Username = *h.m.Username
			user.KUEp = h.m.Pubkey
			err = dao.UpdateUser(user)
			if err != nil {
				return pkt.NewErrorMsg(pkt.SERVER_ERROR, "UpdateUser ERR")
			}
		}
	} else {
		user = &dao.User{UserID: *h.m.UserId}
		user.KUEp = h.m.Pubkey
		user.Username = *h.m.Username
		err = dao.AddUser(user)
		if err != nil {
			return pkt.NewErrorMsg(pkt.SERVER_ERROR, "AddUser ERR")
		}
	}
	dao.AddUserCache(user.UserID, user)
	return &pkt.VoidResp{}
}
