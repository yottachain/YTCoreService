package handle

import (
	"bytes"
	"fmt"
	"time"

	"github.com/aurawing/eos-go/btcsuite/btcutil/base58"
	"github.com/patrickmn/go-cache"
	"github.com/sirupsen/logrus"
	"github.com/yottachain/YTCoreService/dao"
	"github.com/yottachain/YTCoreService/env"
	"github.com/yottachain/YTCoreService/net"
	"github.com/yottachain/YTCoreService/net/eos"
	"github.com/yottachain/YTCoreService/pkt"
	"google.golang.org/protobuf/proto"
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
		if time.Now().Unix()-lasttime.(int64) < 1 {
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
	if env.Version != "" {
		if *h.m.VersionId == "" || bytes.Compare([]byte(*h.m.VersionId), []byte(env.Version)) < 0 {
			errmsg := fmt.Sprintf("[RegUser]Name:%s,ERR:TOO_LOW_VERSION?%s\n", *h.m.Username, *h.m.VersionId)
			logrus.Errorf(errmsg)
			return pkt.NewErrorMsg(pkt.TOO_LOW_VERSION, errmsg)
		}
	}
	user, err := RegUser(*h.m.Username, h.m.PubKey)
	if err != nil {
		return err
	}
	resp := &pkt.RegUserRespV2{SuperNodeNum: new(uint32),
		SuperNodeID: &net.SuperNode.NodeID, SuperNodeAddrs: net.SuperNode.Addrs,
		UserId: new(uint32),
	}
	*resp.SuperNodeNum = uint32(net.SuperNode.ID)
	*resp.UserId = uint32(user.UserID)
	resp.KeyNumber = make([]int32, len(h.m.PubKey))
	for index, pk := range h.m.PubKey {
		resp.KeyNumber[index] = -1
		KUEp := base58.Decode(pk)
		for ii, pk := range user.KUEp {
			if bytes.Equal(pk, KUEp) {
				resp.KeyNumber[index] = int32(ii)
				break
			}
		}
	}
	return resp
}

func RegUser(username string, pubkeys []string) (*dao.User, *pkt.ErrorMessage) {
	logrus.Debugf("[RegUser]User '%s' login request.\n", username)
	pubkeymap := make(map[string]bool)
	pass := false
	for _, pkey := range pubkeys {
		if eos.AuthUserInfo(pkey, username, 3) {
			pubkeymap[pkey] = true
			pass = true
		} else {
			logrus.Warnf("[RegUser]User %s failed to authenticate public key %s\n", username, pkey)
			pubkeymap[pkey] = false
		}
	}
	if !pass {
		logrus.Errorf("[RegUser]User '%s' auth failed\n", username)
		return nil, pkt.NewErrorMsg(pkt.SERVER_ERROR, "UserID invalid")
	}
	logrus.Infof("[RegUser][%s] Certification passed.\n", username)
	user := dao.GetUserByUsername(username)
	if user != nil {
		for k, v := range pubkeymap {
			if !v {
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
			if !exist {
				err := dao.AddUserKUEp(user.UserID, KUEp)
				if err != nil {
					return nil, pkt.NewErrorMsg(pkt.SERVER_ERROR, "AddUserKUEp ERR")
				}
				user.KUEp = append(user.KUEp, KUEp)
			}
		}
	} else {
		user = &dao.User{UserID: int32(dao.GenerateUserID())}
		user.KUEp = [][]byte{}
		user.Username = username
		for k, v := range pubkeymap {
			if !v {
				continue
			}
			KUEp := base58.Decode(k)
			user.KUEp = append(user.KUEp, KUEp)
		}
		err := dao.AddUser(user)
		if err != nil {
			return nil, pkt.NewErrorMsg(pkt.SERVER_ERROR, "AddUser ERR")
		}
	}
	return user, nil
}
