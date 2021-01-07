package api

import (
	"errors"
	"fmt"
	"time"

	"github.com/aurawing/eos-go/btcsuite/btcutil/base58"
	"github.com/sirupsen/logrus"
	"github.com/yottachain/YTCoreService/codec"
	"github.com/yottachain/YTCoreService/env"
	"github.com/yottachain/YTCoreService/net"
	"github.com/yottachain/YTCoreService/pkt"
	"github.com/yottachain/YTCrypto"
)

func createClient(users *env.UserInfo) (*Client, error) {
	keys := []*Key{}
	if users.Privkey == nil {
		return nil, errors.New("private key is nil")
	}
	for _, pkey := range users.Privkey {
		bs := base58.Decode(pkey)
		if len(bs) != 37 {
			return nil, errors.New("Invalid private key " + pkey)
		}
		aeskey := codec.GenerateUserKey(bs)
		pubkey, err := YTCrypto.GetPublicKeyByPrivateKey(pkey)
		if err != nil {
			return nil, errors.New("Invalid private key " + pkey)
		}
		k := &Key{PrivateKey: pkey, KUSp: bs, AESKey: aeskey, PublicKey: pubkey}
		keys = append(keys, k)
	}
	ii := int(time.Now().UnixNano() % int64(net.GetSuperNodeCount()))
	sn := net.GetSuperNode(ii)
	req := &pkt.RegUserReqV2{VersionId: &env.VersionID}
	res, err := net.RequestSN(req, sn, "", 0, false)
	if err != nil {
		emsg := fmt.Sprintf("User '%s' registration failed!%s", " c.Username", pkt.ToError(err))
		logrus.Errorf("[Regist]%s\n", emsg)
		return nil, errors.New(emsg)
	} else {
		resp, ok := res.(*pkt.RegUserResp)
		if ok {
			if resp.SuperNodeNum != nil && resp.UserId != nil && resp.KeyNumber != nil {
				if *resp.SuperNodeNum >= 0 && *resp.SuperNodeNum < uint32(net.GetSuperNodeCount()) {
					//	c.SuperNode = net.GetSuperNode(int(*resp.SuperNodeNum))
					//	c.UserId = *resp.UserId
					//	c.SignKey.KeyNumber = *resp.KeyNumber
					//	c.KeyMap[c.SignKey.KeyNumber] = c.SignKey
					//	logrus.Infof("[Regist]User '%s' registration successful,ID-KeyNumber:%d/%d,at sn %d\n",
					//		c.Username, c.UserId, c.SignKey.KeyNumber, c.SuperNode.ID)
					//	return c.SignKey.MakeSign(c.UserId)
				}
			}
		}
		logrus.Errorf("[Regist]Return err msg.\n")
		return nil, errors.New("Return err msg")
	}

}
