package api

import (
	"errors"
	"fmt"
	"time"

	"github.com/aurawing/eos-go/btcsuite/btcutil/base58"
	"github.com/sirupsen/logrus"
	"github.com/yottachain/YTCoreService/codec"
	"github.com/yottachain/YTCoreService/net"
	"github.com/yottachain/YTCoreService/pkt"
	ytcrypto "github.com/yottachain/YTCrypto"
	"github.com/yottachain/YTDNMgmt"
)

type Client struct {
	Username    string
	PrivateKey  string
	KUSp        []byte
	AESKey      []byte
	UserId      int32
	KeyNumber   int32
	SuperNode   *YTDNMgmt.SuperNode
	AccessorKey string
}

func newClient(uname string, privkey string) (*Client, error) {
	bs := base58.Decode(privkey)
	if len(bs) != 37 {
		return nil, errors.New("Invalid private key.")
	}
	aeskey := codec.GenerateUserKey(bs)
	pubkey, err := ytcrypto.GetPublicKeyByPrivateKey(privkey)
	if err != nil {
		return nil, err
	}
	c := &Client{Username: uname, PrivateKey: privkey, KUSp: bs, AESKey: aeskey, AccessorKey: pubkey}
	return c, nil
}

func (c *Client) Regist() error {
	ii := int(time.Now().Unix() % int64(net.GetSuperNodeCount()))
	sn := net.GetSuperNode(ii)
	req := &pkt.RegUserReqV2{Username: &c.Username, PubKey: &c.AccessorKey, VersionId: &VersionID}
	res, err := net.RequestSN(req, sn, "", 0, false)
	if err != nil {
		emsg := fmt.Sprintf("User '%s' registration failed:%d-%s", c.Username, err.GetCode(), err.GetMsg())
		logrus.Errorf("[Regist]%s\n", emsg)
		return errors.New(emsg)
	} else {
		resp, ok := res.(*pkt.RegUserResp)
		if ok {
			if resp.SuperNodeNum != nil && resp.UserId != nil && resp.KeyNumber != nil {
				if *resp.SuperNodeNum > 0 && *resp.SuperNodeNum < uint32(net.GetSuperNodeCount()) {
					c.SuperNode = net.GetSuperNode(int(*resp.SuperNodeNum))
					c.UserId = int32(*resp.UserId)
					c.KeyNumber = int32(*resp.KeyNumber)
					logrus.Infof("[Regist]User '%s' Registration Successful,ID-KeyNumber:%d-%d\n", c.Username, c.UserId, c.KeyNumber)
					return nil
				}
			}
		}
		logrus.Errorf("[Regist]Return err msg.\n")
		return errors.New("Return err msg")
	}
}
