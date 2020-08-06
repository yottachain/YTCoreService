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
	ytcrypto "github.com/yottachain/YTCrypto"
	"github.com/yottachain/YTDNMgmt"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

type Client struct {
	Username    string
	PrivateKey  string
	KUSp        []byte
	AESKey      []byte
	UserId      uint32
	KeyNumber   uint32
	SuperNode   *YTDNMgmt.SuperNode
	AccessorKey string
	Sign        string
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
	ii := int(time.Now().UnixNano() % int64(net.GetSuperNodeCount()))
	sn := net.GetSuperNode(ii)
	req := &pkt.RegUserReqV2{Username: &c.Username, PubKey: &c.AccessorKey, VersionId: &env.VersionID}
	res, err := net.RequestSN(req, sn, "", 0, false)
	if err != nil {
		emsg := fmt.Sprintf("User '%s' registration failed!%s", c.Username, pkt.ToError(err))
		logrus.Errorf("[Regist]%s\n", emsg)
		return errors.New(emsg)
	} else {
		resp, ok := res.(*pkt.RegUserResp)
		if ok {
			if resp.SuperNodeNum != nil && resp.UserId != nil && resp.KeyNumber != nil {
				if *resp.SuperNodeNum >= 0 && *resp.SuperNodeNum < uint32(net.GetSuperNodeCount()) {
					c.SuperNode = net.GetSuperNode(int(*resp.SuperNodeNum))
					c.UserId = *resp.UserId
					c.KeyNumber = *resp.KeyNumber
					logrus.Infof("[Regist]User '%s' registration successful,ID-KeyNumber:%d/%d,at sn %d\n",
						c.Username, c.UserId, c.KeyNumber, c.SuperNode.ID)
					return c.MakeSign()
				}
			}
		}
		logrus.Errorf("[Regist]Return err msg.\n")
		return errors.New("Return err msg")
	}
}

func (c *Client) MakeSign() error {
	data := fmt.Sprintf("%d%d", c.UserId, c.KeyNumber)
	s, err := YTCrypto.Sign(c.PrivateKey, []byte(data))
	if err != nil {
		logrus.Errorf("[Regist]Sign ERR:%s\n", err)
		return err
	} else {
		c.Sign = s
		return nil
	}
}

func (c *Client) NewUploadObject() *UploadObject {
	return NewUploadObject(c)
}

func (c *Client) NewDownloadObject(vhw []byte) (*DownloadObject, *pkt.ErrorMessage) {
	do := &DownloadObject{UClient: c}
	err := do.InitByVHW(vhw)
	if err != nil {
		return nil, err
	} else {
		return do, nil
	}
}

func (c *Client) NewDownloadBytes(bucketName, filename string, version primitive.ObjectID) (*DownloadObject, *pkt.ErrorMessage) {
	do := &DownloadObject{UClient: c}
	err := do.InitByKey(bucketName, filename, version)
	if err != nil {
		return nil, err
	} else {
		return do, nil
	}
}
