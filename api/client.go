package api

import (
	"crypto/md5"
	"errors"
	"fmt"
	"time"

	"github.com/eoscanada/eos-go/btcsuite/btcutil/base58"
	"github.com/sirupsen/logrus"
	"github.com/yottachain/YTCoreService/api/cache"
	"github.com/yottachain/YTCoreService/codec"
	"github.com/yottachain/YTCoreService/env"
	"github.com/yottachain/YTCoreService/net"
	"github.com/yottachain/YTCoreService/pkt"
	"github.com/yottachain/YTCrypto"
	"github.com/yottachain/YTDNMgmt"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

type Key struct {
	PrivateKey string
	PublicKey  string
	KUSp       []byte
	AESKey     []byte
	KeyNumber  uint32
	Sign       string
}

func (c *Key) MakeSign(uid uint32) error {
	data := fmt.Sprintf("%d%d", uid, c.KeyNumber)
	s, err := YTCrypto.Sign(c.PrivateKey, []byte(data))
	if err != nil {
		logrus.Errorf("[Regist]Sign ERR:%s\n", err)
		return err
	} else {
		c.Sign = s
		return nil
	}
}

type Client struct {
	Username  string
	UserId    uint32
	SuperNode *YTDNMgmt.SuperNode

	SignKey  *Key
	StoreKey *Key
	KeyMap   map[uint32]*Key
}

func addClient(uid, keyNum uint32, signstr string) *Client {
	sn := net.GetUserSuperNode(int32(uid))
	pubv, _ := YTCrypto.CreateKey()
	k := &Key{KeyNumber: keyNum, Sign: signstr, PrivateKey: pubv, PublicKey: pubv}
	m := make(map[uint32]*Key)
	m[k.KeyNumber] = k
	return &Client{UserId: uid, SuperNode: sn, SignKey: k, StoreKey: k, KeyMap: m}
}

func newClient(uname string, privkey string) (*Client, error) {
	bs := base58.Decode(privkey)
	if len(bs) != 37 {
		return nil, errors.New("Invalid private key.")
	}
	aeskey := codec.GenerateUserKey(bs)
	pubkey, err := YTCrypto.GetPublicKeyByPrivateKey(privkey)
	if err != nil {
		return nil, err
	}
	k := &Key{PrivateKey: privkey, KUSp: bs, AESKey: aeskey, PublicKey: pubkey}
	m := make(map[uint32]*Key)
	c := &Client{Username: uname, SignKey: k, StoreKey: k, KeyMap: m}
	return c, nil
}

func (c *Client) Regist() error {
	ii := int(time.Now().UnixNano() % int64(net.GetSuperNodeCount()))
	sn := net.GetSuperNode(ii)
	req := &pkt.RegUserReqV2{Username: &c.Username, PubKey: &c.SignKey.PublicKey, VersionId: &env.VersionID}
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
					c.SignKey.KeyNumber = *resp.KeyNumber
					c.KeyMap[c.SignKey.KeyNumber] = c.SignKey
					logrus.Infof("[Regist]User '%s' registration successful,ID-KeyNumber:%d/%d,at sn %d\n",
						c.Username, c.UserId, c.SignKey.KeyNumber, c.SuperNode.ID)
					return c.SignKey.MakeSign(c.UserId)
				}
			}
		}
		logrus.Errorf("[Regist]Return err msg.\n")
		return errors.New("Return err msg")
	}
}

func (c *Client) GetProgress(bucketname, key string) int32 {
	v := GetUploadObject(int32(c.UserId), bucketname, key)
	if v != nil {
		return v.GetProgress()
	}
	vv := cache.GetValue(int32(c.UserId), bucketname, key)
	if vv != nil {
		return 0
	} else {
		return 100
	}
}

func (c *Client) SyncUploadMultiPartFile(path []string, bucketname, key string) ([]byte, *pkt.ErrorMessage) {
	var up UploadObjectBase
	if env.Driver == "nas" {
		up = NewUploadObjectToDisk(c, bucketname, key)
	} else {
		up = NewUploadObject(c)
	}
	PutUploadObject(int32(c.UserId), bucketname, key, up)
	defer func() {
		DelUploadObject(int32(c.UserId), bucketname, key)
		cache.Delete(path)
	}()
	err := up.UploadMultiFile(path)
	if err != nil {
		return nil, err
	}
	if r, ok := up.(*UploadObject); ok {
		meta := MetaTobytes(up.GetLength(), up.GetMD5())
		err = c.NewObjectAccessor().CreateObject(bucketname, key, r.VNU, meta)
		if err != nil {
			logrus.Errorf("[SyncUploadMultiPartFile]WriteMeta ERR:%s,%s/%s\n", pkt.ToError(err), bucketname, key)
			return nil, err
		} else {
			logrus.Infof("[SyncUploadMultiPartFile]WriteMeta OK,%s/%s\n", bucketname, key)
		}
	}
	return up.GetMD5(), nil
}

func (c *Client) UploadMultiPartFile(path []string, bucketname, key string) ([]byte, *pkt.ErrorMessage) {
	if env.SyncMode == 0 {
		return c.SyncUploadMultiPartFile(path, bucketname, key)
	}
	md5, err := UploadMultiPartFile(int32(c.UserId), path, bucketname, key)
	if err != nil && err.Code == pkt.CACHE_FULL {
		return c.SyncUploadMultiPartFile(path, bucketname, key)
	} else {
		return md5, err
	}
}

func (c *Client) SyncUploadBytes(data []byte, bucketname, key string) ([]byte, *pkt.ErrorMessage) {
	var up UploadObjectBase
	if env.Driver == "nas" {
		up = NewUploadObjectToDisk(c, bucketname, key)
	} else {
		up = NewUploadObject(c)
	}
	PutUploadObject(int32(c.UserId), bucketname, key, up)
	defer func() {
		DelUploadObject(int32(c.UserId), bucketname, key)
	}()
	err := up.UploadBytes(data)
	if err != nil {
		return nil, err
	}
	if r, ok := up.(*UploadObject); ok {
		meta := MetaTobytes(up.GetLength(), up.GetMD5())
		err = c.NewObjectAccessor().CreateObject(bucketname, key, r.VNU, meta)
		if err != nil {
			logrus.Errorf("[SyncUploadBytes]WriteMeta ERR:%s,%s/%s\n", pkt.ToError(err), bucketname, key)
			return nil, err
		} else {
			logrus.Infof("[SyncUploadBytes]WriteMeta OK,%s/%s\n", bucketname, key)
		}
	}
	return up.GetMD5(), nil
}

func (c *Client) UploadBytes(data []byte, bucketname, key string) ([]byte, *pkt.ErrorMessage) {
	if env.SyncMode == 0 {
		return c.SyncUploadBytes(data, bucketname, key)
	}
	md5, err := UploadBytesFile(int32(c.UserId), data, bucketname, key)
	if err != nil && err.Code == pkt.CACHE_FULL {
		return c.SyncUploadBytes(data, bucketname, key)
	} else {
		return md5, err
	}
}

func (c *Client) UploadZeroFile(bucketname, key string) ([]byte, *pkt.ErrorMessage) {
	bs := md5.New().Sum(nil)
	meta := MetaTobytes(0, bs)
	err := c.NewObjectAccessor().CreateObject(bucketname, key, env.ZeroLenFileID(), meta)
	if err != nil {
		return nil, err
	}
	return bs, nil
}

func (c *Client) SyncUploadFile(path string, bucketname, key string) ([]byte, *pkt.ErrorMessage) {
	var up UploadObjectBase
	if env.Driver == "nas" {
		up = NewUploadObjectToDisk(c, bucketname, key)
	} else {
		up = NewUploadObject(c)
	}
	PutUploadObject(int32(c.UserId), bucketname, key, up)
	defer func() {
		DelUploadObject(int32(c.UserId), bucketname, key)
		cache.Delete([]string{path})
	}()
	err := up.UploadFile(path)
	if err != nil {
		return nil, err
	}
	if r, ok := up.(*UploadObject); ok {
		meta := MetaTobytes(up.GetLength(), up.GetMD5())
		err = c.NewObjectAccessor().CreateObject(bucketname, key, r.VNU, meta)
		if err != nil {
			logrus.Errorf("[SyncUploadFile]WriteMeta ERR:%s,%s/%s\n", pkt.ToError(err), bucketname, key)
			return nil, err
		} else {
			logrus.Infof("[SyncUploadFile]WriteMeta OK,%s/%s\n", bucketname, key)
		}
	}
	return up.GetMD5(), nil
}

func (c *Client) UploadFile(path string, bucketname, key string) ([]byte, *pkt.ErrorMessage) {
	if env.SyncMode == 0 {
		return c.SyncUploadFile(path, bucketname, key)
	}
	md5, err := UploadSingleFile(int32(c.UserId), path, bucketname, key)
	if err != nil && err.Code == pkt.CACHE_FULL {
		return c.SyncUploadFile(path, bucketname, key)
	} else {
		return md5, err
	}
}

func FlushCache() {
	for {
		if cache.GetCacheSize() > 0 {
			time.Sleep(time.Duration(5) * time.Second)
		} else {
			break
		}
	}
}

func (c *Client) NewUploadObject() *UploadObject {
	return NewUploadObject(c)
}

func (c *Client) NewDownloadObject(vhw []byte) (*DownloadObject, *pkt.ErrorMessage) {
	do := &DownloadObject{UClient: c, Progress: &DownProgress{}}
	err := do.InitByVHW(vhw)
	if err != nil {
		return nil, err
	} else {
		return do, nil
	}
}

func (c *Client) NewDownloadFile(bucketName, filename string, version primitive.ObjectID) (*DownloadObject, *pkt.ErrorMessage) {
	do := &DownloadObject{UClient: c, Progress: &DownProgress{}}
	err := do.InitByKey(bucketName, filename, version)
	if err != nil {
		return nil, err
	} else {
		return do, nil
	}
}

func (c *Client) NewObjectMeta(bucketName, filename string, version primitive.ObjectID) (*ObjectInfo, *pkt.ErrorMessage) {
	return NewObjectMeta(c, bucketName, filename, version)
}

func (c *Client) NewBucketAccessor() *BucketAccessor {
	return &BucketAccessor{UClient: c}
}

func (c *Client) NewObjectAccessor() *ObjectAccessor {
	return &ObjectAccessor{UClient: c}
}
