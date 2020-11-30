package api

import (
	"crypto/md5"
	"errors"
	"fmt"
	"time"

	"github.com/aurawing/eos-go/btcsuite/btcutil/base58"
	"github.com/sirupsen/logrus"
	"github.com/yottachain/YTCoreService/api/cache"
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

func addClient(uid, keyNum uint32, signstr string) *Client {
	sn := net.GetUserSuperNode(int32(uid))
	priv, _ := ytcrypto.CreateKey()
	return &Client{UserId: uid, KeyNumber: keyNum, Sign: signstr, SuperNode: sn, AccessorKey: priv}
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

func (c *Client) syncUploadMultiPartFile(path []string, bucketname, key string) ([]byte, *pkt.ErrorMessage) {
	var up UploadObjectBase
	if env.Driver == "nas" {
		up = NewUploadObjectToDisk(c, bucketname, key)
	} else {
		up = NewUploadObject(c)
	}
	PutUploadObject(int32(c.UserId), bucketname, key, up)
	defer func() {
		DelUploadObject(int32(c.UserId), bucketname, key)
		Delete(path)
	}()
	err := up.UploadMultiFile(path)
	if err != nil {
		return nil, err
	}
	if r, ok := up.(*UploadObject); ok {
		meta := MetaTobytes(up.GetLength(), up.GetMD5())
		err = c.NewObjectAccessor().CreateObject(bucketname, key, r.VNU, meta)
		if err != nil {
			return nil, err
		}
	}
	return up.GetMD5(), nil
}

func (c *Client) UploadMultiPartFile(path []string, bucketname, key string) ([]byte, *pkt.ErrorMessage) {
	if env.SyncMode == 0 {
		return c.syncUploadMultiPartFile(path, bucketname, key)
	}
	md5, err := UploadMultiPartFile(int32(c.UserId), path, bucketname, key)
<<<<<<< HEAD
	if err != nil && err.Code == pkt.CACHE_FULL {
=======
	if err.Code == pkt.CACHE_FULL {
>>>>>>> 19b9ab2bdb839380ad97ecce750e9201008ad453
		return c.syncUploadMultiPartFile(path, bucketname, key)
	} else {
		return md5, err
	}
}

func (c *Client) syncUploadBytes(data []byte, bucketname, key string) ([]byte, *pkt.ErrorMessage) {
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
			return nil, err
		}
	}
	return up.GetMD5(), nil
}

func (c *Client) UploadBytes(data []byte, bucketname, key string) ([]byte, *pkt.ErrorMessage) {
	if env.SyncMode == 0 {
		return c.syncUploadBytes(data, bucketname, key)
	}
	md5, err := UploadBytesFile(int32(c.UserId), data, bucketname, key)
<<<<<<< HEAD
	if err != nil && err.Code == pkt.CACHE_FULL {
=======
	if err.Code == pkt.CACHE_FULL {
>>>>>>> 19b9ab2bdb839380ad97ecce750e9201008ad453
		return c.syncUploadBytes(data, bucketname, key)
	} else {
		return md5, err
	}
}

func (c *Client) UploadZeroFile(bucketname, key string) ([]byte, *pkt.ErrorMessage) {
	bs := md5.New().Sum(nil)
	meta := MetaTobytes(0, bs)
	err := c.NewObjectAccessor().CreateObject(bucketname, key, primitive.NewObjectID(), meta)
	if err != nil {
		return nil, err
	}
	return bs, nil
}

func (c *Client) syncUploadFile(path string, bucketname, key string) ([]byte, *pkt.ErrorMessage) {
	var up UploadObjectBase
	if env.Driver == "nas" {
		up = NewUploadObjectToDisk(c, bucketname, key)
	} else {
		up = NewUploadObject(c)
	}
	PutUploadObject(int32(c.UserId), bucketname, key, up)
	defer func() {
		DelUploadObject(int32(c.UserId), bucketname, key)
		Delete([]string{path})
	}()
	err := up.UploadFile(path)
	if err != nil {
		return nil, err
	}
	if r, ok := up.(*UploadObject); ok {
		meta := MetaTobytes(up.GetLength(), up.GetMD5())
		err = c.NewObjectAccessor().CreateObject(bucketname, key, r.VNU, meta)
		if err != nil {
			return nil, err
		}
	}
	return up.GetMD5(), nil
}

func (c *Client) UploadFile(path string, bucketname, key string) ([]byte, *pkt.ErrorMessage) {
	if env.SyncMode == 0 {
		return c.syncUploadFile(path, bucketname, key)
	}
	md5, err := UploadSingleFile(int32(c.UserId), path, bucketname, key)
<<<<<<< HEAD
	if err != nil && err.Code == pkt.CACHE_FULL {
=======
	if err.Code == pkt.CACHE_FULL {
>>>>>>> 19b9ab2bdb839380ad97ecce750e9201008ad453
		return c.syncUploadFile(path, bucketname, key)
	} else {
		return md5, err
	}
}

func FlushCache() {
	for {
		if cache.GetCacheSize() > 0 {
			time.Sleep(time.Duration(1) * time.Second)
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
