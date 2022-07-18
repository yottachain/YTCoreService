package api

import (
	"bytes"
	"crypto/md5"
	"crypto/sha256"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/yottachain/YTCoreService/api/cache"
	"github.com/yottachain/YTCoreService/env"
	"github.com/yottachain/YTCoreService/pkt"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

type Client struct {
	Username string
	UserId   uint32

	SignKey  *Key
	StoreKey *Key
	KeyMap   map[uint32]*Key
}

func (c *Client) GetKey(pubkeyhash []byte) *Key {
	for _, k := range c.KeyMap {
		bs := sha256.Sum256([]byte(k.PublicKey))
		if bytes.Equal(bs[:], pubkeyhash) {
			return k
		}
	}
	return nil
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
		logrus.Errorf("[UploadZeroFile]WriteMeta ERR:%s,%s/%s\n", pkt.ToError(err), bucketname, key)
		return nil, err
	} else {
		logrus.Infof("[UploadZeroFile]WriteMeta OK,%s/%s\n", bucketname, key)
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

func (c *Client) NewDownloadLastVersion(bucketName, filename string) (*DownloadObject, *pkt.ErrorMessage) {
	return c.NewDownloadFile(bucketName, filename, primitive.NilObjectID)
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

func (c *Client) UploadPreEncode(bucketname, objectname string) *UploadObjectToDisk {
	return NewUploadObjectToDisk(c, bucketname, objectname)
}

func (c *Client) DownloadToSGX(bucketName, filename string) (*DownloadForSGX, *pkt.ErrorMessage) {
	return c.DownloadToSGXWVer(bucketName, filename, primitive.NilObjectID)
}

func (c *Client) DownloadToSGXWVer(bucketName, filename string, version primitive.ObjectID) (*DownloadForSGX, *pkt.ErrorMessage) {
	do := &DownloadForSGX{}
	do.UClient = c
	do.Progress = &DownProgress{}
	err := do.InitByKey(bucketName, filename, version)
	if err != nil {
		return nil, err
	} else {
		do.GetRefers()
		return do, nil
	}
}

func (c *Client) ImportAuth(bucketName, filename string) *AuthImporter {
	do := &AuthImporter{UClient: c}
	do.bucketName = bucketName
	do.filename = filename
	return do
}

func (c *Client) ExportAuth(bucketName, filename string) (*AuthExporter, *pkt.ErrorMessage) {
	return c.ExportAuthByVer(bucketName, filename, primitive.NilObjectID)
}

func (c *Client) ExportAuthByVer(bucketName, filename string, version primitive.ObjectID) (*AuthExporter, *pkt.ErrorMessage) {
	do := &AuthExporter{UClient: c}
	err := do.InitByKey(bucketName, filename, version)
	if err != nil {
		return nil, err
	} else {
		return do, nil
	}
}

func (c *Client) Auth(bucketName, filename string) (*Auth, *pkt.ErrorMessage) {
	return c.AuthByVer(bucketName, filename, primitive.NilObjectID)
}

func (c *Client) AuthByVer(bucketName, filename string, version primitive.ObjectID) (*Auth, *pkt.ErrorMessage) {
	do := &Auth{}
	do.UClient = c
	err := do.InitByKey(bucketName, filename, version)
	if err != nil {
		return nil, err
	} else {
		do.Bucket = bucketName
		do.Key = filename
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
