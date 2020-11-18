package api

import (
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/sirupsen/logrus"
	"github.com/yottachain/YTCoreService/codec"
	"github.com/yottachain/YTCoreService/env"
	"github.com/yottachain/YTCoreService/pkt"
)

var UPLOADING sync.Map
var CurCacheSize *int64

func PutUploadObject(userid int32, buck, key string, obj *UploadObject) {
	ss := fmt.Sprintf("%d/%s/%s", userid, buck, key)
	UPLOADING.Store(ss, obj)
}

func GetUploadObject(userid int32, buck, key string) *UploadObject {
	ss := fmt.Sprintf("%d/%s/%s", userid, buck, key)
	if vv, ok := UPLOADING.Load(ss); ok {
		return vv.(*UploadObject)
	}
	return nil
}

func DelUploadObject(userid int32, buck, key string) {
	ss := fmt.Sprintf("%d/%s/%s", userid, buck, key)
	UPLOADING.Delete(ss)
}

func UploadMultiPartFile(userid int32, path []string, bucketname, key string) ([]byte, *pkt.ErrorMessage) {
	cachesize := atomic.LoadInt64(CurCacheSize)
	if cachesize > env.MaxCacheSize {
		logrus.Errorf("[UploadMultiPartFile]Cache space overflow! ->%d\n", cachesize)
		return nil, pkt.NewErrorMsg(pkt.INVALID_ARGS, "Cache space overflow")
	}
	enc, err := codec.NewMultiFileEncoder(path)
	if err != nil {
		logrus.Errorf("[UploadMultiPartFile]%s/%s,ERR:%s\n", bucketname, key, err)
		return nil, pkt.NewErrorMsg(pkt.INVALID_ARGS, err.Error())
	}
	defer enc.Close()
	k := &Key{UserID: userid, Bucket: bucketname, ObjectName: key}
	v := MultiPartFileValue(path, enc.GetLength(), enc.GetMD5())
	err = InsertValue(k, v)
	if err != nil {
		logrus.Errorf("[UploadMultiPartFile]%s/%s,Insert cache ERR:%s\n", bucketname, key, err)
		return nil, pkt.NewErrorMsg(pkt.INVALID_ARGS, err.Error())
	}
	atomic.AddInt64(CurCacheSize, enc.GetLength())
	logrus.Infof("[UploadMultiPartFile]%s/%s,Insert cache ok\n", bucketname, key)
	return enc.GetMD5(), nil
}

func UploadSingleFile(userid int32, path string, bucketname, key string) ([]byte, *pkt.ErrorMessage) {
	cachesize := atomic.LoadInt64(CurCacheSize)
	if cachesize > env.MaxCacheSize {
		logrus.Errorf("[UploadSingleFile]Cache space overflow! ->%d\n", cachesize)
		return nil, pkt.NewErrorMsg(pkt.INVALID_ARGS, "Cache space overflow")
	}
	enc, err := codec.NewFileEncoder(path)
	if err != nil {
		logrus.Errorf("[UploadSingleFile]%s/%s,ERR:%s\n", bucketname, key, err)
		return nil, pkt.NewErrorMsg(pkt.INVALID_ARGS, err.Error())
	}
	defer enc.Close()
	k := &Key{UserID: userid, Bucket: bucketname, ObjectName: key}
	v := SingleFileValue(path, enc.GetLength(), enc.GetMD5())
	err = InsertValue(k, v)
	if err != nil {
		logrus.Errorf("[UploadSingleFile]%s/%s,Insert cache ERR:%s\n", bucketname, key, err)
		return nil, pkt.NewErrorMsg(pkt.INVALID_ARGS, err.Error())
	}
	atomic.AddInt64(CurCacheSize, enc.GetLength())
	logrus.Infof("[UploadSingleFile]%s/%s,Insert cache ok\n", bucketname, key)
	return enc.GetMD5(), nil
}

func UploadBytesFile(userid int32, data []byte, bucketname, key string) ([]byte, *pkt.ErrorMessage) {
	cachesize := atomic.LoadInt64(CurCacheSize)
	if cachesize > env.MaxCacheSize {
		logrus.Errorf("[UploadBytesFile]Cache space overflow! ->%d\n", cachesize)
		return nil, pkt.NewErrorMsg(pkt.INVALID_ARGS, "Cache space overflow")
	}
	enc, err := codec.NewBytesEncoder(data)
	if err != nil {
		logrus.Errorf("[UploadBytesFile]%s/%s,ERR:%s\n", bucketname, key, err)
		return nil, pkt.NewErrorMsg(pkt.INVALID_ARGS, err.Error())
	}
	defer enc.Close()
	k := &Key{UserID: userid, Bucket: bucketname, ObjectName: key}
	v := BytesFileValue(data, enc.GetLength(), enc.GetMD5())
	err = InsertValue(k, v)
	if err != nil {
		logrus.Errorf("[UploadBytesFile]%s/%s,Insert cache ERR:%s\n", bucketname, key, err)
		return nil, pkt.NewErrorMsg(pkt.INVALID_ARGS, err.Error())
	}
	atomic.AddInt64(CurCacheSize, enc.GetLength())
	logrus.Infof("[UploadBytesFile]%s/%s,Insert cache ok\n", bucketname, key)
	return enc.GetMD5(), nil
}
