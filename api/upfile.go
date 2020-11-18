package api

import (
	"bytes"
	"fmt"
	"os"
	"path"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/yottachain/YTCoreService/codec"
	"github.com/yottachain/YTCoreService/env"
	"github.com/yottachain/YTCoreService/pkt"
)

var UPLOADING sync.Map
var CurCacheSize *int64 = new(int64)

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
		time.Sleep(time.Duration(10) * time.Second)
		logrus.Errorf("[UploadMultiPartFile]Cache space overflow! ->%d\n", cachesize)
		return nil, pkt.NewErrorMsg(pkt.CACHE_FULL, "Cache space overflow")
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
	Notify()
	return enc.GetMD5(), nil
}

func UploadSingleFile(userid int32, path string, bucketname, key string) ([]byte, *pkt.ErrorMessage) {
	cachesize := atomic.LoadInt64(CurCacheSize)
	if cachesize > env.MaxCacheSize {
		time.Sleep(time.Duration(10) * time.Second)
		logrus.Errorf("[UploadSingleFile]Cache space overflow! ->%d\n", cachesize)
		return nil, pkt.NewErrorMsg(pkt.CACHE_FULL, "Cache space overflow")
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
	Notify()
	return enc.GetMD5(), nil
}

func UploadBytesFile(userid int32, data []byte, bucketname, key string) ([]byte, *pkt.ErrorMessage) {
	cachesize := atomic.LoadInt64(CurCacheSize)
	if cachesize > env.MaxCacheSize {
		time.Sleep(time.Duration(10) * time.Second)
		logrus.Errorf("[UploadBytesFile]Cache space overflow! ->%d\n", cachesize)
		return nil, pkt.NewErrorMsg(pkt.CACHE_FULL, "Cache space overflow")
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
	Notify()
	return enc.GetMD5(), nil
}

func Delete(paths []string) {
	if paths != nil {
		dir := ""
		for _, p := range paths {
			dir = path.Base(p)
			os.Remove(p)
		}
		os.Remove(dir)
	}
}

var CACHE_UP_CH chan int
var LoopCond = sync.NewCond(new(sync.Mutex))

func initCACHEUpPool() int {
	count := env.CheckInt(env.UploadBlockThreadNum/3, 10, 30)
	CACHE_UP_CH = make(chan int, count)
	for ii := 0; ii < count; ii++ {
		CACHE_UP_CH <- 1
	}
	return count
}

func Notify() {
	LoopCond.Signal()
}

func DoCache() {
	count := initCACHEUpPool()
	go func() {
		for {
			time.Sleep(time.Duration(15) * time.Second)
			LoopCond.Signal()
		}
	}()
	for {
		caches := Find(count)
		if len(caches) == 0 {
			cond.L.Lock()
			cond.Wait()
			cond.L.Unlock()
		} else {
			for _, ca := range caches {
				<-CACHE_UP_CH
				go upload(ca)
			}
		}
	}
}

func upload(cache *Cache) {
	defer func() {
		CACHE_UP_CH <- 1
	}()
	var emsg *pkt.ErrorMessage = nil
	if env.SyncMode == 1 {
		emsg = uploadToYotta(cache)
	} else {
		//emsg = uploadToYotta(cache)
	}
	if emsg == nil || (emsg != nil && emsg.Code == pkt.INVALID_ARGS) {
		atomic.AddInt64(CurCacheSize, -cache.V.Length)
		DeleteValue(cache.K)
		Delete(cache.V.Path)
	}
}

func uploadToYotta(cache *Cache) *pkt.ErrorMessage {
	c := GetClientById(uint32(cache.K.UserID))
	if c == nil {
		logrus.Errorf("[UploadToYotta]Client %d offline.\n", cache.K.UserID)
		return pkt.NewErrorMsg(pkt.INVALID_ARGS, "Client offline")
	}
	obj := NewUploadObject(c)
	PutUploadObject(int32(c.UserId), cache.K.Bucket, cache.K.ObjectName, obj)
	defer func() {
		DelUploadObject(int32(c.UserId), cache.K.Bucket, cache.K.ObjectName)
	}()
	var emsg *pkt.ErrorMessage = nil
	if cache.V.Type == 0 {
		emsg = obj.UploadBytes(cache.V.Data)
	} else if cache.V.Type == 0 {
		emsg = obj.UploadFile(cache.V.Path[0])
	} else {
		emsg = obj.UploadMultiFile(cache.V.Path)
	}
	if emsg != nil {
		return emsg
	}
	if !bytes.Equal(cache.V.Md5, obj.GetMD5()) {
		if cache.V.Type > 0 {
			logrus.Warnf("[UploadToYotta]%s,Md5 ERR.\n", cache.V.Path[0])
		} else {
			logrus.Warnf("[UploadToYotta]Md5 ERR.\n")
		}
	}
	meta := MetaTobytes(obj.GetLength(), obj.GetMD5())
	return c.NewObjectAccessor().CreateObject(cache.K.Bucket, cache.K.ObjectName, obj.VNU, meta)
}
