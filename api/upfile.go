package api

import (
	"bytes"
	"fmt"
	"os"
	"path"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/yottachain/YTCoreService/api/cache"
	"github.com/yottachain/YTCoreService/codec"
	"github.com/yottachain/YTCoreService/env"
	"github.com/yottachain/YTCoreService/pkt"
)

var UPLOADING sync.Map

func PutUploadObject(userid int32, buck, key string, obj UploadObjectBase) {
	ss := fmt.Sprintf("%d/%s/%s", userid, buck, key)
	UPLOADING.Store(ss, obj)
}

func GetUploadObject(userid int32, buck, key string) UploadObjectBase {
	ss := fmt.Sprintf("%d/%s/%s", userid, buck, key)
	if vv, ok := UPLOADING.Load(ss); ok {
		return vv.(UploadObjectBase)
	}
	return nil
}

func DelUploadObject(userid int32, buck, key string) {
	ss := fmt.Sprintf("%d/%s/%s", userid, buck, key)
	UPLOADING.Delete(ss)
}

func checkCacheSize() bool {
	cachesize := cache.GetCacheSize()
	if cachesize > 1024*1024*1024 {
		time.Sleep(time.Duration(10) * time.Second)
	}
	if cachesize > env.MaxCacheSize {
		logrus.Errorf("[AyncUploadFile]Cache space overflow! ->%d\n", cachesize)
		return false
	}
	return true
}

func UploadMultiPartFile(userid int32, path []string, bucketname, key string) ([]byte, *pkt.ErrorMessage) {
	if !checkCacheSize() {
		return nil, pkt.NewErrorMsg(pkt.CACHE_FULL, "Cache space overflow")
	}
	enc, err := codec.NewMultiFileEncoder(path)
	if err != nil {
		logrus.Errorf("[UploadMultiPartFile]%s/%s,ERR:%s\n", bucketname, key, err)
		return nil, pkt.NewErrorMsg(pkt.INVALID_ARGS, err.Error())
	}
	defer enc.Close()
	k := &cache.Key{UserID: userid, Bucket: bucketname, ObjectName: key}
	v := cache.MultiPartFileValue(path, enc.GetLength(), enc.GetMD5())
	err = cache.InsertValue(k, v)
	if err != nil {
		logrus.Errorf("[UploadMultiPartFile]%s/%s,Insert cache ERR:%s\n", bucketname, key, err)
		return nil, pkt.NewErrorMsg(pkt.INVALID_ARGS, err.Error())
	}
	atomic.AddInt64(cache.CurCacheSize, enc.GetLength())
	logrus.Infof("[UploadMultiPartFile]%s/%s,Insert cache ok\n", bucketname, key)
	Notify()
	return enc.GetMD5(), nil
}

func UploadSingleFile(userid int32, path string, bucketname, key string) ([]byte, *pkt.ErrorMessage) {
	if !checkCacheSize() {
		return nil, pkt.NewErrorMsg(pkt.CACHE_FULL, "Cache space overflow")
	}
	enc, err := codec.NewFileEncoder(path)
	if err != nil {
		logrus.Errorf("[UploadSingleFile]%s/%s,ERR:%s\n", bucketname, key, err)
		return nil, pkt.NewErrorMsg(pkt.INVALID_ARGS, err.Error())
	}
	defer enc.Close()
	k := &cache.Key{UserID: userid, Bucket: bucketname, ObjectName: key}
	v := cache.SingleFileValue(path, enc.GetLength(), enc.GetMD5())
	err = cache.InsertValue(k, v)
	if err != nil {
		logrus.Errorf("[UploadSingleFile]%s/%s,Insert cache ERR:%s\n", bucketname, key, err)
		return nil, pkt.NewErrorMsg(pkt.INVALID_ARGS, err.Error())
	}
	atomic.AddInt64(cache.CurCacheSize, enc.GetLength())
	logrus.Infof("[UploadSingleFile]%s/%s,Insert cache ok\n", bucketname, key)
	Notify()
	return enc.GetMD5(), nil
}

func UploadBytesFile(userid int32, data []byte, bucketname, key string) ([]byte, *pkt.ErrorMessage) {
	if !checkCacheSize() {
		return nil, pkt.NewErrorMsg(pkt.CACHE_FULL, "Cache space overflow")
	}
	enc, err := codec.NewBytesEncoder(data)
	if err != nil {
		logrus.Errorf("[UploadBytesFile]%s/%s,ERR:%s\n", bucketname, key, err)
		return nil, pkt.NewErrorMsg(pkt.INVALID_ARGS, err.Error())
	}
	defer enc.Close()
	k := &cache.Key{UserID: userid, Bucket: bucketname, ObjectName: key}
	v := cache.BytesFileValue(data, enc.GetLength(), enc.GetMD5())
	err = cache.InsertValue(k, v)
	if err != nil {
		logrus.Errorf("[UploadBytesFile]%s/%s,Insert cache ERR:%s\n", bucketname, key, err)
		return nil, pkt.NewErrorMsg(pkt.INVALID_ARGS, err.Error())
	}
	atomic.AddInt64(cache.CurCacheSize, enc.GetLength())
	logrus.Infof("[UploadBytesFile]%s/%s,Insert cache ok\n", bucketname, key)
	Notify()
	return enc.GetMD5(), nil
}

func Delete(paths []string) {
	if paths != nil {
		dir := ""
		for _, p := range paths {
			p = strings.ReplaceAll(p, "\\", "/")
			dir = path.Dir(p)
			os.Remove(p)
		}
		os.Remove(dir)
	}
}

var CACHE_UP_CH chan int
var LoopCond = sync.NewCond(new(sync.Mutex))
var DoingList sync.Map

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

func IsDoing(key *cache.Key) bool {
	_, ok := DoingList.Load(key.ToString())
	return ok
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
		caches := cache.Find(count, IsDoing)
		if len(caches) == 0 {
			LoopCond.L.Lock()
			LoopCond.Wait()
			LoopCond.L.Unlock()
		} else {
			for _, ca := range caches {
				<-CACHE_UP_CH
				DoingList.Store(ca.K.ToString(), ca)
				go upload(ca)
			}
		}
	}
}

func upload(ca *cache.Cache) {
	defer func() {
		CACHE_UP_CH <- 1
		DoingList.Delete(ca.K.ToString())
	}()
	emsg := DoUpload(ca)
	if emsg != nil && (emsg.Code == pkt.CONN_ERROR || emsg.Code == pkt.INVALID_USER_ID || emsg.Code == pkt.SERVER_ERROR || emsg.Code == pkt.COMM_ERROR) {
		time.Sleep(time.Duration(15) * time.Second)
	} else {
		atomic.AddInt64(cache.CurCacheSize, -ca.V.Length)
		cache.DeleteValue(ca.K)
		Delete(ca.V.Path)
	}
}

func DoUpload(ca *cache.Cache) *pkt.ErrorMessage {
	c := GetClientById(uint32(ca.K.UserID))
	if c == nil {
		logrus.Errorf("[UploadToYotta]Client %d offline.\n", ca.K.UserID)
		return pkt.NewErrorMsg(pkt.INVALID_USER_ID, "Client offline")
	}
	var obj UploadObjectBase
	if env.Driver == "nas" {
		obj = NewUploadObjectToDisk(c)
	} else {
		obj = NewUploadObject(c)
	}
	PutUploadObject(int32(c.UserId), ca.K.Bucket, ca.K.ObjectName, obj)
	defer func() {
		DelUploadObject(int32(c.UserId), ca.K.Bucket, ca.K.ObjectName)
	}()
	var emsg *pkt.ErrorMessage = nil
	if ca.V.Type == 0 {
		emsg = obj.UploadBytes(ca.V.Data)
	} else if ca.V.Type == 0 {
		emsg = obj.UploadFile(ca.V.Path[0])
	} else {
		emsg = obj.UploadMultiFile(ca.V.Path)
	}
	if emsg != nil {
		return emsg
	}
	if !bytes.Equal(ca.V.Md5, obj.GetMD5()) {
		if ca.V.Type > 0 {
			logrus.Warnf("[UploadToYotta]%s,Md5 ERR.\n", ca.V.Path[0])
		} else {
			logrus.Warnf("[UploadToYotta]Md5 ERR.\n")
		}
	}
	if r, ok := obj.(*UploadObject); ok {
		meta := MetaTobytes(obj.GetLength(), obj.GetMD5())
		return c.NewObjectAccessor().CreateObject(ca.K.Bucket, ca.K.ObjectName, r.VNU, meta)
	} else {
		return nil
	}
}
