package api

import (
	"bytes"
	"fmt"
	"sync"
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
	logrus.Infof("[UploadBytesFile]%s/%s,Insert cache ok\n", bucketname, key)
	Notify()
	return enc.GetMD5(), nil
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
	if env.StartSync > 0 {
		return
	}
	count := initCACHEUpPool()
	go func() {
		for {
			if env.SyncMode == 0 {
				time.Sleep(time.Duration(120) * time.Second)
			} else {
				time.Sleep(time.Duration(15) * time.Second)
			}
			size := cache.GetCacheSize()
			if size > 0 {
				logrus.Infof("[AyncUpload]Cache size %d\n", cache.GetCacheSize())
			} else {
				cache.Clear()
			}
			LoopCond.Signal()
		}
	}()
	for {
		caches := cache.FindCache(count*2, IsDoing)
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
	emsg := doUpload(ca)
	if emsg != nil && !(emsg.Code == pkt.CODEC_ERROR || emsg.Code == pkt.INVALID_ARGS) {
		time.Sleep(time.Duration(15) * time.Second)
	} else {
		cache.CurCacheSize.Add(-ca.V.Length)
		cache.DeleteValue(ca.K)
		if emsg != nil {
			if ca.V.Type > 0 {
				logrus.Errorf("[AyncUpload]%s,Upload ERR:%s\n", ca.V.PathString(), pkt.ToError(emsg))
			} else {
				logrus.Errorf("[AyncUpload]Upload ERR:%s\n", pkt.ToError(emsg))
			}
		}
		cache.Delete(ca.V.Path)
	}
}

func doUpload(ca *cache.Cache) *pkt.ErrorMessage {
	c := GetClientById(uint32(ca.K.UserID))
	if c == nil {
		logrus.Errorf("[AyncUpload]Client %d offline.\n", ca.K.UserID)
		return pkt.NewErrorMsg(pkt.INVALID_USER_ID, "Client offline")
	}
	var obj UploadObjectBase
	if env.Driver == "nas" {
		obj = NewUploadObjectToDisk(c, ca.K.Bucket, ca.K.ObjectName)
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
	} else if ca.V.Type == 1 {
		emsg = obj.UploadFile(ca.V.Path[0])
	} else {
		emsg = obj.UploadMultiFile(ca.V.Path)
	}
	if emsg != nil {
		return emsg
	}
	if !bytes.Equal(ca.V.Md5, obj.GetMD5()) {
		if ca.V.Type > 0 {
			logrus.Warnf("[AyncUpload]%s,Md5 ERR.\n", ca.V.Path[0])
		} else {
			logrus.Warnf("[AyncUpload]Md5 ERR.\n")
		}
	}
	if r, ok := obj.(*UploadObject); ok {
		meta := MetaTobytes(obj.GetLength(), obj.GetMD5())
		err := c.NewObjectAccessor().CreateObject(ca.K.Bucket, ca.K.ObjectName, r.VNU, meta)
		if err != nil {
			logrus.Errorf("[AyncUpload]WriteMeta ERR:%s,%s/%s\n", pkt.ToError(err), ca.K.Bucket, ca.K.ObjectName)
			return err
		} else {
			logrus.Infof("[AyncUpload]WriteMeta OK,%s/%s\n", ca.K.Bucket, ca.K.ObjectName)
		}
	}
	return nil
}
