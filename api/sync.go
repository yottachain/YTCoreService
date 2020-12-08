package api

import (
	"os"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/yottachain/YTCoreService/api/cache"
	"github.com/yottachain/YTCoreService/env"
	"github.com/yottachain/YTCoreService/pkt"
)

var SYNC_UP_CH chan int
var LoopSyncCond = sync.NewCond(new(sync.Mutex))
var SyncDoingList sync.Map

func initSyncUpPool() int {
	count := env.CheckInt(env.UploadBlockThreadNum/3, 10, 30)
	SYNC_UP_CH = make(chan int, count)
	for ii := 0; ii < count; ii++ {
		SYNC_UP_CH <- 1
	}
	return count
}

func NotifyLoop() {
	LoopSyncCond.Signal()
}

func isSyncDoing(key []byte) bool {
	_, ok := SyncDoingList.Load(string(key))
	return ok
}

func StartSync() {
	if env.StartSync == 0 {
		return
	}
	logrus.Infof("[SyncUpload]Start sync...\n")
	count := initSyncUpPool()
	go func() {
		for {
			time.Sleep(time.Duration(15) * time.Second)
			LoopSyncCond.Signal()
		}
	}()
	for {
		caches := cache.FindSyncObject(count*2, isSyncDoing)
		if len(caches) == 0 {
			LoopSyncCond.L.Lock()
			LoopSyncCond.Wait()
			LoopSyncCond.L.Unlock()
		} else {
			for _, ca := range caches {
				<-SYNC_UP_CH
				SyncDoingList.Store(string(ca), "")
				go syncUpload(ca)
			}
		}
	}
}

func syncUpload(key []byte) {
	defer func() {
		SYNC_UP_CH <- 1
		SyncDoingList.Delete(string(key))
	}()
	emsg := doSyncUpload(key)
	if emsg != nil {
		if emsg.Code == pkt.CODEC_ERROR || emsg.Code == pkt.INVALID_ARGS {
			cache.DeleteSyncObject(key)
		} else {
			time.Sleep(time.Duration(15) * time.Second)
		}
	} else {
		cache.DeleteSyncObject(key)
	}
}

func doSyncUpload(key []byte) *pkt.ErrorMessage {
	up, err := NewUploadObjectSync(key)
	if err != nil {
		return err
	}
	err = up.Upload()
	if err != nil {
		return err
	}
	os.Remove(up.decoder.GetPath())
	return nil
}
