package api

import (
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/yottachain/YTCoreService/env"
)

type UpLoadShards struct {
	sync.RWMutex
	cancel    *int32
	logPrefix string
	okSign    chan int
	bakSign   chan int
	bakcount  int
	waitcount int
	ress      []*UploadShardResult
	ress2     []*UploadShardResult
	count     int
}

func NewUpLoad(logpre string, ress []*UploadShardResult, ress2 []*UploadShardResult, chansize, chansize2, chansize3 int) *UpLoadShards {
	dns := &UpLoadShards{cancel: new(int32), logPrefix: logpre}
	dns.okSign = make(chan int, chansize)
	dns.ress = ress
	dns.ress2 = ress2
	dns.bakcount = chansize2
	dns.waitcount = chansize3
	if chansize2 > 0 {
		dns.bakSign = make(chan int, chansize2+chansize3)
	}
	*dns.cancel = 0
	return dns
}

func (upLoadShards *UpLoadShards) WaitUpload() error {
	startTime := time.Now().Unix()
	size := len(upLoadShards.ress)
	for ii := 0; ii < size; ii++ {
		sign := <-upLoadShards.okSign
		if sign < 0 {
			return errors.New("")
		}
	}
	for ii := 0; ii < upLoadShards.bakcount; ii++ {
		sign := <-upLoadShards.bakSign
		if sign < 0 {
			return errors.New("")
		}
	}
	t := int64(env.BlkTimeout) - (time.Now().Unix() - startTime)
	if t <= 0 {
		atomic.StoreInt32(upLoadShards.cancel, 1)
		return nil
	}
	timeout := time.After(time.Second * time.Duration(t))
	for ii := 0; ii < upLoadShards.waitcount; ii++ {
		select {
		case <-upLoadShards.bakSign:
		case <-timeout:
			atomic.StoreInt32(upLoadShards.cancel, 1)
			return nil
		}
	}
	atomic.StoreInt32(upLoadShards.cancel, 1)
	return nil
}

func (upLoadShards *UpLoadShards) Count() int {
	upLoadShards.RLock()
	defer upLoadShards.RUnlock()
	return upLoadShards.count
}

func (upLoadShards *UpLoadShards) OnResponse(rec *UploadShardResult) {
	upLoadShards.Lock()
	defer upLoadShards.Unlock()
	if upLoadShards.ress[rec.SHARDID] == nil {
		if rec.NODE == nil {
			upLoadShards.okSign <- -1
		} else {
			upLoadShards.ress[rec.SHARDID] = rec
			upLoadShards.okSign <- 1
			upLoadShards.count++
		}
	} else {
		if rec.NODE == nil {
			upLoadShards.bakSign <- -1
		} else {
			upLoadShards.ress2[rec.SHARDID] = rec
			upLoadShards.bakSign <- 1
			upLoadShards.count++
		}
	}
}

func (upLoadShards *UpLoadShards) IsCancle() bool {
	return atomic.LoadInt32(upLoadShards.cancel) == 1
}
