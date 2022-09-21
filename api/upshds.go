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
	ress      []*UploadShardResult
	ress2     []*UploadShardResult
	waitnum   int32
	oknum     int
	baknum    int
}

func NewUpLoad(logpre string, ress []*UploadShardResult, ress2 []*UploadShardResult, num int) *UpLoadShards {
	dns := &UpLoadShards{cancel: new(int32), logPrefix: logpre}
	dns.okSign = make(chan int, len(ress))
	dns.ress = ress
	dns.ress2 = ress2
	dns.waitnum = int32(num)
	dns.oknum = num
	*dns.cancel = 0
	return dns
}

func (upLoadShards *UpLoadShards) WaitComplete() error {
	for atomic.LoadInt32(&upLoadShards.waitnum) > 0 {
		atomic.AddInt32(&upLoadShards.waitnum, -1)
		sign := <-upLoadShards.okSign
		if sign < 0 {
			return errors.New("")
		}
	}
	return nil
}

func (upLoadShards *UpLoadShards) WaitUpload() error {
	size := int(upLoadShards.waitnum)
	timeout := time.After(time.Second * time.Duration(int64(env.BlkTimeout)))
	for ii := 0; ii < size; ii++ {
		select {
		case sign := <-upLoadShards.okSign:
			upLoadShards.waitnum--
			if sign < 0 {
				return errors.New("")
			}
		case <-timeout:
			return nil
		}
	}
	return nil
}

func (upLoadShards *UpLoadShards) OnResponse(rec *UploadShardResult, iserr bool) {
	upLoadShards.Lock()
	defer upLoadShards.Unlock()
	if iserr {
		upLoadShards.okSign <- -1
	} else {
		upLoadShards.okSign <- 1
	}
	if upLoadShards.ress[rec.SHARDID] == nil {
		if rec.NODE != nil {
			upLoadShards.ress[rec.SHARDID] = rec
			upLoadShards.oknum--
		}
	} else {
		if rec.NODE != nil {
			upLoadShards.ress2[rec.SHARDID] = rec
			upLoadShards.baknum++
		}
	}
	if upLoadShards.oknum == 0 {
		atomic.StoreInt32(upLoadShards.cancel, 1)
	}
}

func (upLoadShards *UpLoadShards) IsCancle() bool {
	return atomic.LoadInt32(upLoadShards.cancel) == 1
}
