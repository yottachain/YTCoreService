package api

import (
	"sync"
	"time"

	"github.com/grd/statistics"
	"github.com/sirupsen/logrus"
	"github.com/yottachain/YTCoreService/env"
)

var ShardOKTimes []int64
var ShardLock sync.RWMutex

var BlockOKTimes []int64
var BlockLock sync.RWMutex

const maxSize = 10000 * 500

func StartUploadStat() {
	waittimes := 0
	for {
		time.Sleep(time.Second * 60)
		waittimes++
		ShardLock.RLock()
		count := len(ShardOKTimes)
		ShardLock.RUnlock()
		if count > maxSize {
			sum()
			waittimes = 0
			continue
		}
		if waittimes > 10 {
			sum()
			waittimes = 0
		}
	}
}

func sum() {
	ShardLock.Lock()
	defer ShardLock.Unlock()
	data := statistics.Int64(ShardOKTimes)
	count := len(ShardOKTimes)

	min, _, max, _ := statistics.Minmax(&data)
	avg := statistics.Mean(&data)
	variance := statistics.Variance(&data)

	bdata := statistics.Int64(BlockOKTimes)
	bcount := len(BlockOKTimes)
	bmin, _, bmax, _ := statistics.Minmax(&bdata)
	bavg := statistics.Mean(&bdata)
	bvariance := statistics.Variance(&bdata)
	logrus.Infof("[UploadStat]总分片数：%d,最短时间：%f,最长时间:%f,平均值:%f,方差:%f\n", count, min, max, avg, variance)
	logrus.Infof("[UploadStat]总块数：%d,最短时间：%f,最长时间:%f,平均值:%f,方差:%f\n", bcount, bmin, bmax, bavg, bvariance)
	if count > maxSize {
		ShardOKTimes = []int64{}
		BlockOKTimes = []int64{}
	}
}

func AddShardOK(t int64) {
	if !env.UploadStat {
		return
	}
	ShardLock.Lock()
	defer ShardLock.Unlock()
	ShardOKTimes = append(ShardOKTimes, t)
}

func AddBlockOK(t int64) {
	if !env.UploadStat {
		return
	}
	BlockLock.Lock()
	defer BlockLock.Unlock()
	BlockOKTimes = append(BlockOKTimes, t)
}
