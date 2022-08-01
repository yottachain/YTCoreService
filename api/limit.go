package api

import (
	"sync"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/yottachain/YTCoreService/env"
)

const StatSize = 1640
const SuccessRate = 85

var Send_Stat *SendStat

type SendStat struct {
	sync.RWMutex
	delay    int64
	oktimes  int
	errtimes int
	routines int
}

func InitSendStat() {
	Send_Stat = &SendStat{}
	if env.DelayLine >= 0 {
		go Send_Stat.loop()
	}
}

func (q *SendStat) reset() {
	q.Lock()
	defer q.Unlock()
	q.errtimes = 0
	q.oktimes = 0
	q.delay = 0
}

func (q *SendStat) adderr() {
	q.Lock()
	defer q.Unlock()
	q.errtimes++
}

func (q *SendStat) addok(t int64) {
	q.Lock()
	defer q.Unlock()
	q.delay = q.delay + t
	q.oktimes++
}

func (q *SendStat) loop() {
	for {
		time.Sleep(time.Second * 5)
		if q.iscontinue() {
			continue
		} else {
			q.suit()
		}
	}
}

func (q *SendStat) suit() {
	timeout, rate := q.sum()
	curnum := env.UploadShardThreadNum - q.routines
	if rate < SuccessRate {
		logrus.Infof("[Limit]The rate of success:%d,The average time to send:%d,Current concurrency:%d", rate, timeout, curnum)
		if curnum <= 328 {
			return
		}
		decnum := curnum * 1 / 10
		if curnum-decnum < 328 {
			decnum = curnum - 328
		}
		for ii := 0; ii < decnum; ii++ {
			<-SHARD_UP_CH
			q.routines++
		}
		logrus.Infof("[Limit]Reduce %d concurrency", decnum)
		q.reset()
	} else {
		if timeout > int64(env.DelayLine) {
			if curnum <= 328 {
				return
			}
			<-SHARD_UP_CH
			q.routines++
			logrus.Tracef("[Limit]Reduce 1 concurrency,Current concurrency:%d", curnum)
		} else {
			if curnum >= env.UploadShardThreadNum {
				return
			}
			SHARD_UP_CH <- 1
			q.routines--
			logrus.Tracef("[Limit]Add 1 concurrency,Current concurrency:%d", curnum)
		}
	}
}

func (q *SendStat) iscontinue() bool {
	q.RLock()
	defer q.RUnlock()
	return q.oktimes < StatSize
}

func (q *SendStat) sum() (int64, int) {
	q.RLock()
	defer q.RUnlock()
	rate := q.oktimes * 100 / (q.oktimes + q.errtimes)
	return q.delay / int64(q.oktimes), rate
}

func SetOK(t int64) {
	if env.DelayLine == 0 {
		return
	}
	Send_Stat.addok(t)
}

func SetERR() {
	if env.DelayLine == 0 {
		return
	}
	Send_Stat.adderr()
}
