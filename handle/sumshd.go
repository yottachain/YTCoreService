package handle

import (
	"time"

	"github.com/sirupsen/logrus"
	"github.com/yottachain/YTCoreService/dao"
	"github.com/yottachain/YTCoreService/env"
	"github.com/yottachain/YTCoreService/net"
)

const DELAY_TIMES = 60 * 5

var firstId int64

func StartIterateShards() {
	for {
		if !net.IsActive() {
			time.Sleep(time.Duration(30) * time.Second)
			continue
		}
		if FindFirstId() {
			break
		}
	}
	for {
		if !net.IsActive() {
			time.Sleep(time.Duration(30) * time.Second)
			continue
		}
		Iterate()
	}
}

func FindFirstId() bool {
	defer env.TracePanic()
	id, err := dao.GetShardCountProgress()
	if err != nil {
		time.Sleep(time.Duration(30) * time.Second)
		return false
	} else {
		if id == 0 {
			firstId = dao.GenerateZeroID(time.Now().Unix())
			err = dao.SetShardCountProgress(firstId)
			if err != nil {
				time.Sleep(time.Duration(30) * time.Second)
				return false
			}
		} else {
			firstId = id
		}
		logrus.Infof("[IterateShard]Start iterate the shards table from:%s\n", time.Unix(firstId>>32, 0).Format("2006-01-02 15:04:05"))
		return true
	}
}

func Iterate() {
	defer env.TracePanic()
	lasttime := firstId>>32 + int64(env.LsShardInterval)
	querylasttime := time.Now().Unix() - DELAY_TIMES
	if lasttime > querylasttime {
		time.Sleep(time.Duration(30) * time.Second)
	} else {
		logrus.Infof("[IterateShard]Start iterate shards from id:%d\n", firstId)
		lastid := dao.GenerateZeroID(lasttime)
		hash, err := dao.ListNodeShardCount(firstId, lastid)
		if err != nil {
			time.Sleep(time.Duration(30) * time.Second)
			return
		}
		hash2, metas, err := dao.ListRebuildShardCount(firstId, lastid)
		if err != nil {
			time.Sleep(time.Duration(30) * time.Second)
			return
		}
		if len(hash2) > 0 {
			for k, v := range hash2 {
				num, ok := hash[k]
				if ok {
					hash[k] = num + v
				} else {
					hash[k] = v
				}
			}
			UpdateShardMeta(metas)
		}
		UpdateShardCount(hash, firstId, lastid)
		err = dao.SetShardCountProgress(lastid)
		if err != nil {
			time.Sleep(time.Duration(30) * time.Second)
			return
		}
		s1 := time.Unix(firstId>>32, 0).Format("20060102")
		s2 := time.Unix(lastid>>32, 0).Format("20060102")
		if s1 != s2 {
			dao.DropNodeShardColl(firstId)
		}
		logrus.Infof("[IterateShard]Iterate shards OK, lastId:%d\n", lastid)
		firstId = lastid
	}
}

const BATCH_UPDATE_MAXSIZE = 500

func UpdateShardCount(hash map[int32]int64, firstid int64, lastid int64) {
	size := len(hash)
	if size > 0 {
		if size > BATCH_UPDATE_MAXSIZE {
			m := make(map[int32]int64)
			for k, v := range hash {
				m[k] = v
				if len(m) >= BATCH_UPDATE_MAXSIZE {
					UpdateShardCountWRetry(m, firstid, lastid)
					m = make(map[int32]int64)
				}
			}
			if len(m) > 0 {
				UpdateShardCountWRetry(m, firstid, lastid)
			}
		} else {
			UpdateShardCountWRetry(hash, firstid, lastid)
		}
	}
}

func UpdateShardCountWRetry(hash map[int32]int64, firstid int64, lastid int64) {
	for {
		err := dao.UpdateShardCount(hash, firstid, lastid)
		if err != nil {
			logrus.Errorf("[IterateShard]UpdateShardCount ERR:%s,count %d\n", err, len(hash))
			time.Sleep(time.Duration(30) * time.Second)
		} else {
			logrus.Infof("[IterateShard]UpdateShardCount OK,count %d\n", len(hash))
			return
		}
	}
}

func UpdateShardMeta(metas map[int64]int32) {
	size := len(metas)
	if size > 0 {
		if size > BATCH_UPDATE_MAXSIZE {
			m := make(map[int64]int32)
			for k, v := range metas {
				m[k] = v
				if len(m) >= BATCH_UPDATE_MAXSIZE {
					UpdateShardMetaWRetry(m)
					m = make(map[int64]int32)
				}
			}
			if len(m) > 0 {
				UpdateShardMetaWRetry(m)
			}
		} else {
			UpdateShardMetaWRetry(metas)
		}
	}
}

func UpdateShardMetaWRetry(metas map[int64]int32) {
	for {
		err := dao.UpdateShardMeta(metas)
		if err != nil {
			logrus.Errorf("[IterateShard]UpdateShardMeta ERR:%s,count %d\n", err, len(metas))
			time.Sleep(time.Duration(30) * time.Second)
		} else {
			logrus.Infof("[IterateShard]UpdateShardMeta OK,count %d\n", len(metas))
			return
		}
	}
}
