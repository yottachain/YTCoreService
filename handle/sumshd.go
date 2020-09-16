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
		IterateUploadShards()
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
		logrus.Infof("[IterateShards]Start iterate the shards table from:%s\n",
			time.Unix(firstId>>32, 0).Format("2006-01-02 15:04:05"))

		return true
	}
}

func IterateUploadShards() {
	defer env.TracePanic()
	querylasttime := dao.GenerateZeroID(time.Now().Unix() - DELAY_TIMES)
	logrus.Infof("[IterateShards]Start iterate shards from id:%d\n", firstId)
	hash, id, has, err := dao.ListNodeShardCount(firstId, querylasttime)
	if err != nil {
		time.Sleep(time.Duration(30) * time.Second)
		return
	}
	if len(hash) > 0 {
		UpdateShardCount(hash, firstId, id)
	}
	var s1, s2 string
	if id != firstId {
		err = dao.SetShardCountProgress(id)
		if err != nil {
			time.Sleep(time.Duration(30) * time.Second)
			return
		}
		s1 = time.Unix(firstId>>32, 0).Format("2006010215")
		s2 = time.Unix(id>>32, 0).Format("2006010215")
		if s1 != s2 {
			dao.DropNodeShardColl(firstId)	 
		}
		logrus.Infof("[IterateShards]Iterate shards OK, lastId:%d\n", id)
		firstId = id
	}
	if !has && s1 == s2 {
		time.Sleep(time.Duration(DELAY_TIMES) * time.Second)
	} else {
		time.Sleep(time.Duration(1) * time.Second)
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
			logrus.Errorf("[IterateShards]UpdateShardCount ERR:%s,count %d\n", err, len(hash))
			time.Sleep(time.Duration(30) * time.Second)
		} else {
			logrus.Infof("[IterateShards]UpdateShardCount OK,count %d\n", len(hash))
			return
		}
	}
}
