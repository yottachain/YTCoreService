package handle

import (
	"time"

	"github.com/yottachain/YTCoreService/dao"
	"github.com/yottachain/YTCoreService/env"
)

const DELAY_TIMES = 60 * 5

func StartIterate() {
	var firstId int64
	for {
		id, err := dao.GetShardCountProgress()
		if err != nil {
			time.Sleep(time.Duration(30) * time.Second)
		} else {
			firstId = id
			env.Log.Infof("Start iterate the shards table from id:%d\n", id)
			break
		}
	}
	for {
		lasttime := firstId>>32 + int64(env.LsShardInterval*60)
		querylasttime := time.Now().Unix() - DELAY_TIMES
		if lasttime > querylasttime {
			time.Sleep(time.Duration(30) * time.Second)
		} else {
			lastid := dao.GenerateZeroID(lasttime)
			hash, err := dao.ListShardCount(firstId, lastid)
			if err != nil {
				time.Sleep(time.Duration(30) * time.Second)
				continue
			}
			hash2, metas, err := dao.ListRebuildShardCount(firstId, lastid)
			if err != nil {
				time.Sleep(time.Duration(30) * time.Second)
				continue
			}
			for k, v := range hash2 {
				num, ok := hash[k]
				if ok {
					hash[k] = num + v
				} else {
					hash[k] = v
				}
			}
			err = dao.UpdateShardMeta(metas)
			if err != nil {
				time.Sleep(time.Duration(30) * time.Second)
				continue
			}
			err = dao.UpdateShardCount(hash, firstId, lastid)
			if err != nil {
				time.Sleep(time.Duration(30) * time.Second)
				continue
			}
			err = dao.SetShardCountProgress(lastid)
			if err != nil {
				time.Sleep(time.Duration(30) * time.Second)
				continue
			}
			firstId = lasttime
		}
	}

}
