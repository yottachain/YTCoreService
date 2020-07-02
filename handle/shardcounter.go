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
			if id == 0 {
				firstId = dao.GenerateZeroID(time.Now().Unix())
				err = dao.SetShardCountProgress(firstId)
				if err != nil {
					time.Sleep(time.Duration(30) * time.Second)
					continue
				}
			} else {
				firstId = id
			}
			env.Log.Infof("Start iterate the shards table from:%s\n", time.Unix(firstId>>32, 0).Format("2006-01-02 15:04:05"))
			break
		}
	}
	for {
		lasttime := firstId>>32 + int64(env.LsShardInterval*60)
		querylasttime := time.Now().Unix() - DELAY_TIMES
		if lasttime > querylasttime {
			time.Sleep(time.Duration(30) * time.Second)
		} else {
			env.Log.Infof("Start iterate  shards  from id:%d\n", firstId)
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
			if len(hash2) > 0 {
				for k, v := range hash2 {
					num, ok := hash[k]
					if ok {
						hash[k] = num + v
					} else {
						hash[k] = v
					}
				}
				if len(metas) > 0 {
					err = dao.UpdateShardMeta(metas)
					if err != nil {
						time.Sleep(time.Duration(30) * time.Second)
						continue
					}
					env.Log.Infof("UpdateShardMeta OK, count %d\n", len(metas))
				}
			}
			if len(hash) > 0 {
				err = dao.UpdateShardCount(hash, firstId, lastid)
				if err != nil {
					time.Sleep(time.Duration(30) * time.Second)
					continue
				}
				env.Log.Infof("UpdateShardCount OK, count %d\n", len(hash))
			}
			err = dao.SetShardCountProgress(lastid)
			if err != nil {
				time.Sleep(time.Duration(30) * time.Second)
				continue
			}
			env.Log.Infof("Iterate  shards OK, lastId:%d\n", lastid)
			firstId = lastid
		}
	}

}
