package handle

import (
	"time"

	"github.com/mr-tron/base58"
	"github.com/sirupsen/logrus"
	"github.com/yottachain/YTCoreService/dao"
	"github.com/yottachain/YTCoreService/env"
	"github.com/yottachain/YTCoreService/pkt"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

var DEL_BLK_CH chan int

func InitDELPool() {
	DEL_BLK_CH = make(chan int, env.MAX_DELBLK_ROUTINE)
	for ii := 0; ii < int(env.MAX_DELBLK_ROUTINE); ii++ {
		DEL_BLK_CH <- 1
	}
}

func StartDoDelete() {
	InitDELPool()
	time.Sleep(time.Duration(10) * time.Second)
	for {
		IterateDELLog()
		time.Sleep(time.Duration(10) * time.Second)
	}
}

func IterateDELLog() {
	for {
		log := dao.FindOneDelLOG()
		if log == nil {
			return
		}
		DelBlocks(log.UID, log.VNU, true, false)
	}
}

const VBI_COUNT_LIMIT = 10

func DelBlocks(uid int32, vnu primitive.ObjectID, decSpace bool, del bool) {
	for {
		meta, err := dao.DelOrUpObject(uid, vnu, decSpace, del)
		if err != nil {
			time.Sleep(time.Duration(30) * time.Second)
			logrus.Errorf("[DeleteOBJ][%d]Deleting object %s,ERR:%s\n", uid, vnu.Hex(), err)
			break
		} else {
			if meta != nil {
				logrus.Debugf("[DeleteOBJ][%d]Deleting object %s,block count %d...\n", uid, vnu.Hex(), len(meta.BlockList))
				for _, refbs := range meta.BlockList {
					refer := pkt.NewRefer(refbs)
					if refer == nil {
						logrus.Errorf("[DeleteOBJ][%d]Refer data err\n", uid)
						continue
					}
					<-DEL_BLK_CH
					go delBlock(refer.VBI)
				}
				logrus.Infof("[DeleteOBJ][%d]Deleting object %s ok,block count %d\n", uid, vnu.Hex(), len(meta.BlockList))
			}
			break
		}
	}
}

func delBlock(vbi int64) {
	defer func() { DEL_BLK_CH <- 1 }()
	startTime := time.Now()
	shds, er := dao.DelOrUpBLK(vbi)
	if er == nil {
		dbtime := time.Since(startTime).Milliseconds()
		er = writeLOG(shds)
		if er == nil {
			alltime := time.Since(startTime).Milliseconds()
			logrus.Infof("[DeleteBlock]Delete block:%d,take times %d/%d ms\n", vbi, dbtime, alltime)
		}
	}
}

func writeLOG(shds []*dao.ShardMeta) error {
	if shds != nil {
		for _, shd := range shds {
			log, err := GetNodeLog(shd.NodeId)
			if err != nil {
				logrus.Errorf("[ShardLOG]GetNodeLog ERR:%s\n", err)
				return err
			}
			err = log.WriteLog(base58.Encode(shd.VHF))
			if err != nil {
				logrus.Errorf("[ShardLOG]WriteLog %d ERR:%s\n", shd.NodeId, err)
				return err
			}
			if shd.NodeId2 > 0 {
				log, err = GetNodeLog(shd.NodeId2)
				if err != nil {
					logrus.Errorf("[ShardLOG]GetNodeLog ERR:%s\n", err)
					return err
				}
				err = log.WriteLog(base58.Encode(shd.VHF))
				if err != nil {
					logrus.Errorf("[ShardLOG]WriteLog %d ERR:%s\n", shd.NodeId2, err)
					return err
				}
			}
		}
		decShardCount(shds)
	}
	return nil
}

func decShardCount(ls []*dao.ShardMeta) error {
	vbi := dao.GenerateShardID(1)
	m := make(map[int32]int16)
	for _, shard := range ls {
		num, ok := m[shard.NodeId]
		if ok {
			m[shard.NodeId] = num - 1
		} else {
			m[shard.NodeId] = -1
		}
		if shard.NodeId2 > 0 {
			num, ok = m[shard.NodeId2]
			if ok {
				m[shard.NodeId2] = num - 1
			} else {
				m[shard.NodeId2] = -1
			}
		}
	}
	bs := dao.ToBytes(m)
	return dao.SaveNodeShardCount(vbi, bs)
}
