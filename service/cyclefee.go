package service

import (
	"time"

	"github.com/sirupsen/logrus"
	"github.com/yottachain/YTCoreService/dao"
	"github.com/yottachain/YTCoreService/env"
	"github.com/yottachain/YTCoreService/net/eos"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

func startDoCycleFee() {
	for {
		if env.SUM_USER_FEE == 0 {
			time.Sleep(time.Duration(30) * time.Second)
			continue
		}
		iterateUser()
		time.Sleep(time.Duration(180) * time.Second)
	}
}

func iterateUser() {
	defer env.TracePanic("[SumUsedFee]")
	var lastId int32 = 0
	limit := 100
	logrus.Infof("[SumUsedFee]Start iterate user...\n")
	for {
		us, err := dao.ListUsers(lastId, limit, bson.M{"_id": 1, "nextCycle": 1, "username": 1, "costPerCycle": 1})
		if err != nil {
			time.Sleep(time.Duration(30) * time.Second)
			continue
		}
		if len(us) == 0 {
			break
		} else {
			for _, user := range us {
				lastId = user.UserID
				sum := &UserObjectSum{UserID: user.UserID, UsedSpace: 0, UserName: user.Username, CostPerCycle: uint64(user.CostPerCycle)}
				flag := false
				flag, err := eos.CheckFreeSpace(user.UserID)
				if err != nil {
					logrus.Errorf("[SumUsedFee][%d]CheckFreeSpace ERR:%s\n", lastId, err)
				}
				if !flag {
					sum.IterateObjects()
				} else {
					logrus.Infof("[SumUsedFee][%d]Use free space\n", lastId)
				}
			}
		}
	}
	logrus.Infof("[SumUsedFee]Iterate user OK!\n")
}

type UserObjectSum struct {
	UserID       int32
	UserName     string
	UsedSpace    int64
	CostPerCycle uint64
}

func (me *UserObjectSum) IterateObjects() {
	for {
		lasttime, err := dao.GetUserSumTime(me.UserID)
		if err != nil {
			time.Sleep(time.Duration(15) * time.Second)
			continue
		} else {
			if time.Now().Unix()*1000-lasttime < int64(env.CostSumCycle) {
				return
			}
			break
		}
	}
	logrus.Infof("[SumUsedFee]Start sum fee,UserID:%d\n", me.UserID)
	limit := 10000
	firstId := primitive.NilObjectID
	for {
		ls, id, err := dao.ListObjects(uint32(me.UserID), firstId, limit)
		if err != nil {
			logrus.Errorf("[SumUsedFee]UserID %d list object err:%s\n", me.UserID, err)
			time.Sleep(time.Duration(30) * time.Second)
			continue
		} else {
			logrus.Infof("[SumUsedFee]UserID %d list object ok,usedspace %d,time %s\n", me.UserID, ls, id.Timestamp().Format("2006-01-02 15:04:05"))
		}
		me.UsedSpace = me.UsedSpace + int64(ls)
		firstId = id
		if firstId == primitive.NilObjectID {
			break
		}
	}
	me.SetCycleFee()
}

func (me *UserObjectSum) SetCycleFee() {
	cost := env.CalCycleFee(me.UsedSpace)
	logrus.Infof("[SumUsedFee]File statistics completed,UserID:%d,usedspace:%d,cost:%d\n", me.UserID, me.UsedSpace, cost)
	var err error
	if cost > 0 {
		if me.CostPerCycle == cost {
			logrus.Warnf("[SumUsedFee]Not need to set costPerCycle,old cost:%d,UserID:%d\n", me.CostPerCycle, me.UserID)
		} else {
			num := 0
			for {
				err = eos.SetHfee(me.UserName, cost)
				if err != nil {
					num++
					if num > 8 {
						break
					} else {
						time.Sleep(time.Duration(15) * time.Second)
					}
				} else {
					dao.UpdateUserCost(me.UserID, cost)
					logrus.Infof("[SumUsedFee]Set costPerCycle:%d,usedspace:%d,UserID:%d\n", cost, me.UsedSpace, me.UserID)
					break
				}
			}
		}
	}
	if err == nil {
		dao.SetUserSumTime(me.UserID)
	}
}
