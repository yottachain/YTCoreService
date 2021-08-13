package handle

import (
	"bytes"
	"fmt"
	"os"
	"sync/atomic"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/yottachain/YTCoreService/dao"
	"github.com/yottachain/YTCoreService/env"
	"github.com/yottachain/YTCoreService/net"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

var log *env.NoFmtLog

func StatUser() {
	env.InitServer()
	dao.InitMongo()
	logrus.SetOutput(os.Stdout)
	net.InitNodeMgr(dao.MongoAddress)
	net.EOSInit()
	IterateUsers()
	logrus.Infof("[StatUser]STAT complete.\n")
	dao.Close()
}

func GetBlance(username string) (int64, error) {
	if username == "pollydevnew2" {
		return -5, nil
	}
	balance, err := net.GetBalance(username)
	if err != nil {
		return 0, err
	}
	return balance, nil
}

func StartSumUser() {
	if !net.IsActive() {
		return
	}
	IterateUsers()
	//for {
	//	IterateUsers()
	//	time.Sleep(time.Duration(24 * time.Hour))
	//}
}

var UserSTATCache = struct {
	Value atomic.Value
}{}

func IterateUsers() {
	defer env.TracePanic("[StatUser]")
	var content bytes.Buffer
	content.WriteString("UserName	ID	balance	UsedSpace	UsedSpace1	UsedSpace2	Cost\n")
	var lastId int32 = 0
	limit := 100
	logrus.Infof("[StatUser]Start iterate user...\n")
	logrus.Infof("[StatUser]UserName	ID	balance	UsedSpace	UsedSpace1	UsedSpace2	Cost\n")
	cyusedspce := int64(0)
	cycost := int64(0)
	usedspace := int64(0)
	usedspace1 := int64(0)
	for {
		us, err := dao.ListUsers(lastId, limit, bson.M{"_id": 1, "usedspace": 1, "username": 1, "costPerCycle": 1})
		if err != nil {
			time.Sleep(time.Duration(30) * time.Second)
			continue
		}
		if len(us) == 0 {
			break
		} else {
			for _, user := range us {
				lastId = user.UserID
				sum := &UsedspaceSum{UserID: user.UserID, UsedSpace: new(int64), UserName: user.Username}
				atomic.StoreInt64(sum.UsedSpace, 0)
				sum.IterateObjects()
				sum1 := &UsedspaceSum{UserID: user.UserID, UsedSpace: new(int64), UserName: user.Username}
				atomic.StoreInt64(sum1.UsedSpace, 0)
				sum1.IterateObjects2()
				usedspace1 = usedspace1 + sum1.GetUsedSpace()
				cyusedspce = cyusedspce + sum.GetUsedSpace()
				cycost = cycost + sum.Cost
				usedspace = usedspace + user.Usedspace
				balance, err := net.GetBalance(user.Username)
				if err != nil {
					content.WriteString(fmt.Sprintf("%s	%d	ERR	%d	%d	%d	%d\n", user.Username, user.UserID, user.Usedspace, sum1.GetUsedSpace(), sum.GetUsedSpace(), sum.Cost))
					logrus.Infof("[StatUser]%s	%d	ERR	%d	%d	%d	%d\n", user.Username, user.UserID, user.Usedspace, sum1.GetUsedSpace(), sum.GetUsedSpace(), sum.Cost)
				} else {
					content.WriteString(fmt.Sprintf("%s	%d	%d	%d	%d	%d	%d\n", user.Username, user.UserID, balance, user.Usedspace, sum1.GetUsedSpace(), sum.GetUsedSpace(), sum.Cost))
					logrus.Infof("[StatUser]%s	%d	%d	%d	%d	%d	%d\n", user.Username, user.UserID, balance, user.Usedspace, sum1.GetUsedSpace(), sum.GetUsedSpace(), sum.Cost)
				}
			}

		}
	}
	content.WriteString(fmt.Sprintf("ALL	-	-	%d	%d	%d	%d\n", usedspace, usedspace1, cyusedspce, cycost))
	UserSTATCache.Value.Store(content.String())
	logrus.Infof("[StatUser]ALL	-	-	%d	%d	%d	%d\n", usedspace, usedspace1, cyusedspce, cycost)
	logrus.Infof("[StatUser]Iterate user OK!\n")
}

type UsedspaceSum struct {
	UserID    int32
	UserName  string
	UsedSpace *int64
	Cost      int64
}

func (me *UsedspaceSum) AddUsedSapce(space int64) {
	atomic.AddInt64(me.UsedSpace, space)
}

func (me *UsedspaceSum) GetUsedSpace() int64 {
	return atomic.LoadInt64(me.UsedSpace)
}

func (me *UsedspaceSum) IterateObjects2() {
	logrus.Infof("[StatUser]Start sum fee,UserID:%d\n", me.UserID)
	limit := 10000
	firstId := primitive.NilObjectID
	for {
		ls, id, err := dao.ListObjects3(uint32(me.UserID), firstId, limit)
		if err != nil {
			logrus.Errorf("[StatUser]UserID %d list object err:%s\n", me.UserID, err)
			time.Sleep(time.Duration(30) * time.Second)
			continue
		} else {
			logrus.Infof("[StatUser]UserID %d list object ok,usedspace %d,time %s\n", me.UserID, ls, id.Timestamp().Format("2006-01-02 15:04:05"))
		}
		me.AddUsedSapce(int64(ls))
		firstId = id
		if firstId == primitive.NilObjectID {
			break
		}
	}
}

func (me *UsedspaceSum) IterateObjects() {
	logrus.Infof("[StatUser]Start sum fee,UserID:%d\n", me.UserID)
	limit := 10000
	firstId := primitive.NilObjectID
	for {
		ls, id, err := dao.ListObjects2(uint32(me.UserID), firstId, limit)
		if err != nil {
			logrus.Errorf("[StatUser]UserID %d list object err:%s\n", me.UserID, err)
			time.Sleep(time.Duration(30) * time.Second)
			continue
		} else {
			logrus.Infof("[StatUser]UserID %d list object ok,usedspace %d,time %s\n", me.UserID, ls, id.Timestamp().Format("2006-01-02 15:04:05"))
		}
		me.AddUsedSapce(int64(ls))
		firstId = id
		if firstId == primitive.NilObjectID {
			break
		}
	}
	me.SetCycleFee()
}

func (me *UsedspaceSum) SetCycleFee() {
	usedSpace := me.GetUsedSpace()
	cost := CalCycleFee(usedSpace)
	me.Cost = int64(cost)
	logrus.Infof("[StatUser]File statistics completed,UserID:%d,usedspace:%d,cost:%d\n", me.UserID, usedSpace, cost)
}
