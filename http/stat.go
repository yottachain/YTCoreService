package http

import (
	"bytes"
	"fmt"
	"math/big"
	"net/http"
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

func StartIterateUser() {
	env.InitServer()
	dao.InitMongo()
	logrus.SetOutput(os.Stdout)
	net.InitNodeMgr(dao.MongoAddress)
	net.EOSInit()

	logname := env.YTSN_HOME + "/users.txt"
	os.Remove(logname)
	f, err := env.AddLog(logname)
	if err != nil {
		logrus.Panicf("[StatUser]Create LOG err:%s\n", err)
	}
	log = f
	log.Writer.Info("用户	ID	余额	存储占用	计费存储量	周期费用\n")
	defer log.Close()
	IterateUser()
	logrus.Infof("[SumUsedFee]STAT complete.\n")
	dao.Close()
}

func StartSumUser() {
	if !env.GC {
		return
	}
	if !net.IsActive() {
		return
	}
	for {
		IterateUser()
		time.Sleep(time.Duration(3 * time.Hour))
	}
}

const UserSTATExpiredTime = 60 * 60 * 5

var UserSTATCache = struct {
	Value atomic.Value
}{}

func UserStatHandle(w http.ResponseWriter, req *http.Request) {
	b := checkRoutine()
	defer atomic.AddInt32(RoutineConter, -1)
	if !b {
		WriteErr(w, "HTTP_ROUTINE:Too many routines")
		return
	}
	if !checkIp(req.RemoteAddr) {
		WriteErr(w, fmt.Sprintf("Invalid IP:%s", req.RemoteAddr))
		return
	}
	s := UserSTATCache.Value.Load()
	if s == nil {
		WriteText(w, "")
	} else {
		ss, _ := s.(string)
		WriteText(w, ss)
	}
}

func IterateUser() {
	defer env.TracePanic("[StatUser]")
	var content bytes.Buffer
	content.WriteString("用户	ID	余额	存储占用	计费存储量	周期费用\n")
	var lastId int32 = 0
	limit := 100
	logrus.Infof("[StatUser]Start iterate user...\n")
	cyusedspce := int64(0)
	cycost := int64(0)
	usedspace := int64(0)
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
				sum := &UserObjectSum{UserID: user.UserID, UsedSpace: new(int64), UserName: user.Username}
				atomic.StoreInt64(sum.UsedSpace, 0)
				sum.IterateObjects2()
				balance, err := net.GetBalance(user.Username)
				if err != nil {
					content.WriteString(fmt.Sprintf("%s	%d	ERR	%d	%d	%d\n", user.Username, user.UserID, user.Usedspace, sum.GetUsedSpace(), sum.Cost))
				} else {
					content.WriteString(fmt.Sprintf("%s	%d	%d	%d	%d	%d\n", user.Username, user.UserID, balance, user.Usedspace, sum.GetUsedSpace(), sum.Cost))
				}
				cyusedspce = cyusedspce + sum.GetUsedSpace()
				cycost = cycost + sum.Cost
				usedspace = usedspace + user.Usedspace
			}

		}
	}
	content.WriteString(fmt.Sprintf("ALL	0	0	%d	%d	%d\n", usedspace, cyusedspce, cycost))
	UserSTATCache.Value.Store(content.String())
	logrus.Infof("[StatUser]Iterate user OK!\n")
}

type UserObjectSum struct {
	UserID    int32
	UserName  string
	UsedSpace *int64
	Cost      int64
}

func (me *UserObjectSum) AddUsedSapce(space int64) {
	atomic.AddInt64(me.UsedSpace, space)
}

func (me *UserObjectSum) GetUsedSpace() int64 {
	return atomic.LoadInt64(me.UsedSpace)
}

func (me *UserObjectSum) IterateObjects2() {
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

func CalCycleFee(usedpace int64) uint64 {
	uspace := big.NewInt(usedpace)
	unitCycleCost := big.NewInt(int64(env.UnitCycleCost))
	unitSpace := big.NewInt(int64(env.UnitSpace))
	bigcost := big.NewInt(0)
	bigcost = bigcost.Mul(uspace, unitCycleCost)
	bigcost = bigcost.Div(bigcost, unitSpace)
	return uint64(bigcost.Int64())
}

func (me *UserObjectSum) SetCycleFee() {
	usedSpace := me.GetUsedSpace()
	cost := CalCycleFee(usedSpace)
	me.Cost = int64(cost)
	logrus.Infof("[StatUser]File statistics completed,UserID:%d,usedspace:%d,cost:%d\n", me.UserID, usedSpace, cost)
}
