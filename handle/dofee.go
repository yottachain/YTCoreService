package handle

import (
	"math/big"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/yottachain/YTCoreService/dao"
	"github.com/yottachain/YTCoreService/env"
	"github.com/yottachain/YTCoreService/net"
)

func StartDoCacheFee() {
	time.Sleep(time.Duration(30) * time.Second)
	for {
		if !DoCacheAction() {
			time.Sleep(time.Duration(300) * time.Second)
		} else {
			time.Sleep(time.Duration(env.PayInterval) * time.Millisecond)
		}
	}
}

func DoCacheAction() bool {
	defer env.TracePanic("[DoCacheFee]")
	action := dao.FindAndDeleteNewObject()
	if action == nil {
		return false
	}
	usedspace := action.UsedSpace
	if action.Step == 0 {
		unitspace := uint64(1024 * 16)
		addusedspace := usedspace / unitspace
		if usedspace%unitspace > 1 {
			addusedspace = addusedspace + 1
		}
		err := net.AddUsedSpace(action.Username, addusedspace)
		if err != nil {
			dao.AddAction(action)
			logrus.Errorf("[DoCacheFee][%d] Add usedSpace ERR:%s\n", action.UserID, err)
			time.Sleep(time.Duration(60) * time.Second)
			return true
		}
		logrus.Infof("[DoCacheFee]User [%d] add usedSpace:%d\n", action.UserID, addusedspace)
	}
	firstCost := CalFirstFee(int64(usedspace))
	err := net.SubBalance(action.Username, firstCost)
	if err != nil {
		action.Step = 1
		dao.AddAction(action)
		logrus.Errorf("[DoCacheFee][%d] Sub Balance ERR:%s\n", action.UserID, err)
		time.Sleep(time.Duration(60) * time.Second)
	} else {
		logrus.Infof("[DoCacheFee]User [%d] sub balance:%d\n", action.UserID, firstCost)
	}
	return true
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

func CalFirstFee(usedpace int64) uint64 {
	uspace := big.NewInt(usedpace)
	unitFirstCost := big.NewInt(int64(env.UnitFirstCost))
	unitSpace := big.NewInt(int64(env.UnitSpace))
	bigcost := big.NewInt(0)
	bigcost = bigcost.Mul(uspace, unitFirstCost)
	bigcost = bigcost.Div(bigcost, unitSpace)
	return uint64(bigcost.Int64())
}
