package handle

import (
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
			time.Sleep(time.Duration(30) * time.Second)
		}
	}
}

func DoCacheAction() bool {
	action := dao.FindOneNewObject()
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
			time.Sleep(time.Duration(3) * time.Minute)
			return true
		}
		logrus.Infof("[DoCacheFee]User [%d] add usedSpace:%d\n", action.UserID, addusedspace)
	}
	firstCost := env.UnitFirstCost * usedspace / env.UnitSpace
	err := net.SubBalance(action.Username, firstCost)
	if err != nil {
		action.Step = 1
		dao.AddAction(action)
		logrus.Errorf("[DoCacheFee][%d] Sub Balance ERR:%s\n", action.UserID, err)
		time.Sleep(time.Duration(3) * time.Minute)
	} else {
		logrus.Infof("[DoCacheFee]User [%d] sub balance:%d\n", action.UserID, firstCost)
	}
	return true
}