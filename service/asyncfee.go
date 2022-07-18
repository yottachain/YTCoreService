package handle

import (
	"time"

	"github.com/sirupsen/logrus"
	"github.com/yottachain/YTCoreService/dao"
	"github.com/yottachain/YTCoreService/env"
	"github.com/yottachain/YTCoreService/eos"
)

func Start() {
	go initLog()
	if env.SUM_SERVICE {
		go startIterateShards()
		go startDoCacheFee()
		go startDoCycleFee()
		go startDoDelete()
		go startGC()
		go startRelationshipSum()
	}
}

func startDoCacheFee() {
	time.Sleep(time.Duration(30) * time.Second)
	for {
		if !doCacheAction() {
			time.Sleep(time.Duration(300) * time.Second)
		} else {
			time.Sleep(time.Duration(env.PayInterval) * time.Millisecond)
		}
	}
}

func doCacheAction() bool {
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
		err := eos.AddUsedSpace(action.Username, addusedspace)
		if err != nil {
			dao.AddAction(action)
			logrus.Errorf("[DoCacheFee][%d] Add usedSpace ERR:%s\n", action.UserID, err)
			time.Sleep(time.Duration(60) * time.Second)
			return true
		}
		logrus.Infof("[DoCacheFee]User [%d] add usedSpace:%d\n", action.UserID, addusedspace)
	}
	firstCost := env.CalFirstFee(int64(usedspace))
	err := eos.SubBalance(action.Username, firstCost)
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
