package service

import (
	"github.com/sirupsen/logrus"
	"github.com/yottachain/YTCoreService/dao"
	"github.com/yottachain/YTCoreService/env"
	"github.com/yottachain/YTCoreService/eos"
	"github.com/yottachain/YTCoreService/handle"
	"github.com/yottachain/YTCoreService/http"
	"github.com/yottachain/YTCoreService/net"
)

func StartService() {
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

func StartSN() error {
	env.InitServer()
	dao.Init()
	net.InitServer(dao.MongoAddress, handle.OnMessage)
	eos.Init()
	handle.StartHandler()
	StartService()
	http.StartHttp(env.HttpPort)
	return nil
}

func StopSN() error {
	dao.Close()
	logrus.Infof("[Booter]Service shutdown.\n")
	return nil
}
