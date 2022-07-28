package service

import (
	"github.com/sirupsen/logrus"
	"github.com/yottachain/YTCoreService/dao"
	"github.com/yottachain/YTCoreService/env"
	"github.com/yottachain/YTCoreService/handle"
	"github.com/yottachain/YTCoreService/net"
	"github.com/yottachain/YTCoreService/net/eos"
	"github.com/yottachain/YTCoreService/service/http"
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

func StartSN() {
	env.InitServer()
	dao.Init()
	net.InitServer(dao.MongoAddress, handle.OnMessage)
	eos.Init()
	handle.StartHandler()
	StartService()
	http.StartHttp(env.HttpPort)
}

func StopSN() {
	dao.Close()
	logrus.Infof("[Booter]Service shutdown.\n")
}
