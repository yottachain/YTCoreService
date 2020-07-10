package main

import (
	"os"

	"github.com/kardianos/service"
	"github.com/sirupsen/logrus"
	"github.com/yottachain/YTCoreService/dao"
	"github.com/yottachain/YTCoreService/env"
	"github.com/yottachain/YTCoreService/handle"
	"github.com/yottachain/YTCoreService/http"
	"github.com/yottachain/YTCoreService/net"
	"github.com/yottachain/YTCoreService/test"
)

var logger service.Logger
var serviceConfig = &service.Config{
	Name:        "ytsnd",
	DisplayName: "go ytsn service",
	Description: "go ytsn daemons service",
}

type Program struct{}

func (p *Program) Start(s service.Service) error {
	go p.run()
	return nil
}

func (p *Program) Stop(s service.Service) error {
	StopServer()
	return nil
}

func (p *Program) run() {
	StartServer()
}

func main() {
	test.TestLRC()
}

func main11() {
	prog := &Program{}
	s, err := service.New(prog, serviceConfig)
	if err != nil {
		panic(err)
	}
	logger, err = s.Logger(nil)
	if err != nil {
		panic(err)
	}
	if len(os.Args) > 1 {
		cmd := os.Args[1]
		if cmd == "init" {
			handle.InitSN()
			return
		}
		if cmd == "start" {
			err = s.Start()
			if err != nil {
				logger.Info("Maybe the daemons are not installed.Start err:", err.Error())
			} else {
				logger.Info("Start OK.")
			}
			return
		}
		if cmd == "restart" {
			err = s.Restart()
			if err != nil {
				logger.Info("Maybe the daemons are not installed.Restart err:", err.Error())
			} else {
				logger.Info("Restart OK.")
			}
			return
		}
		if cmd == "stop" {
			err = s.Stop()
			if err != nil {
				logger.Info("Stop err:", err.Error())
			} else {
				logger.Info("Stop OK.")
			}
			return
		}
		if cmd == "install" {
			err = s.Install()
			if err != nil {
				logger.Info("Install err:", err.Error())
			} else {
				logger.Info("Install OK.")
			}
			return
		}
		if cmd == "uninstall" {
			err = s.Uninstall()
			if err != nil {
				logger.Info("Uninstall err:", err.Error())
			} else {
				logger.Info("Uninstall OK.")
			}
			return
		}
		logger.Info("Commands:")
		logger.Info("init          Create  yotta.SuperNode table.")
		logger.Info("start        Start in the background as a daemon process.")
		logger.Info("stop         Stop if running as a daemon or in another console.")
		logger.Info("restart         Restart if running as a daemon or in another console.")
		logger.Info("install      Install to start automatically when system boots.")
		logger.Info("uninstall    Uninstall.")
		return
	}
	err = s.Run()
	if err != nil {
		logger.Info("Run err:", err.Error())
	}
}

func StartServer() {
	env.InitServer()
	dao.InitMongo()
	net.InitNodeMgr(dao.MongoAddress)
	net.EOSInit()
	dao.InitUserID_seq()

	net.Start(int32(env.Port), int32(env.Port2), net.GetSuperNode(env.SuperNodeID).PrivKey)
	net.RegisterGlobalMsgHandler(handle.OnMessage)

	handle.Start()
	http.Start(env.HttpPort)
}

func StopServer() {
	net.Stop()
	dao.Close()
	logrus.Infof("[Booter]Service shutdown.\n")
	http.Stop()
}
