package main

import (
	"fmt"
	"os"

	"github.com/kardianos/service"
	"github.com/sirupsen/logrus"
	"github.com/yottachain/YTCoreService/dao"
	"github.com/yottachain/YTCoreService/env"
	"github.com/yottachain/YTCoreService/handle"
	"github.com/yottachain/YTCoreService/http"
	ytnet "github.com/yottachain/YTCoreService/net"
)

var programName = "ytsn"
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
	programName = os.Args[0]
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
		if cmd == "version" {
			fmt.Println(env.VersionID)
			return
		}
		if cmd == "init" {
			handle.InitSN()
			return
		}
		if cmd == "statuser" {
			http.StartIterateUser()
			return
		}
		if cmd == "console" {
			env.Console = true
			err = s.Run()
			if err != nil {
				logger.Info("Run err:", err.Error())
			}
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
		logger.Info("version      Show versionid.")
		logger.Info("init         Create yotta.SuperNode table.")
		logger.Info("console      Launch in the current console.")
		logger.Info("start        Start in the background as a daemon process.")
		logger.Info("stop         Stop if running as a daemon or in another console.")
		logger.Info("restart      Restart if running as a daemon or in another console.")
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
	ytnet.InitNodeMgr(dao.MongoAddress)
	ytnet.EOSInit()
	dao.InitUserID_seq()

	ytnet.Start(int32(env.Port), int32(env.Port2), ytnet.GetSuperNode(env.SuperNodeID).PrivKey)
	ytnet.RegisterGlobalMsgHandler(handle.OnMessage)

	handle.Start()
	http.Start(env.HttpPort)
}

func StopServer() {
	ytnet.Stop()
	dao.Close()
	logrus.Infof("[Booter]Service shutdown.\n")
	http.Stop()
}
