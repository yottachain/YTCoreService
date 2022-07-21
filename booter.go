package main

import (
	"fmt"
	"net"
	"net/http"
	"os"
	"strings"

	"github.com/sirupsen/logrus"
	"github.com/yottachain/YTCoreService/api/backend"
	"github.com/yottachain/YTCoreService/dao"
	"github.com/yottachain/YTCoreService/env"
	"github.com/yottachain/YTCoreService/eos"
	"github.com/yottachain/YTCoreService/handle"
	ythttp "github.com/yottachain/YTCoreService/http"
	ytnet "github.com/yottachain/YTCoreService/net"
	"github.com/yottachain/YTCoreService/s3"
	ytservice "github.com/yottachain/YTCoreService/service"
)

func main() {
	if strings.Contains(strings.ToUpper(os.Args[0]), "YTSN") {
		StartYTSN()
	} else {
		StartYTS3()
	}
}

func StartYTSN() {
	env.YTSN.AddStart(StartServer)
	env.YTSN.AddStop(StopServer)
	env.YTSN.Init = InitSN
	env.LaunchYTSN()
}

func StartServer() error {
	env.InitServer()
	dao.Init()
	ytnet.InitServer(dao.MongoAddress, nil)
	eos.Init()
	handle.Start()
	ytservice.StartServer()
	ythttp.Start(env.HttpPort)
	return nil
}

func StopServer() error {
	dao.Close()
	logrus.Infof("[Booter]Service shutdown.\n")
	return nil
}

func StartYTS3() {
	env.YTS3.AddStart(StartS3)
	env.LaunchYTS3()
}

func StartS3() error {
	fs := backend.NewYTFS()
	addr := fmt.Sprintf(":%d", env.S3Port)
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	defer listener.Close()
	server := &http.Server{Addr: addr, Handler: s3.NewS3(fs).Server()}
	if env.CertFilePath != "" {
		logrus.Infof("[Booter]Start S3 server https port :%d\n", listener.Addr().(*net.TCPAddr).Port)
		return server.ServeTLS(listener, env.CertFilePath, env.KeyFilePath)
	} else {
		logrus.Infof("[Booter]Start S3 server http port :%d\n", listener.Addr().(*net.TCPAddr).Port)
		return server.Serve(listener)
	}
}

func StopS3() error {
	return nil
}
