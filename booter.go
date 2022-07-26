package main

import (
	"os"
	"strings"

	"github.com/yottachain/YTCoreService/api/backend"
	"github.com/yottachain/YTCoreService/env"
	"github.com/yottachain/YTCoreService/service"
	"github.com/yottachain/YTCoreService/test"
)

func main() {
	if strings.Contains(strings.ToUpper(os.Args[0]), "DEBUG") {
		StartYTSN()
	}

	if strings.Contains(strings.ToUpper(os.Args[0]), "YTSN") {
		StartYTSN()
	} else {
		StartYTS3()
	}
}

func StartYTSN() {
	env.YTSN.AddStart(service.StartSN)
	env.YTSN.AddStop(service.StopSN)
	env.YTSN.Init = InitSN
	env.LaunchYTSN()
}

func StartYTS3() {
	env.YTS3.AddStart(backend.StartS3)
	env.YTS3.Test = test.UpAndDown
	env.LaunchYTS3()
}
