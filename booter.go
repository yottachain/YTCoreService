package main

import (
	"os"
	"strings"

	"github.com/yottachain/YTCoreService/api/backend"
	"github.com/yottachain/YTCoreService/api/s3ext"
	"github.com/yottachain/YTCoreService/env"
	"github.com/yottachain/YTCoreService/examples"
	"github.com/yottachain/YTCoreService/service"
)

func main() {
	programname := os.Getenv("ProgramName")
	if programname == "" {
		programname = os.Args[0]
	}
	programname = strings.ToUpper(programname)
	if strings.Contains(programname, "YTSN") {
		StartYTSN()
	} else if strings.Contains(programname, "WRITEFILE") {
		examples.TestWriteFile()
	} else if strings.Contains(programname, "CODEC") {
		examples.TestCodec()
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
	env.YTS3.AddStart(s3ext.StartServer)
	env.YTS3.AddStop(backend.StopS3)
	env.YTS3.Test = TestApi
	env.LaunchYTS3()
}
