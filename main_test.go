package main

import (
	"testing"

	"github.com/yottachain/YTCoreService/env"
	"github.com/yottachain/YTCoreService/examples"
	"github.com/yottachain/YTCoreService/service"
)

func Test_Test(t *testing.T) {
	env.DelLogPath = "D:/"
	log, err := service.GetNodeLog(1221)
	if err != nil {
		return
	}
	log.WriteLog("dsssssss")
	log.WriteLog("dsssssss")
	log.WriteLog("dsssssss")

}

func Test_S3(t *testing.T) {
	examples.S3()
}

func Test_MakeConst(t *testing.T) {
	Make()
}

func Test_SetVersion(t *testing.T) {
	SetVersion()
}

func Test_Codec(t *testing.T) {
	examples.TestCodec()
}
