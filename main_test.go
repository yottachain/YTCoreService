package main

import (
	"os"
	"testing"

	"github.com/yottachain/YTCoreService/env"
	"github.com/yottachain/YTCoreService/test"
)

func init() {
	env.Console = true
}

func Test(t *testing.T) {
	defer env.TracePanic("Test")
	if len(os.Args) > 1 {
		if os.Args[1] == "makeconst" {
			Make()
		} else if os.Args[1] == "auth" {
			test.Auth()
		} else if os.Args[1] == "codec" {
			test.Codec()
		} else if os.Args[1] == "eos" {
			test.EOS()
		} else if os.Args[1] == "sync" {
			test.Sync()
		} else if os.Args[1] == "s3" {
			test.S3()
		} else if os.Args[1] == "up&down" {
			test.UpAndDown()
		}
		return
	}
	call()
	select {}
}

func call() {
	log, _ := env.AddLog("d:/sddwe.log")

	log.Writer.Info("11111111111111sdssdssdjksdsdsdsasd", "\n")
	log.Writer.Info("1111111111sdsrrsdsdsdsds", "\n")

	log.Close()

	log, _ = env.AddLog("d:/sddwe.log")
	log.Writer.Info("2222222111sdssdssdjksdsdsdsasd", "\n")
	log.Writer.Info("222sdsrrsdsdsdsds", "\n")

}
