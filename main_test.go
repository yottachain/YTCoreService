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
	log, _ := env.AddLog("d:/sdd.log")
	log.Writer.Info("sdssdssdjksdsdsdsasd", "\n")
	log.Writer.Info("sdsrrsdsdsdsds", "\n")

	log.Close()

	log, _ = env.AddLog("d:/sdd.log")
	log.Writer.Info("22sdsrrsdsdsdsds", "\n")
	log.Writer.Info("22sdsrrsdsdsdsds", "\n")

	log.Close()

	log, _ = env.AddLog("d:/sdd.log")
	log.Writer.Info("332sdsrrsdsdsdsds", "\n")
	log.Writer.Info("33232sdsrrsdsdsdsds", "\n")

	/*
		_, err := handle.NewNodeLog(2, "d:/")
		if err != nil {
			//fmt.Println(err)
		}*/
}
