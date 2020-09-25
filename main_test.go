package main

import (
	"testing"

	"github.com/yottachain/YTCoreService/env"
)

func init() {
	env.Console = true
}

func Test(t *testing.T) {
	defer env.TracePanic()

	//***********api test*********
	//test.ListObj()
	//test.UpAndDownFile()
	//test.DownLoadByKey()
	//test.UpAndDownBytes()

	//**********code test**********
	//env.InitClient()
	//test.TestCutFile()
	//test.TestLRC()
	select {}
}
