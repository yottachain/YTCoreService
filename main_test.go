package main

import (
	"testing"

	"github.com/yottachain/YTCoreService/env"
	"github.com/yottachain/YTCoreService/test"
)

func init() {
	env.Console = true
}

func Test(t *testing.T) {
	defer env.TracePanic()

	//***********api test*********
	//test.ListObj()
	test.UpAndDownFile()

	//**********code test**********
	//env.InitClient()
	//test.TestCutFile
	//test.TestLRC()
	select {}
}
