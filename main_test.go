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

	//test.TestPkt()

	//***********api test*********
	//test.ListObj()
	test.UpAndDown()

	//**********code test**********
	//env.InitClient()
	//test.TestCutFile
	//test.TestLRC()
}
