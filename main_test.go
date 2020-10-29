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
	//test.DownLoadByKey()
	//test.UpAndDownBytes()
	//test.ListBucket()

	//**********code test**********
	//env.InitClient()
	//test.TestMultiCutFile()
	//test.TestCutFile()
	//test.TestLRC()
	select {}
}
