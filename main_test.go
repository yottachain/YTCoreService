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
	defer env.TracePanic("Test")

	test.TestEOS()

	//***********api test*********
	//test.UpAndDownFile()
	//test.DownLoadByKey()
	//test.UpAndDownBytes()

	//************s3 test****************
	//test.UploadFile()
	//test.ListBucket()
	//test.ListObj()

	//**********code test**********
	//env.InitClient()
	//test.TestMultiCutFile()
	//test.TestCutFile()
	//test.TestLRC()
	select {}
}
