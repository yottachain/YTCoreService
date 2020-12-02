package main

import (
	"sync"
	"testing"

	"github.com/yottachain/YTCoreService/env"
	"github.com/yottachain/YTCoreService/test"
)

func init() {
	env.Console = true
}

var SyncList sync.Map

func Test(t *testing.T) {
	defer env.TracePanic("Test")

	//***********api test*********
	//test.UpAndDownFile()
	test.DownLoadByKey()
	//test.UpAndDownBytes()

	//************s3 test****************
	//test.SyncFile()
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
