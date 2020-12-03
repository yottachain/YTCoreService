package main

import (
	"fmt"
	"path"
	"sync"
	"testing"

	"github.com/yottachain/YTCoreService/env"
)

func init() {
	env.Console = true
}

var SyncList sync.Map

func Test(t *testing.T) {
	defer env.TracePanic("Test")

	p := "/sd\\ssdsds//dfdf//s"
	dir := path.Clean(p)
	fmt.Println(dir)
	//***********api test*********
	//test.UpAndDownFile()
	//test.DownLoadByKey()
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
