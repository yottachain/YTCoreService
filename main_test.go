package main

import (
	"crypto/md5"
	"fmt"
	"sync"
	"testing"

	"github.com/yottachain/YTCoreService/env"
)

func init() {
	env.Console = true
}

func Test(t *testing.T) {
	defer env.TracePanic("Test")
	bs := md5.New().Sum([]byte("ss"))

	var DoingList sync.Map

	DoingList.Store(string(bs), "")

	v, ok := DoingList.Load("1111")
	if ok {
		fmt.Println(v)
	}

	//***********api test*********
	//test.ListObj()
	//test.UpAndDownFile()
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
