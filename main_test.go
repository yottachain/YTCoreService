package main

import (
	"encoding/hex"
	"fmt"
	"io/ioutil"
	"sync"
	"testing"

	"github.com/golang/protobuf/proto"
	"github.com/yottachain/YTCoreService/env"
	"github.com/yottachain/YTCoreService/pkt"
)

func init() {
	env.Console = true
}

var SyncList sync.Map

func Test(t *testing.T) {
	defer env.TracePanic("Test")
	//Make()
	//***********api test*********

	bs, err := ioutil.ReadFile("d:/aa.txt")
	bss, err := hex.DecodeString(string(bs))
	req := &pkt.SaveObjectMetaReq{}
	err = proto.Unmarshal(bss, req)
	fmt.Println(err)

	//test.UpAndDownBytes()

	//************s3 test****************

	//test.SyncFile()
	//test.UploadFile()
	//test.DownLoadFile()
	//test.ListBucket()
	//test.ListObj()

	//**********code test**********
	//env.InitClient()
	//test.TestMultiCutFile()
	//test.TestCutFile()
	//test.TestLRC()

	//**************************
	//test.TestEOS()
	select {}
}
