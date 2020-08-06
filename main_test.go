package main

import (
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/yottachain/YTCoreService/env"
	"github.com/yottachain/YTCoreService/test"
)

func CatchError(name string) {
	if r := recover(); r != nil {
		logrus.Errorf("[%s]ERR:%s\n", name, r)
	}
}

func Test(t *testing.T) {
	//defer CatchError("Test")
	env.Console = true
	test.UpDnLoad()
	//test.TestCodec()
	select {}
}
