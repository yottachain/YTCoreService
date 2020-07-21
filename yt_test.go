package main

import (
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/yottachain/YTCoreService/env"
	"github.com/yottachain/YTCoreService/test"
)

func init() {
	env.Console = true
	env.InitServer()
}

func CatchError(name string) {
	if r := recover(); r != nil {
		logrus.Tracef("[%s]ERR:%s\n", name, r)
	}
}

func Test(t *testing.T) {
	defer CatchError("Test")

	test.TestEOS()
	//test.TestLRC()
}
