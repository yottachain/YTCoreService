package main

import (
	"fmt"
	"testing"

	"github.com/yottachain/YTCoreService/env"
	"github.com/yottachain/YTCoreService/test"
)

func CatchError(name string) {
	if r := recover(); r != nil {
		fmt.Printf("[%s]ERR:%s\n", name, r)
	}
}

func Test(t *testing.T) {
	defer CatchError("Test")
	//env.InitServer()

	env.Console = true
	test.Upload()
	//test.TestCodec()
	select {}
}
