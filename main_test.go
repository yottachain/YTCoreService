package main

import (
	"fmt"
	"testing"

	"github.com/yottachain/YTCoreService/env"
	"github.com/yottachain/YTCoreService/test"
)

func init() {
	env.Console = true

}

func CatchError(name string) {
	if r := recover(); r != nil {
		fmt.Printf("[%s]ERR:%s\n", name, r)
	}
}

func Test(t *testing.T) {
	defer CatchError("Test")
	//env.InitServer()

	env.InitClient()

	test.Upload()
	//test.TestCodec()
	select {}
}
