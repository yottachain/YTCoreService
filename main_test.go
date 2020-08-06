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

	//***********api test*********
	test.UpAndDown()

	//**********code test**********
	//env.InitClient()
	//test.TestCut
	//test.TestLRC()
}
