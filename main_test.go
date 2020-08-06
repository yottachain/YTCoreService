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

	test.UpDnLoad()
	//test.TestLRC()
}
