package test

import (
	"fmt"

	"github.com/yottachain/YTCoreService/env"
	"github.com/yottachain/YTCoreService/net"
)

func TestEOS() {
	env.InitServer()
	net.InitShadowPriKey()
	net.EOSInit()
	ii, err := net.GetBalance("username1234")
	if err != nil {
		panic(err)
	} else {
		fmt.Printf("%d:", ii)
	}
}
