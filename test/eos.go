package test

import (
	"fmt"

	"github.com/yottachain/YTCoreService/env"
	"github.com/yottachain/YTCoreService/net"
)

func TestEOS() bool {
	env.InitServer()
	net.InitShadowPriKey()
	net.EOSInit()
	//ii, err := net.GetBalance("i5baoguxctpi")
	ii, err := net.GetBalance("pollyzhang11")
	if err != nil {
		panic(err)
	} else {
		fmt.Printf("%d:", ii)
	}
	return false
}
