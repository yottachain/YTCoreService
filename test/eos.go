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
	ii, err := net.GetBalance("username1234")
	if err != nil {
		panic(err)
	} else {
		fmt.Printf("balance:%d", ii)
	}

	err = net.Login("username1234", "5J7zkMUwXY52L3MVfiNxVzu8VgkMwF8tybQdj883dADEPfxLrfP")
	//err = net.Login("devtestuser1", "5KTF2yAamvcaoDu6juAvxT5nxTn3UGfNoY2CJn8VAQ4giAfma2a")
	if err != nil {
		panic(err)
	}
	return false
}
