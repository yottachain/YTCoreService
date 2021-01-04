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
	ii, err := net.GetBalance("wendytest123")
	if err != nil {
		panic(err)
	} else {
		fmt.Printf("balance:%d", ii)
	}
	err = net.Login("wendytest123", "5Hy2inA8sZ1Ygk3SWSmF9UegqZ7A2eMyC65W62Y6i6aQ9ry7egf")
	//err = net.Login("devtestuser1", "5KTF2yAamvcaoDu6juAvxT5nxTn3UGfNoY2CJn8VAQ4giAfma2a")
	if err != nil {
		panic(err)
	}

	tx, err := net.LoginInfo("wendytest123", "5Hy2inA8sZ1Ygk3SWSmF9UegqZ7A2eMyC65W62Y6i6aQ9ry7egf")
	if err != nil {
		panic(err)
	}
	err = net.PushLogin(tx)
	if err != nil {
		panic(err)
	}

	return false
}
