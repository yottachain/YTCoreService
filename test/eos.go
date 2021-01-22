package test

import (
	"fmt"

	"github.com/yottachain/YTCoreService/env"
	"github.com/yottachain/YTCoreService/net"
	ytcrypto "github.com/yottachain/YTCrypto"
)

func TestEOS() bool {
	env.InitServer()
	net.InitShadowPriKey()
	net.EOSInit()
	publickey, _ := ytcrypto.GetPublicKeyByPrivateKey("5JReF8eeGS53B8prdcrSfTf6dGbvu3QJ6KceE8rLsnRaNMMCYw9")
	b := net.AuthUserInfo(publickey, "devvtest1111", 1)
	fmt.Print("pass:", b)
	return false
}
