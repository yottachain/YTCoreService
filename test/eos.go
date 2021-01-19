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
	//ii, err := net.GetBalance("i5baoguxctpi")
	//ii, err := net.GetBalance("pollyzhang11")
	//if err != nil {
	//	panic(err)
	//} else {
	//	fmt.Printf("balance:%d", ii)
	//}
	publickey, _ := ytcrypto.GetPublicKeyByPrivateKey("5JVwTWuJWcmXy22f12YzjjpKiiqQyJnqoSjx4Mk2JxtgQYAb3Fw")
	//		user = "pollyzhang11"
	//pkey = "5JVwTWuJWcmXy22f12YzjjpKiiqQyJnqoSjx4Mk2JxtgQYAb3Fw"
	ss, err := net.GetUserInfoWRetry(publickey, 1)
	if err != nil {
		panic(err)
	} else {
		fmt.Printf("result:%s", ss)
	}
	return false
}
