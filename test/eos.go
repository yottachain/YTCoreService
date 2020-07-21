package test

import (
	"fmt"

	ytcrypto "github.com/yottachain/YTCrypto"
)

func TestEOS() bool {
	pub, _ := ytcrypto.GetPublicKeyByPrivateKey("5Kh5MhSNM9zjNwGz1GrC88bat9JptJpAVkeQWVdssAhtVS312hK")

	fmt.Printf("%s:", pub)

	/*
		env.InitServer()
		net.InitShadowPriKey()
		net.EOSInit()
		ii, err := net.GetBalance("username1234aa")
		if err != nil {
			panic(err)
		} else {
			fmt.Printf("%d:", ii)
		}
	*/
	return false
}
