package main

import (
	"fmt"
	"os"
	"testing"

	"github.com/yottachain/YTCoreService/env"
	"github.com/yottachain/YTCoreService/test"
)

func init() {
	env.Console = true
}

func Test(t *testing.T) {
	defer env.TracePanic("Test")
	if len(os.Args) > 1 {
		if os.Args[1] == "makeconst" {
			Make()
		} else if os.Args[1] == "auth" {
			test.Auth()
		} else if os.Args[1] == "codec" {
			test.Codec()
		} else if os.Args[1] == "eos" {
			test.EOS()
		} else if os.Args[1] == "sync" {
			test.Sync()
		} else if os.Args[1] == "s3" {
			test.S3()
		} else if os.Args[1] == "up&down" {
			test.UpAndDown()
		}
		return
	}
	call()
	select {}
}

func call() {

	var ii int64 = 6961375064894061206
	bs := env.IdToBytes(ii)
	fmt.Println(bs[4])

	ii = 6961374983020798451
	bs = env.IdToBytes(ii)
	fmt.Println(bs[4])

	ii = 6961375399579267679
	bs = env.IdToBytes(ii)
	fmt.Println(bs[4])

	ii = 6961375330859773087
	bs = env.IdToBytes(ii)
	fmt.Println(bs[4])

	ii = 6961375399579267679
	bs = env.IdToBytes(ii)
	fmt.Println(bs[4])

}
