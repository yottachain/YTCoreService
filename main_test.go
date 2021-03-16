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
	dd := -63
	fmt.Print(uint64(dd))
}
