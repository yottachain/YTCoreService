package main

import (
	"os"
	"testing"

	"github.com/yottachain/YTCoreService/env"
	"github.com/yottachain/YTCoreService/examples"
)

func Test(t *testing.T) {
	env.Console = false
	defer env.TracePanic("Test")
	if len(os.Args) > 1 {
		if os.Args[1] == "makeconst" {
			Make()
		} else if os.Args[1] == "auth" {
			examples.Auth()
		} else if os.Args[1] == "sync" {
			examples.Sync()
		} else if os.Args[1] == "s3" {
			examples.S3()
		}
	}
	select {}
}
