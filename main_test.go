package main

import (
	"os"
	"testing"

	"github.com/yottachain/YTCoreService/env"
)

func Test(t *testing.T) {
	defer env.TracePanic("Test")
	if len(os.Args) > 1 {
		if os.Args[1] == "makeconst" {
			Make()
		}
	}
	select {}
}
