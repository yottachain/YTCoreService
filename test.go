//go:build ignore
// +build ignore

package main

import (
	"net/http"

	"github.com/yottachain/YTCoreService/test"
)

func main() {
	go func() {
		http.ListenAndServe("0.0.0.0:8088", nil)
	}()
	//env.Console = true
	test.UpAndDown()
	select {}
}
