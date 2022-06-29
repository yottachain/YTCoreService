package main

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/yottachain/YTCoreService/env"
	"github.com/yottachain/YTCoreService/test"
)

func init() {
	env.Console = false
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

var MemCond = sync.NewCond(new(sync.Mutex))
var MemSize int64 = 0
var MaxSize int64 = 50

func stest(Lock chan int) error {
	time.Sleep(5 * time.Second)
	Lock <- 1
	//fmt.Printf("stest %d\n", ii)
	return nil
}
func call() {
	Lock := make(chan int, 1)
	timeout := time.Second * time.Duration(10)
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	go stest(Lock)

	time.Sleep(6 * time.Second)
	for {
		select {
		case <-ctx.Done():
			fmt.Println("timeout")
			cancel()

		case <-Lock:
			fmt.Println("OK")
		}
		time.Sleep(time.Second)
	}

}
