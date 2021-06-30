package main

import (
	"fmt"
	"math/rand"
	"os"
	"sync"
	"testing"
	"time"

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

var MemCond = sync.NewCond(new(sync.Mutex))
var MemSize int64 = 0
var MaxSize int64 = 50

func call() {
	for ii := 0; ii < 10; ii++ {
		go add()
	}

}

func add() {
	for {
		t := rand.Intn(20)
		AddMem(int64(t))
		time.Sleep(time.Duration(1) * time.Millisecond)
		DecMen(int64(t))
	}
}

func AddMem(length int64) {
	MemCond.L.Lock()
	for MemSize+length >= int64(MaxSize) {
		MemCond.Wait()
	}
	MemSize = MemSize + length
	fmt.Printf("add,len=%d\n", MemSize)
	MemCond.L.Unlock()
}

func DecMen(length int64) {
	MemCond.L.Lock()
	MemSize = MemSize - length
	MemCond.Signal()
	fmt.Printf("dec,len=%d\n", MemSize)
	MemCond.L.Unlock()
}
