package main

import (
	"fmt"
	"math/rand"
	"os"
	"sync"
	"sync/atomic"
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

func call() {
	atomicint := env.NewAtomInt64(1)
	for ii := 0; ii < 10; ii++ {
		i := atomicint.Add(10)
		fmt.Println(i)
		ii := atomicint.Add(-5)
		fmt.Println(ii)
	}
}

var count *int64 = new(int64)

func add() {
	for ii := 0; ii < 10; ii++ {
		fmt.Printf("add,len=%d\n", atomic.AddInt64(count, 1))
		t := rand.Intn(40)
		AddMem(int64(t))
		time.Sleep(time.Duration(t) * time.Millisecond)
		DecMen(int64(t))
		fmt.Printf("dec,len=%d\n", atomic.AddInt64(count, -1))
	}
}

func AddMem(length int64) {
	MemCond.L.Lock()
	for MemSize+length >= int64(MaxSize) {
		MemCond.Wait()
	}
	MemSize = MemSize + length
	if MemSize < int64(MaxSize) {
		MemCond.Signal()
	}
	MemCond.L.Unlock()
}

func DecMen(length int64) {
	MemCond.L.Lock()
	MemSize = MemSize - length
	MemCond.Signal()

	MemCond.L.Unlock()
}
