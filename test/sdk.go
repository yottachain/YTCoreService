package test

import (
	"fmt"
	"sync/atomic"
	"time"
)

var nodes chan string
var sign *int32 = new(int32)

func Test() {
	*sign = 0
	nodes = make(chan string)
	go PutNodeStat()
	go GetPutNodeStat()

	select {}
}

func GetPutNodeStat() {
	for {
		time.Sleep(time.Duration(5) * time.Second)
		n, ok := <-nodes
		if ok {
			fmt.Printf("get:%s\n", n)
		} else {
			fmt.Printf("exit GetPutNodeStat\n")
			break
		}
	}
}
func PutNodeStat() {
	ss := []string{"a", "b", "c", "a", "b", "c", "a", "b", "c", "a", "b", "c", "a", "b", "c", "a", "b", "c"}
	for _, n := range ss {
		timeout := time.After(time.Second * 3)
		select {
		case nodes <- n:
			fmt.Printf("put:%s\n", n)
			break
		case <-timeout:
			fmt.Printf("timeout\n")
			if atomic.LoadInt32(sign) == 1 {
				goto lable
			}
		}
	}
lable:
	close(nodes)
	fmt.Printf("exit PutNodeStat\n")
}
