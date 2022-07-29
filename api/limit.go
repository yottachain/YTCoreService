package api

import (
	"sync"
	"time"

	"github.com/yottachain/YTCoreService/env"
)

const QUEUE_SIZE = 32800

var QUEUE *Queue

type Queue struct {
	sync.Mutex
	array [QUEUE_SIZE]int64
	pos   int
}

func NewQueue() {
	QUEUE = &Queue{}
	if env.CriticalTime >= 0 {
		go QUEUE.loop()
	}
}

func (q *Queue) add(t int64) {
	q.Lock()
	defer q.Unlock()
	if q.pos >= QUEUE_SIZE {
		q.pos = 0
	}
	q.array[q.pos] = t
	q.pos++

}

func (q *Queue) loop() {
	for {
		time.Sleep(time.Second * 5)
		q.check()
	}

}

func (q *Queue) check() {

}

func SetOK(t int64) {
	if env.CriticalTime == 0 {
		return
	}
	QUEUE.add(t)
}
