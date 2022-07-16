package codec

import (
	"container/list"
	"sync"
	"time"
	"unsafe"

	"github.com/yottachain/YTCoreService/env"
)

const ExpiredTime = 60

var BlockDataPool *PointPool
var BlockParityPool *PointPool
var PointArrayPool *PointPool

func InitPool() {
	BlockDataPool = NewPointPool(env.PFL*env.Max_Shard_Count, &BytesCreator{})
	BlockParityPool = NewPointPool(env.PFL*env.Default_PND, &BytesCreator{})
	PointArrayPool = NewPointPool(env.Max_Shard_Count, &ArrayCreator{})
}

type PointCreater interface {
	Create(size int) unsafe.Pointer
	Free(p unsafe.Pointer)
}

type Pointer struct {
	PTR     unsafe.Pointer
	usetime int64
}

type PointPool struct {
	sync.Mutex
	blockSize int
	pool      *list.List
	create    PointCreater
}

func NewPointPool(size int, pc PointCreater) *PointPool {
	pool := &PointPool{blockSize: size, pool: list.New(), create: pc}
	go pool.ClearLoop()
	return pool
}

func (me *PointPool) GetPointer() *Pointer {
	var p *Pointer
	me.Lock()
	defer me.Unlock()
	e := me.pool.Back()
	if e != nil {
		me.pool.Remove(e)
		p = e.Value.(*Pointer)
		p.usetime = time.Now().Unix()
	} else {
		up := me.create.Create(me.blockSize)
		p = &Pointer{PTR: up, usetime: time.Now().Unix()}
	}
	return p
}

func (me *PointPool) BackPointer(p *Pointer) {
	me.Lock()
	defer me.Unlock()
	me.pool.PushBack(p)
}

func (me *PointPool) ClearLoop() {
	for {
		time.Sleep(ExpiredTime * time.Second)
		me.Clear()
	}
}

func (me *PointPool) Clear() {
	me.Lock()
	defer me.Unlock()
	for {
		e := me.pool.Front()
		if e != nil {
			p := e.Value.(*Pointer)
			if time.Now().Unix()-p.usetime > ExpiredTime {
				me.pool.Remove(e)
				me.create.Free(p.PTR)
			} else {
				break
			}
		} else {
			break
		}
	}
}
