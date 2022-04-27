package env

import (
	"sync"
	"sync/atomic"
)

type AtomInt64 struct {
	sync.RWMutex
	v int64
	p *int64
}

func NewAtomInt64(inivalue int64) *AtomInt64 {
	atom := &AtomInt64{}
	xbit := 32 << (^uint(0) >> 63)
	if xbit == 32 {
		atom.v = inivalue
	} else {
		atom.p = &inivalue
	}
	return atom
}

func (a *AtomInt64) Set(inivalue int64) {
	if a.p != nil {
		atomic.StoreInt64(a.p, inivalue)
	} else {
		a.Lock()
		a.v = inivalue
		a.Unlock()
	}
}

func (a *AtomInt64) Add(delta int64) int64 {
	if a.p != nil {
		return atomic.AddInt64(a.p, delta)
	} else {
		a.Lock()
		defer a.Unlock()
		a.v = a.v + delta
		return a.v
	}
}

func (a *AtomInt64) Value() int64 {
	if a.p != nil {
		return atomic.LoadInt64(a.p)
	} else {
		a.RLock()
		defer a.RUnlock()
		return a.v
	}
}
