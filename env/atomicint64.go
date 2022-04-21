package env

import (
	"sync/atomic"
	"unsafe"
)

type AtomInt64 struct {
	x []byte
	p *int64
}

func NewAtomInt64(inivalue int64) *AtomInt64 {
	atom := &AtomInt64{}
	xbit := 32 << (^uint(0) >> 63)
	if xbit == 32 {
		atom.x = make([]byte, 15)
		atom.Set(inivalue)
	} else {
		atom.p = &inivalue
	}
	return atom
}

func (a *AtomInt64) xAddr() *int64 {
	if a.p != nil {
		return a.p
	} else {
		pi := unsafe.Pointer((uintptr(unsafe.Pointer(&a.x)) + 7) / 8 * 8)
		return (*int64)(pi)
	}
}

func (a *AtomInt64) Set(inivalue int64) {
	atomic.StoreInt64(a.xAddr(), inivalue)
}

func (a *AtomInt64) Add(delta int64) int64 {
	return atomic.AddInt64(a.xAddr(), delta)
}

func (a *AtomInt64) Value() int64 {
	return atomic.LoadInt64(a.xAddr())
}
