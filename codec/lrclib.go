package codec

/*
#cgo LDFLAGS: -lm
#include <lrc/YTLRC.h>
#include <lrc/YTLRC.c>
#include <lrc/cm256.h>
#include <lrc/cm256.c>
#include <lrc/gf256.h>
#include <lrc/gf256.c>

void *allocBytes(int size){
	return malloc(size);
}

void **allocArray(int size) {
	return malloc(size * sizeof(void *));
}

void freeArray(void **p,int size) {
	for (int i = 0; i < size; i++) {
		free(p[i]);
	}
	free(p);
}
*/
import "C"

import (
	"errors"
	"unsafe"

	"github.com/sirupsen/logrus"
	"github.com/yottachain/YTCoreService/env"
)

func InitLRC() {
	s1 := C.short(int16(env.Default_PND - 23))
	ret := C.LRC_Initial(s1)
	if ret <= 0 {
		logrus.Panicf("[LRC]Init ERR,return:%d\n", ret)
	}
}

type LRC_Decoder struct {
	orgsize int64
	handle  unsafe.Pointer
	inptr   []unsafe.Pointer
	outptr  unsafe.Pointer
	out     []byte
}

func (me *LRC_Decoder) GetOut() []byte {
	if me.inptr == nil {
		return me.out
	} else {
		return nil
	}
}

func (me *LRC_Decoder) Decode(bs []byte) ([]byte, error) {
	if me.inptr == nil {
		return me.out[0:me.orgsize], nil
	}
	inptr := C.CBytes(bs)
	ret := C.LRC_Decode(me.handle, inptr)
	osize := int16(ret)
	if osize < 0 {
		me.Free()
		return nil, errors.New("LRC decode ERR.")
	}
	if osize > 0 {
		me.out = C.GoBytes(me.outptr, C.int(me.orgsize))
		me.Free()
		me.inptr = nil
		return me.out, nil
	} else {
		me.inptr = append(me.inptr, inptr)
		return nil, nil
	}
}

func (me *LRC_Decoder) Free() {
	if me.handle != nil {
		C.LRC_FreeHandle(me.handle)
		C.free(me.outptr)
		for _, p := range me.inptr {
			C.free(p)
		}
		me.handle = nil
	}
}

func LRC_Decode(originalCount int64) (*LRC_Decoder, error) {
	shardsize := int64(env.PFL - 1)
	shardCount := originalCount / shardsize
	remainSize := originalCount % shardsize
	if remainSize > 0 {
		shardCount++
	}
	outp := C.allocBytes(C.int(env.PFL * shardCount))
	ret := C.LRC_BeginDecode(C.ushort(shardCount), C.ulong(env.PFL), outp)
	if ret == nil {
		return nil, errors.New("LRC begin decode ERR.")
	}
	return &LRC_Decoder{
		orgsize: originalCount,
		handle:  unsafe.Pointer(ret),
		inptr:   []unsafe.Pointer{},
		outptr:  outp,
	}, nil
}

func LRC_Encode(data [][]byte) ([][]byte, error) {
	size := uint16(len(data))
	outsize := env.PFL * env.Default_PND
	outptr := C.allocBytes(C.int(outsize))
	ptrs := C.allocArray(C.int(size))
	ps := (*[env.Max_Shard_Count]unsafe.Pointer)(unsafe.Pointer(ptrs))[:size]
	for ii := 0; ii < int(size); ii++ {
		ps[ii] = C.CBytes(data[ii])
	}
	defer func() {
		C.freeArray(ptrs, C.int(size))
		C.free(outptr)
	}()
	ret := C.LRC_Encode((*unsafe.Pointer)(ptrs), C.ushort(size), C.ulong(uint64(env.PFL)), outptr)
	osize := int16(ret)
	if osize <= 0 {
		return nil, errors.New("LRC encode ERR.")
	}
	out := C.GoBytes(outptr, C.int(outsize))
	pout := make([][]byte, osize)
	for ii := 0; ii < int(osize); ii++ {
		spos := ii * env.PFL
		epos := (ii + 1) * env.PFL
		pout[ii] = out[spos:epos]
	}
	return pout, nil
}
