package codec

/*
#cgo LDFLAGS: -lm

#include <stdlib.h>
#include <stdio.h>
#include <math.h>
#include <lrc/YTLRC.h>
#include <lrc/YTLRC.c>
#include <lrc/cm256.h>
#include <lrc/cm256.c>
#include <lrc/gf256.h>
#include <lrc/gf256.c>


void *allocBytes(int size){
	return malloc(size);
}

void *allocArray(int size) {
	return malloc(size * sizeof(void *));
}

void freeArray(void *p) {
	free(p);
}
*/
import "C"

import (
	"errors"
	"runtime"
	"unsafe"

	"github.com/sirupsen/logrus"
	"github.com/yottachain/YTCoreService/env"
)

var NoCGO bool = false

func InitLRC() {
	if BlockDataPool != nil {
		return
	}
	LRCNoCGO := env.GetConfig().GetBool("LRCNoCGO", false)
	sysType := runtime.GOOS
	if sysType == "windows" {
		if LRCNoCGO {
			xbit := 32 << (^uint(0) >> 63)
			if xbit == 32 {
				NoCGO = true
			} else {
				v, err := env.GetWinVersion()
				if err == nil && v == "6.1" {
					NoCGO = true
				}
			}
		}
	}
	s1 := int16(env.LRCInit)
	ret := C.LRC_Initial(C.short(s1))
	if ret <= 0 {
		logrus.Panicf("[LRC]Init ERR,return:%d\n", ret)
	}
	InitPool()
}

type BytesCreator struct{}

func (me *BytesCreator) Create(size int) unsafe.Pointer {
	return C.allocBytes(C.int(size))
}

func (me *BytesCreator) Free(p unsafe.Pointer) {
	C.free(p)
}

type ArrayCreator struct{}

func (me *ArrayCreator) Create(size int) unsafe.Pointer {
	return C.allocArray(C.int(size))
}

func (me *ArrayCreator) Free(p unsafe.Pointer) {
	C.freeArray(p)
}

type LRC_Decoder interface {
	Decode(bs []byte) ([]byte, error)
	GetOut() []byte
	Free()
}

type LRC_Decoder_Linux struct {
	orgsize int64
	handle  unsafe.Pointer
	inptr   []unsafe.Pointer
	outptr  *Pointer
	out     []byte
}

func (me *LRC_Decoder_Linux) GetOut() []byte {
	if me.inptr == nil {
		return me.out
	} else {
		return nil
	}
}

func (me *LRC_Decoder_Linux) Decode(bs []byte) ([]byte, error) {
	if me.inptr == nil {
		return me.out, nil
	}
	inptr := C.CBytes(bs)
	me.inptr = append(me.inptr, inptr)
	ret := C.LRC_Decode(me.handle, inptr)
	osize := int16(ret)
	if osize < 0 {
		me.Free()
		return nil, errors.New("lrc decode ERR")
	}
	if osize > 0 {
		ptr := me.outptr.PTR
		me.out = C.GoBytes(ptr, C.int(me.orgsize))
		me.Free()
		me.inptr = nil
		return me.out, nil
	} else {
		return nil, nil
	}
}

func (me *LRC_Decoder_Linux) Free() {
	if me.handle != nil {
		C.LRC_FreeHandle(me.handle)
		BlockDataPool.BackPointer(me.outptr)
		for _, p := range me.inptr {
			C.free(p)
		}
		me.handle = nil
	}
}

type LRC_Decoder_Win struct {
	orgsize int64
	handle  unsafe.Pointer
	out     []byte
	in      [][]byte
}

func (me *LRC_Decoder_Win) GetOut() []byte {
	if me.in == nil {
		return me.out[0:me.orgsize]
	} else {
		return nil
	}
}

func (me *LRC_Decoder_Win) Decode(bs []byte) ([]byte, error) {
	if me.in == nil {
		return me.out[0:me.orgsize], nil
	}
	inptr := unsafe.Pointer(&bs[0])
	me.in = append(me.in, bs)
	ret := C.LRC_Decode(me.handle, inptr)
	osize := int16(ret)
	if osize < 0 {
		me.Free()
		return nil, errors.New("lrc decode ERR")
	}
	if osize > 0 {
		me.Free()
		me.in = nil
		return me.out[0:me.orgsize], nil
	} else {
		return nil, nil
	}
}

func (me *LRC_Decoder_Win) Free() {
	if me.handle != nil {
		C.LRC_FreeHandle(me.handle)
		me.handle = nil
	}
}

func LRC_Decode(originalCount int64) (LRC_Decoder, error) {
	shardsize := int64(env.PFL - 1)
	shardCount := originalCount / shardsize
	remainSize := originalCount % shardsize
	if remainSize > 0 {
		shardCount++
	}
	if NoCGO {
		o := make([]byte, env.PFL*shardCount)
		outp := unsafe.Pointer(&o[0])
		ret := C.LRC_BeginDecode(C.ushort(shardCount), C.ulong(env.PFL), outp)
		if ret == nil {
			return nil, errors.New("lrc begin decode ERR")
		}
		return &LRC_Decoder_Win{
			orgsize: originalCount,
			handle:  ret,
			in:      [][]byte{},
			out:     o,
		}, nil
	} else {
		outp := BlockDataPool.GetPointer()
		ret := C.LRC_BeginDecode(C.ushort(shardCount), C.ulong(env.PFL), outp.PTR)
		if ret == nil {
			BlockDataPool.BackPointer(outp)
			return nil, errors.New("lrc begin decode ERR")
		}
		return &LRC_Decoder_Linux{
			orgsize: originalCount,
			handle:  ret,
			inptr:   []unsafe.Pointer{},
			outptr:  outp,
		}, nil
	}
}

func LRC_Encode(data [][]byte) ([][]byte, error) {
	if NoCGO {
		return LRC_Encode_Win(data)
	} else {
		return LRC_Encode_Linux(data)
	}
}

const PointNum = 128

func LRC_Encode_Linux(data [][]byte) ([][]byte, error) {
	size := uint16(len(data))
	outsize := env.PFL * env.Default_PND
	parityPoint := BlockParityPool.GetPointer()
	outptr := parityPoint.PTR
	pointArray := PointArrayPool.GetPointer()
	ptrs := pointArray.PTR
	ps := (*[PointNum]unsafe.Pointer)(unsafe.Pointer(ptrs))[:size]
	for ii := 0; ii < int(size); ii++ {
		ps[ii] = C.CBytes(data[ii])
	}
	defer func() {
		for ii := 0; ii < int(size); ii++ {
			C.free(ps[ii])
		}
		PointArrayPool.BackPointer(pointArray)
		BlockParityPool.BackPointer(parityPoint)
	}()
	ret := C.LRC_Encode((*unsafe.Pointer)(ptrs), C.ushort(size), C.ulong(uint64(env.PFL)), outptr)
	osize := int16(ret)
	if osize <= 0 {
		return nil, errors.New("lrc encode ERR")
	}
	out := C.GoBytes(outptr, C.int(outsize))
	pout := make([][]byte, osize)
	for ii := 0; ii < int(osize); ii++ {
		spos := ii * int(env.PFL)
		epos := (ii + 1) * int(env.PFL)
		pout[ii] = out[spos:epos]
	}
	return pout, nil
}

func LRC_Encode_Win(data [][]byte) ([][]byte, error) {
	size := uint16(len(data))
	outsize := env.PFL * env.Default_PND
	out := make([]byte, outsize)
	outptr := unsafe.Pointer(&out[0])
	ptrs := C.allocArray(C.int(size))
	defer C.freeArray(ptrs)
	ps := (*[PointNum]unsafe.Pointer)(unsafe.Pointer(ptrs))[:size]
	for ii := 0; ii < int(size); ii++ {
		ps[ii] = unsafe.Pointer(&data[ii][0])
	}
	ret := C.LRC_Encode((*unsafe.Pointer)(ptrs), C.ushort(size), C.ulong(uint64(env.PFL)), outptr)
	osize := int16(ret)
	if osize <= 0 {
		return nil, errors.New("lrc encode ERR")
	}
	pout := make([][]byte, osize)
	for ii := 0; ii < int(osize); ii++ {
		spos := ii * int(env.PFL)
		epos := (ii + 1) * int(env.PFL)
		pout[ii] = out[spos:epos]
	}
	return pout, nil
}
