package codec

/*
#cgo LDFLAGS: -lm
#include <lrc/YTLRC.h>
#include <lrc/YTLRC.c>
#include <lrc/cm256.h>
#include <lrc/cm256.c>
#include <lrc/gf256.h>
#include <lrc/gf256.c>

void* allocArray(size_t ln) {
	return (void*) malloc(ln * sizeof(void*));
}

void freeArr(void* p) {
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
	s1 := int16(env.Default_PND - 23)
	s2 := int16(env.LRCMAXHANDLERS)
	ret := C.LRC_Initial(C.short(s1), C.short(s2))
	if ret <= 0 {
		logrus.Panicf("[LRC]Init ERR,return:%d\n", ret)
	}
}

type LRC_Decoder struct {
	orgsize int64
	handle  int16
	out     []byte
	in      [][]byte
}

func (me *LRC_Decoder) GetOut() []byte {
	if me.in == nil {
		return me.out[0:me.orgsize]
	} else {
		return nil
	}
}

func (me *LRC_Decoder) Decode(bs []byte) ([]byte, error) {
	if me.in == nil {
		return me.out[0:me.orgsize], nil
	}
	inptr := unsafe.Pointer(&bs[0])
	ret := C.LRC_Decode(C.short(me.handle), inptr)
	osize := int16(ret)
	if osize < 0 {
		me.Free()
		return nil, errors.New("LRC decode ERR.")
	}
	if osize > 0 {
		me.Free()
		me.in = nil
		return me.out[0:me.orgsize], nil
	} else {
		me.in = append(me.in, bs)
		return nil, nil
	}
}

func (me *LRC_Decoder) Free() {
	if me.handle >= 0 {
		C.LRC_FreeHandle(C.short(me.handle))
		me.handle = -1
	}
}

func LRC_Decode(originalCount int64) (*LRC_Decoder, error) {
	shardsize := int64(env.PFL - 1)
	shardCount := originalCount / shardsize
	remainSize := originalCount % shardsize
	if remainSize > 0 {
		shardCount++
	}
	bs := make([]byte, env.PFL*shardCount)
	outptr := unsafe.Pointer(&bs[0])
	ret := C.LRC_BeginDecode(C.ushort(shardCount), C.ulong(env.PFL), outptr)
	r := int16(ret)
	if r < 0 {
		return nil, errors.New("LRC begin decode ERR.")
	}
	return &LRC_Decoder{orgsize: originalCount, handle: r, out: bs, in: [][]byte{}}, nil
}

func LRC_Encode(data [][]byte) ([][]byte, error) {
	size := uint16(len(data))
	ptrs := C.allocArray(C.size_t(size))
	defer C.freeArr(ptrs)
	ps := (*[100000]unsafe.Pointer)(unsafe.Pointer(ptrs))[:size:size]
	for ii := 0; ii < int(size); ii++ {
		ps[ii] = unsafe.Pointer(&data[ii][0])
	}
	outsize := env.PFL * env.Default_PND
	out := make([]byte, outsize)
	outptr := unsafe.Pointer(&out[0])
	ret := C.LRC_Encode((*unsafe.Pointer)(ptrs), C.ushort(size), C.ulong(uint64(env.PFL)), outptr)
	osize := int16(ret)
	if osize <= 0 {
		return nil, errors.New("LRC encode ERR.")
	}
	pout := make([][]byte, osize)
	for ii := 0; ii < int(osize); ii++ {
		spos := ii * env.PFL
		epos := (ii + 1) * env.PFL
		pout[ii] = out[spos:epos]
	}
	return pout, nil
}
