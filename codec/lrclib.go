package codec

import (
	"errors"
	"runtime"
	"syscall"
	"unsafe"

	"github.com/sirupsen/logrus"
	"github.com/yottachain/YTCoreService/env"
)

var (
	LRC_Initial_PROC     *syscall.LazyProc
	LRC_Encode_PROC      *syscall.LazyProc
	LRC_BeginDecode_PROC *syscall.LazyProc
	LRC_Decode_PROC      *syscall.LazyProc
	LRC_FreeHandle_PROC  *syscall.LazyProc
)

func InitLRC() {
	var LRC *syscall.LazyDLL
	sysType := runtime.GOOS
	if sysType == "windows" {
		LRC = syscall.NewLazyDLL("lib/lrc.dll")
	} else {
		LRC = syscall.NewLazyDLL("lib/lrc.so")
	}
	LRC_Initial_PROC = LRC.NewProc("LRC_Initial")
	LRC_Encode_PROC = LRC.NewProc("LRC_Encode")
	LRC_BeginDecode_PROC = LRC.NewProc("LRC_BeginDecode")
	LRC_Decode_PROC = LRC.NewProc("LRC_Decode")
	LRC_FreeHandle_PROC = LRC.NewProc("LRC_FreeHandle")
	LRC_Initial(int16(env.Default_PND-23), int16(env.LRCMAXHANDLERS))
}

func LRC_Initial(globalRecoveryCount, maxHandles int16) {
	ret, _, err := LRC_Initial_PROC.Call(uintptr(globalRecoveryCount), uintptr(maxHandles))
	if err != nil {
		logrus.Infof("[LRC]Init result:%s\n", err)
	}
	if int16(ret) <= 0 {
		logrus.Panicf("[LRC]Init ERR,return:%d\n", int16(ret))
	}
}

func LRC_Encode(data [][]byte) ([][]byte, error) {
	size := uint16(len(data))
	ptrs := make([]uintptr, size)
	for ii := 0; ii < int(size); ii++ {
		aa := unsafe.Pointer(&data[ii][0])
		ptrs[ii] = uintptr(aa)
	}
	outsize := env.PFL * env.Default_PND
	out := make([]byte, outsize)
	outptr := uintptr(unsafe.Pointer(&out[0]))
	ret, _, _ := LRC_Encode_PROC.Call(uintptr(unsafe.Pointer(&ptrs[0])), uintptr(size), uintptr(uint64(env.PFL)), outptr)
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

func LRC_Decode(originalCount int64) (*LRC_Decoder, error) {
	shardsize := int64(env.PFL - 1)
	shardCount := originalCount / shardsize
	remainSize := originalCount % shardsize
	if remainSize > 0 {
		shardCount++
	}
	bs := make([]byte, env.PFL*shardCount)
	outptr := uintptr(unsafe.Pointer(&bs[0]))
	ret, _, _ := LRC_BeginDecode_PROC.Call(uintptr(uint16(shardCount)), uintptr(uint64(env.PFL)), outptr)
	r := int16(ret)
	if r < 0 {
		return nil, errors.New("LRC begin decode ERR.")
	}
	return &LRC_Decoder{orgsize: originalCount, handle: r, out: bs, in: [][]byte{}}, nil
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
	inptr := uintptr(unsafe.Pointer(&bs[0]))
	ret, _, _ := LRC_Decode_PROC.Call(uintptr(me.handle), inptr)
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
		LRC_FreeHandle_PROC.Call(uintptr(me.handle))
		me.handle = -1
	}
}
