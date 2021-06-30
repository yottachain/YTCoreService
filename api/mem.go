package api

import (
	"sync"

	"github.com/yottachain/YTCoreService/codec"
	"github.com/yottachain/YTCoreService/env"
)

var MemSize int64 = 0
var MemCond = sync.NewCond(new(sync.Mutex))

func AddBlockMen(b *codec.Block) {
	size := len(b.Data)
	AddMem(int64(size))
}

func AddSyncBlockMen(b *codec.EncodedBlock) {
	if b.DATA == nil {
		return
	}
	size := len(b.DATA)
	AddMem(int64(size))
}

func DecSyncBlockMen(b *codec.EncodedBlock) {
	if b.DATA != nil {
		size := int64(len(b.DATA))
		DecMen(size)
	}
}

func DecBlockMen(b *codec.Block) {
	if b.Data != nil {
		size := int64(len(b.Data))
		DecMen(size)
	}
}

func AddEncoderMem(enc *codec.ErasureEncoder) int64 {
	var size int64
	if enc.IsCopyShard() {
		size = int64(env.PFL + 16)
	} else {
		size = int64((env.PFL + 16) * len(enc.Shards))
	}
	AddMem(int64(size))
	return size
}

func AddMem(length int64) {
	MemCond.L.Lock()
	for MemSize+length >= int64(env.UploadFileMaxMemory) {
		MemCond.Wait()
	}
	MemSize = MemSize + length
	MemCond.L.Unlock()
}

func DecMen(length int64) {
	MemCond.L.Lock()
	MemSize = MemSize - length
	MemCond.Signal()
	MemCond.L.Unlock()
}
