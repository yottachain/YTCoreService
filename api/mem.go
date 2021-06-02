package api

import (
	"sync"
	"sync/atomic"

	"github.com/yottachain/YTCoreService/codec"
	"github.com/yottachain/YTCoreService/env"
)

var MemSize = new(int64)
var MemCond = sync.NewCond(new(sync.Mutex))

func init() {
	*MemSize = 0
}

func AddBlockMen(b *codec.Block) {
	size := len(b.Data)
	//length := atomic.AddInt64(MemSize, int64(size))
	AddMem(int64(size))
}

func AddSyncBlockMen(b *codec.EncodedBlock) {
	if b.DATA == nil {
		return
	}
	size := len(b.DATA)
	//length := atomic.AddInt64(MemSize, int64(size))
	AddMem(int64(size))
}

func DecSyncBlockMen(b *codec.EncodedBlock) {
	if b.DATA != nil {
		size := int64(len(b.DATA))
		atomic.AddInt64(MemSize, -size)
		MemCond.Broadcast()
	}
}

func DecBlockMen(b *codec.Block) {
	if b.Data != nil {
		size := int64(len(b.Data))
		atomic.AddInt64(MemSize, -size)
		MemCond.Broadcast()
	}
}

func AddMem(size int64) {
	for {
		MemCond.L.Lock()
		length := atomic.LoadInt64(MemSize)
		if length >= int64(env.UploadFileMaxMemory) {
			MemCond.Wait()
			MemCond.L.Unlock()
			//length = atomic.LoadInt64(MemSize)
		} else {
			atomic.AddInt64(MemSize, size)
			MemCond.L.Unlock()
			break
		}
	}
}

func AddEncoderMem(enc *codec.ErasureEncoder) int64 {
	var size int64
	if enc.IsCopyShard() {
		size = int64(env.PFL + 16)
	} else {
		size = int64((env.PFL + 16) * len(enc.Shards))
	}
	//length := atomic.AddInt64(MemSize, size)
	AddMem(size)
	return size
}

func DecMen(length int64) {
	atomic.AddInt64(MemSize, -length)
	MemCond.Broadcast()
}
