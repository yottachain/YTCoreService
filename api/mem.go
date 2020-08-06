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
	length := atomic.AddInt64(MemSize, int64(size))
	AddMem(length)
}

func AddMem(length int64) {
	for {
		if length >= int64(env.UploadFileMaxMemory) {
			MemCond.L.Lock()
			MemCond.Wait()
			MemCond.L.Unlock()
			length = atomic.LoadInt64(MemSize)
		} else {
			break
		}
	}
}

func AddEncoderMem(enc *codec.ErasureEncoder) int64 {
	var size int
	if enc.IsCopyShard() {
		size = env.PFL + 16
	} else {
		size = (env.PFL + 16) * len(enc.Shards)
	}
	length := atomic.AddInt64(MemSize, int64(size))
	AddMem(length)
	return length
}

func DecMen(length int64) {
	atomic.AddInt64(MemSize, -length)
	MemCond.Broadcast()
}

func DecBlockMen(b *codec.Block) {
	if b.Data != nil {
		size := int64(len(b.Data))
		atomic.AddInt64(MemSize, -size)
		MemCond.Broadcast()
	}
}
