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

func DecBlockMen(b *codec.Block) {
	size := int64(len(b.Data))
	atomic.AddInt64(MemSize, -size)
	MemCond.Broadcast()
}

func DecShardMem(shd *codec.Shard) {
	if !shd.IsCopyShard() {
		shd.Clear()
		atomic.AddInt64(MemSize, -env.PFL)
		MemCond.Broadcast()
	}
}

func DecCopyShardMem(shd *codec.Shard) {
	shd.Clear()
	atomic.AddInt64(MemSize, -env.PFL)
	MemCond.Broadcast()
}
