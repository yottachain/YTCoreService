package api

import (
	"sync/atomic"

	"github.com/yottachain/YTCoreService/codec"
	"github.com/yottachain/YTCoreService/env"
)

var MemSize *int64

func init() {
	MemSize = new(int64)
	*MemSize = 0
}

func DecMem(shd *codec.Shard) {
	if !shd.IsCopyShard() {
		shd.Clear()
		atomic.AddInt64(MemSize, env.PFL)
	}
}

func DecCopyShardMem(shd *codec.Shard) {
	shd.Clear()
	atomic.AddInt64(MemSize, env.PFL)
}
