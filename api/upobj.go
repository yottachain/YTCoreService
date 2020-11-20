package api

import (
	"sync/atomic"

	"github.com/yottachain/YTCoreService/pkt"
)

type UpProgress struct {
	Length        *int64
	ReadinLength  *int64
	ReadOutLength *int64
	WriteLength   *int64
}

func (self *UpProgress) GetProgress() int32 {
	l1 := atomic.LoadInt64(self.Length)
	l2 := atomic.LoadInt64(self.ReadinLength)
	l3 := atomic.LoadInt64(self.ReadOutLength)
	l4 := atomic.LoadInt64(self.WriteLength)
	if l1 == 0 || l3 == 0 {
		return 0
	}
	p1 := l2 * 100 / l1
	p2 := l4 * 100 / l3
	return int32(p1 * p2 / 100)
}

type UploadObjectBase interface {
	UploadMultiFile(path []string) *pkt.ErrorMessage
	UploadFile(path string) *pkt.ErrorMessage
	UploadBytes(data []byte) *pkt.ErrorMessage
	GetProgress() int32
	GetMD5() []byte
	GetSHA256() []byte
	GetLength() int64
}
