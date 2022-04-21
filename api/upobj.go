package api

import (
	"github.com/yottachain/YTCoreService/env"
	"github.com/yottachain/YTCoreService/pkt"
)

type UpProgress struct {
	Length        *env.AtomInt64
	ReadinLength  *env.AtomInt64
	ReadOutLength *env.AtomInt64
	WriteLength   *env.AtomInt64
}

func (self *UpProgress) GetProgress() int32 {
	l1 := self.Length.Value()
	l2 := self.ReadinLength.Value()
	l3 := self.ReadOutLength.Value()
	l4 := self.WriteLength.Value()
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
