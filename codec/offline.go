package codec

import (
	"io"
	"path/filepath"

	"github.com/yottachain/YTCoreService/env"
	"github.com/yottachain/YTCoreService/pkt"
)

type DupBlockChecker interface {
	Check(b *PlainBlock, id int) (*EncodedBlock, *pkt.ErrorMessage)
}

type EncodedBlock struct {
	OriginalSize int64
	RealSize     int64
	VHP          []byte
	KEU          []byte
	IsDup        bool
	VHB          []byte
	KED          []byte
	DATA         []byte
}

func (eb *EncodedBlock) Length() int64 {
	if eb.IsDup {
		return 0
	} else {
		return int64(len(eb.DATA))
	}
}

func ReadInt64(f io.Reader) (int64, error) {
	bs := make([]byte, 8)
	err := ReadFull(f, bs)
	if err != nil {
		return 0, err
	}
	i := env.BytesToId(bs)
	return i, nil
}

func ReadInt32(f io.Reader) (int32, error) {
	bs := make([]byte, 4)
	err := ReadFull(f, bs)
	if err != nil {
		return 0, err
	}
	i := env.BytesToInt32(bs)
	return i, nil
}

func ReadBool(f io.Reader) (bool, error) {
	bs := make([]byte, 1)
	err := ReadFull(f, bs)
	if err != nil {
		return false, err
	}
	if bs[0] == 0x00 {
		return false, nil
	} else {
		return true, nil
	}
}

func ReadFull(r io.Reader, bs []byte) error {
	size := len(bs)
	pos := 0
	for {
		n, err := r.Read(bs[pos:])
		pos = pos + n
		if pos == size {
			break
		}
		if err != nil {
			return err
		}
	}
	return nil
}

func SimpleName(name string) string {
	return filepath.Base(name)
}
