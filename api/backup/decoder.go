package api

import (
	"bufio"
	"bytes"
	"io"
	"os"

	"github.com/aurawing/eos-go/btcsuite/btcutil/base58"
	"github.com/yottachain/YTCoreService/codec"
	"github.com/yottachain/YTCoreService/env"
)

type Decoder struct {
	path   string
	length int64
	md5    []byte
	sha    []byte
	ifenc  bool
	reader io.Reader
	file   *os.File
	pos    int64
	readin int64
}

func NewDecoder(p string) (*Decoder, error) {
	de := &Decoder{path: p, readin: 0}
	err := de.readHead()
	if err != nil {
		return nil, err
	}
	return de, nil
}

func (self *Decoder) Close() {
	if self.file != nil {
		self.file.Close()
		self.file = nil
	}
}

func (self *Decoder) HasNextBlock() bool {
	if self.readin >= self.pos {
		return false
	} else {
		return true
	}
}

func (self *Decoder) NextBlock() (interface{}, error) {
	if self.ifenc {
		return nil, nil
	} else {
		return self.NextPlainBlock()
	}
}

func (self *Decoder) NextEncryptedBlock() (interface{}, error) {
	b, err := ReadBool(self.reader)
	if err != nil {
		return nil, err
	}
	self.readin = self.readin + 1
	if b {
		return self.NextDupBlock()
	} else {
		return self.NextNODupBlock()
	}
}

func (self *Decoder) NextNODupBlock() (*NODupBlock, error) {
	ii1, err := ReadInt64(self.reader)
	if err != nil {
		return nil, err
	}
	ii2, err := ReadInt64(self.reader)
	if err != nil {
		return nil, err
	}
	ii3, err := ReadInt64(self.reader)
	if err != nil {
		return nil, err
	}
	bs1:=make([byte,32])
	err = ReadFull(self.reader,bs1)
	if err != nil {
		return nil, err
	}
	bs2:=make([byte,32])
	err = ReadFull(self.reader,bs2)
	if err != nil {
		return nil, err
	}
	bs3:=make([byte,32])
	err = ReadFull(self.reader,bs3)
	if err != nil {
		return nil, err
	}
	bs4:=make([byte,ii3])
	err = ReadFull(self.reader,bs4)
	if err != nil {
		return nil, err
	}
	b := &NODupBlock{OriginalSize: ii1, RealSize: ii2,VHP:bs1,KEU:bs2,KED:bs3,DATA:bs4}
	return b, nil
}

func (self *Decoder) NextDupBlock() (*DupBlock, error) {
	ii1, err := ReadInt64(self.reader)
	if err != nil {
		return nil, err
	}
	ii2, err := ReadInt64(self.reader)
	if err != nil {
		return nil, err
	}
	b := &DupBlock{OriginalSize: ii1, RealSize: ii2}
	bs1:=make([byte,32])
	err = ReadFull(self.reader,bs1)
	if err != nil {
		return nil, err
	}
	bs2:=make([byte,32])
	err = ReadFull(self.reader,bs2)
	if err != nil {
		return nil, err
	}
	bs3:=make([byte,16])
	err = ReadFull(self.reader,bs3)
	if err != nil {
		return nil, err
	}
	b.VHP=bs1
	b.KEU=bs2
	b.VHB=bs3
	self.readin = self.readin + 8 + 8 + 32 + 32 + 16
	return b,nil
}

func (self *Decoder) NextPlainBlock() (*codec.PlainBlock, error) {
	ii1, err := ReadInt64(self.reader)
	if err != nil {
		return nil, err
	}
	size, err := ReadInt64(self.reader)
	if err != nil {
		return nil, err
	}
	bs := make([]byte, size)
	err = ReadFull(self.reader, bs)
	if err != nil {
		return nil, err
	}
	b := codec.NewPlainBlock(bs, ii1)
	self.readin = self.readin + int64(size) + 16
	return b, nil
}

func (self *Decoder) readHead() error {
	f, err := os.OpenFile(self.path, os.O_RDONLY, 0644)
	if err != nil {
		return err
	}
	self.file = f
	shastr := SimpleName(f.Name())
	self.sha = base58.Decode(shastr)
	self.reader = bufio.NewReader(f)
	pos, err := ReadInt64(self.reader)
	if err != nil {
		return err
	}
	self.pos = pos
	ii, err := ReadInt64(self.reader)
	if err != nil {
		return err
	}
	self.length = ii
	b, err := ReadBool(self.reader)
	if err != nil {
		return err
	}
	self.ifenc = b
	bs := make([]byte, 16)
	err = ReadFull(self.reader, bs)
	if err != nil {
		return err
	}
	self.md5 = bs
	self.readin = 8 + 8 + 1 + 16
	return nil
}
