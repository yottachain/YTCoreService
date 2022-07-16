package codec

import (
	"bufio"
	"io"
	"os"

	"github.com/aurawing/eos-go/btcsuite/btcutil/base58"
)

type Decoder struct {
	path         string
	length       int64
	md5          []byte
	sha          []byte
	reader       io.Reader
	file         *os.File
	pos          int64
	readin       int64
	UserId       uint32
	KeyNumber    uint32
	StoreNumber  uint32
	Sign         string
	readinTotal  int64
	readoutTotal int64
}

func NewDecoder(p string) (*Decoder, error) {
	de := &Decoder{path: p, readin: 0, readinTotal: 0, readoutTotal: 0}
	err := de.readHead()
	if err != nil {
		de.Close()
		return nil, err
	}
	return de, nil
}

func (dec *Decoder) GetPath() string {
	return dec.path
}

func (dec *Decoder) GetLength() int64 {
	return dec.length
}

func (dec *Decoder) GetMD5() []byte {
	return dec.md5
}

func (dec *Decoder) GetVHW() []byte {
	return dec.sha
}

func (dec *Decoder) GetReadinTotal() int64 {
	return dec.readinTotal
}

func (dec *Decoder) GetReadoutTotal() int64 {
	return dec.readoutTotal
}

func (dec *Decoder) Close() {
	if dec.file != nil {
		dec.file.Close()
		dec.file = nil
	}
}

func (dec *Decoder) ReadNextKey() (string, error) {
	if dec.readin < dec.pos {
		dec.Close()
		f, err := os.OpenFile(dec.path, os.O_RDONLY, 0644)
		if err != nil {
			return "", err
		}
		dec.file = f
		_, err = dec.file.Seek(dec.pos, io.SeekStart)
		if err != nil {
			return "", err
		}
		dec.readin = dec.pos
		dec.reader = bufio.NewReader(dec.file)
	}
	ii, err := ReadInt32(dec.reader)
	if err != nil {
		if err == io.EOF {
			return "", nil
		}
		return "", err
	}
	bs := make([]byte, ii)
	err = ReadFull(dec.reader, bs)
	if err != nil {
		return "", err
	}
	dec.readin = dec.readin + 4 + int64(ii)
	return string(bs), nil
}

func (dec *Decoder) HasNextBlock() bool {
	if dec.readin >= dec.pos {
		return false
	} else {
		return true
	}
}

func (dec *Decoder) ReadNext() (*EncodedBlock, error) {
	if dec.HasNextBlock() {
		b, err := dec.NextBlock()
		if err != nil {
			return nil, err
		}
		dec.readinTotal = dec.readinTotal + b.OriginalSize
		dec.readoutTotal = dec.readoutTotal + b.Length()
		return b, nil
	} else {
		return nil, nil
	}
}

func (dec *Decoder) NextBlock() (*EncodedBlock, error) {
	b, err := ReadBool(dec.reader)
	if err != nil {
		return nil, err
	}
	dec.readin = dec.readin + 1
	if b {
		return dec.NextDupBlock()
	} else {
		return dec.NextNODupBlock()
	}
}

func (dec *Decoder) NextNODupBlock() (*EncodedBlock, error) {
	ii1, err := ReadInt64(dec.reader)
	if err != nil {
		return nil, err
	}
	ii2, err := ReadInt64(dec.reader)
	if err != nil {
		return nil, err
	}
	ii3, err := ReadInt64(dec.reader)
	if err != nil {
		return nil, err
	}
	bs1 := make([]byte, 32)
	err = ReadFull(dec.reader, bs1)
	if err != nil {
		return nil, err
	}
	bs2 := make([]byte, 32)
	err = ReadFull(dec.reader, bs2)
	if err != nil {
		return nil, err
	}
	bs3 := make([]byte, 32)
	err = ReadFull(dec.reader, bs3)
	if err != nil {
		return nil, err
	}
	bs4 := make([]byte, ii3)
	err = ReadFull(dec.reader, bs4)
	if err != nil {
		return nil, err
	}
	dec.readin = dec.readin + 8 + 8 + 8 + 32 + 32 + 32 + ii3
	b := &EncodedBlock{OriginalSize: ii1, RealSize: ii2, VHP: bs1, KEU: bs2, KED: bs3, DATA: bs4, IsDup: false}
	return b, nil
}

func (dec *Decoder) NextDupBlock() (*EncodedBlock, error) {
	ii1, err := ReadInt64(dec.reader)
	if err != nil {
		return nil, err
	}
	ii2, err := ReadInt64(dec.reader)
	if err != nil {
		return nil, err
	}
	b := &EncodedBlock{OriginalSize: ii1, RealSize: ii2, IsDup: true}
	bs1 := make([]byte, 32)
	err = ReadFull(dec.reader, bs1)
	if err != nil {
		return nil, err
	}
	bs2 := make([]byte, 32)
	err = ReadFull(dec.reader, bs2)
	if err != nil {
		return nil, err
	}
	bs3 := make([]byte, 16)
	err = ReadFull(dec.reader, bs3)
	if err != nil {
		return nil, err
	}
	b.VHP = bs1
	b.KEU = bs2
	b.VHB = bs3
	dec.readin = dec.readin + 8 + 8 + 32 + 32 + 16
	return b, nil
}

func (dec *Decoder) readHead() error {
	f, err := os.OpenFile(dec.path, os.O_RDONLY, 0644)
	if err != nil {
		return err
	}
	dec.file = f
	shastr := SimpleName(f.Name())
	dec.sha = base58.Decode(shastr)
	dec.reader = bufio.NewReader(f)
	pos, err := ReadInt64(dec.reader)
	if err != nil {
		return err
	}
	dec.pos = pos
	ii, err := ReadInt64(dec.reader)
	if err != nil {
		return err
	}
	dec.length = ii
	bs := make([]byte, 16)
	err = ReadFull(dec.reader, bs)
	if err != nil {
		return err
	}
	dec.md5 = bs
	i, err := ReadInt32(dec.reader)
	if err != nil {
		return err
	}
	dec.UserId = uint32(i)
	i, err = ReadInt32(dec.reader)
	if err != nil {
		return err
	}
	dec.KeyNumber = uint32(i)
	i, err = ReadInt32(dec.reader)
	if err != nil {
		return err
	}
	dec.StoreNumber = uint32(i)
	i, err = ReadInt32(dec.reader)
	if err != nil {
		return err
	}
	bss := make([]byte, i)
	err = ReadFull(dec.reader, bss)
	if err != nil {
		return err
	}
	dec.Sign = string(bss)
	dec.readin = 8 + 8 + 16 + 4 + 4 + 4 + 4 + int64(len(bss))
	return nil
}
