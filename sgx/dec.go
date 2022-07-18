package sgx

import (
	"bytes"
	"compress/zlib"
	"errors"
	"io"
)

type BlockReader struct {
	block  []byte
	head   int
	reader io.Reader
}

func NewBlockReader(data []byte) (*BlockReader, error) {
	var ret int16 = 0
	ret <<= 8
	ret |= int16(data[0] & 0xFF)
	ret <<= 8
	ret |= int16(data[1] & 0xFF)
	r := new(BlockReader)
	r.block = data
	r.head = int(ret)
	var err error = nil
	if r.head == 0 {
		r.reader, err = zlib.NewReader(bytes.NewReader(data[2:]))
	} else if r.head < 0 {
		r.reader = bytes.NewReader(data[2:])
	} else {
		end := len(data) - r.head
		if end < 2 {
			err = errors.New("Decode err")
		} else {
			r.reader, err = zlib.NewReader(bytes.NewReader(data[2:end]))
		}
	}
	return r, err
}

func (br *BlockReader) Read(p []byte) (n int, err error) {
	var num int = 0
	num, err = br.reader.Read(p)
	if err == io.EOF {
		if num > 0 {
			return num, nil
		}
		if br.head > 0 {
			size := len(br.block)
			br.reader = bytes.NewReader(br.block[size-br.head : size])
			br.head = 0
			return br.Read(p)
		}
	}
	return num, err
}
