package sgx

import (
	"bytes"
	"compress/flate"
	"compress/zlib"
	"io"
)

type BlockReader struct {
	block  []byte
	head   int
	reader io.Reader
}

func NewBlockReader(data []byte) *BlockReader {
	var ret int16 = 0
	ret <<= 8
	ret |= int16(data[0] & 0xFF)
	ret <<= 8
	ret |= int16(data[1] & 0xFF)
	r := new(BlockReader)
	r.block = data
	r.head = int(ret)
	var err error
	if r.head == 0 {
		r.reader, err = zlib.NewReader(bytes.NewReader(data[2:]))
		if err != nil {
			r.reader = flate.NewReader(bytes.NewReader(data[2:]))
		}
	} else if r.head < 0 {
		r.reader = bytes.NewReader(data[2:])
	} else {
		r.reader, err = zlib.NewReader(bytes.NewReader(data[2 : len(data)-r.head]))
		if err != nil {
			r.reader = flate.NewReader(bytes.NewReader(data[2 : len(data)-r.head]))
		}
	}
	return r
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
