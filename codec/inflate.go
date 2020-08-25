package codec

import (
	"bytes"
	"compress/flate"
	"crypto/sha256"
	"hash"
	"io"
	"os"

	"github.com/yottachain/YTCoreService/env"
)

type BlockReader struct {
	saver  io.WriteCloser
	block  *PlainBlock
	head   int
	reader io.Reader
}

func NewBlockReader(b *PlainBlock) *BlockReader {
	var ret int16 = 0
	ret <<= 8
	ret |= int16(b.Data[0] & 0xFF)
	ret <<= 8
	ret |= int16(b.Data[1] & 0xFF)
	r := new(BlockReader)
	r.block = b
	r.head = int(ret)
	if r.head == 0 {
		r.reader = flate.NewReader(bytes.NewReader(b.Data[2:]))
	} else if r.head < 0 {
		r.reader = bytes.NewReader(b.Data[2:])
	} else {
		r.reader = flate.NewReader(bytes.NewReader(b.Data[2 : len(b.Data)-r.head]))
	}
	return r
}

func (br *BlockReader) SetPath(p string) {
	if p != "" {
		f, err := os.OpenFile(p+"block.src", os.O_WRONLY|os.O_TRUNC|os.O_CREATE, 0644)
		if err == nil {
			br.saver = f
		}
	}
}

func (br *BlockReader) Skip(n int64) error {
	if n <= 0 {
		return nil
	}
	remain := n
	for {
		out := make([]byte, remain)
		reasn, err := br.Read(out)
		if err != nil && err != io.EOF {
			return err
		}
		remain = remain - int64(reasn)
		if remain == 0 {
			return nil
		}
	}
}

func (br *BlockReader) Read(p []byte) (n int, err error) {
	var num int = 0
	num, err = br.reader.Read(p)
	if err == io.EOF {
		if num > 0 {
			if br.saver != nil {
				br.saver.Write(p[0:num])
				br.saver.Close()
			}
			return num, nil
		}
		if br.head > 0 {
			size := len(br.block.Data)
			br.reader = bytes.NewReader(br.block.Data[size-br.head : size])
			br.head = 0
			return br.Read(p)
		} else {
			if br.saver != nil {
				br.saver.Close()
			}
		}
	}
	if br.saver != nil {
		br.saver.Write(p[0:num])
	}
	return num, err
}

type FileDecoder struct {
	blocks []interface{}
	path   string
	length int64
	vhw    []byte
}

func NewFileDecoder(p string) *FileDecoder {
	f := new(FileDecoder)
	f.blocks = []interface{}{}
	f.path = p
	return f
}

func (decoder *FileDecoder) AddPlainBlock(b *PlainBlock) {
	decoder.blocks = append(decoder.blocks, b)
}

func (decoder *FileDecoder) AddEncryptedBlock(b *EncryptedBlock) {
	decoder.blocks = append(decoder.blocks, b)
}

func (decoder *FileDecoder) Handle() error {
	f, err := os.Create(decoder.path)
	if err != nil {
		return err
	}
	defer f.Close()
	sha256Digest := sha256.New()
	var size int64 = 0
	for _, item := range decoder.blocks {
		var value *PlainBlock
		if r, ok := item.(*PlainBlock); ok {
			value = r
			value.Load()
		} else {
			r := item.(*EncryptedBlock)
			r.Load()
			baes := NewBlockAESDecryptor(r)
			v, err := baes.Decrypt()
			if err != nil {
				return err
			}
			value = v
		}
		br := NewBlockReader(value)
		num, err := decoder.readBlock(sha256Digest, f, br)
		if err == io.EOF {
			continue
		}
		size = size + int64(num)
	}
	decoder.vhw = sha256Digest.Sum(nil)
	decoder.length = size
	return nil
}

func (decoder *FileDecoder) readBlock(sha256 hash.Hash, file *os.File, br *BlockReader) (int64, error) {
	var size int64 = 0
	buf := make([]byte, env.READFILE_BUF_SIZE)
	for {
		num, err := br.Read(buf)
		if err != nil {
			return size, err
		}
		_, err = sha256.Write(buf[0:num])
		_, err = file.Write(buf[0:num])
		size = size + int64(num)
		if err != nil {
			return size, err
		}
	}
}

func (decoder *FileDecoder) GetLength() int64 {
	return decoder.length
}

func (decoder *FileDecoder) GetVHW() []byte {
	return decoder.vhw
}
