package codec

import (
	"bytes"
	"compress/zlib"
	"crypto/sha256"
	"errors"
	"io"
	"os"

	"github.com/yottachain/YTCoreService/env"
)

type FileEncoder struct {
	finished     bool
	length       int64
	readinTotal  int64
	readoutTotal int64
	vhw          []byte
	reader       io.ReadSeeker
	curBlock     *PlainBlock
}

func NewBytesEncoder(bs []byte) (*FileEncoder, error) {
	if len(bs) > env.Max_Memory_Usage {
		return nil, errors.New("Length over 10M")
	}
	size := int64(len(bs))
	if size <= 0 {
		return nil, errors.New("Zero length file")
	}
	sha256Digest := sha256.New()
	sha256Digest.Write(bs)
	r := new(FileEncoder)
	r.length = size
	r.reader = bytes.NewReader(bs)
	r.vhw = sha256Digest.Sum(nil)
	return r, nil
}

func NewFileEncoder(path string) (*FileEncoder, error) {
	var size int64
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	sha256Digest := sha256.New()
	size, err = io.Copy(sha256Digest, f)
	if err != nil {
		return nil, err
	}
	newf, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	r := new(FileEncoder)
	r.length = int64(size)
	r.reader = NewBufferReader(*newf, env.READFILE_BUF_SIZE)
	r.vhw = sha256Digest.Sum(nil)
	return r, nil
}

func (fileEncoder *FileEncoder) Close() {
	if r, ok := fileEncoder.reader.(*BufferReader); ok {
		r.Close()
	}
}

func (fileEncoder *FileEncoder) ReadNext() (*PlainBlock, error) {
	has, err := fileEncoder.HasNext()
	if err != nil {
		return nil, err
	}
	if has {
		fileEncoder.readoutTotal = fileEncoder.readoutTotal + fileEncoder.curBlock.Length()
		return fileEncoder.curBlock, nil
	} else {
		return nil, nil
	}
}

func (fileEncoder *FileEncoder) Next() *PlainBlock {
	if fileEncoder.curBlock != nil {
		fileEncoder.readoutTotal = fileEncoder.readoutTotal + fileEncoder.curBlock.Length()
	}
	return fileEncoder.curBlock
}

func (fileEncoder *FileEncoder) HasNext() (bool, error) {
	if fileEncoder.finished {
		fileEncoder.Close()
		return false, nil
	}
	readTotal, err := fileEncoder.deflate()
	if err != nil {
		fileEncoder.Close()
		return false, err
	}
	if readTotal > 0 {
		_, err1 := fileEncoder.reader.Seek(readTotal*-1, io.SeekCurrent)
		if err1 != nil {
			fileEncoder.Close()
			return false, err1
		}
		err2 := fileEncoder.pack()
		if err2 != nil {
			fileEncoder.Close()
			return false, err2
		}
	}
	if fileEncoder.finished {
		fileEncoder.Close()
	}
	if fileEncoder.curBlock == nil {
		return false, nil
	} else {
		return true, nil
	}
}

func (fileEncoder *FileEncoder) pack() error {
	buf := bytes.NewBuffer(nil)
	size := -1
	buf.Write([]byte{uint8(size >> 8), uint8(size)})
	data := make([]byte, env.Default_Block_Size-2)
	num, err := fileEncoder.reader.Read(data)
	if err != nil && err != io.EOF {
		return err
	}
	if num > 0 {
		buf.Write(data[0:num])
		fileEncoder.curBlock = NewPlainBlock(buf.Bytes(), int64(num))
		if err == io.EOF || num < env.Default_Block_Size-2 {
			fileEncoder.finished = true
		}
		fileEncoder.readinTotal = fileEncoder.readinTotal + int64(num)
	} else {
		fileEncoder.curBlock = nil
		fileEncoder.finished = true
	}
	return nil
}

func (fileEncoder *FileEncoder) deflate() (int64, error) {
	buf := bytes.NewBuffer(nil)
	buf.Write([]byte{0, 0})
	flateWrite := zlib.NewWriter(buf)
	var err error
	bs := make([]byte, 16)
	var totalIn int64 = 0
	var num int
	for {
		num, err = fileEncoder.reader.Read(bs)
		if err != nil && err != io.EOF {
			return 0, err
		}
		if err == io.EOF && num <= 0 {
			break
		}
		totalIn = totalIn + int64(num)
		flateWrite.Write(bs[0:num])
		remainSize := env.Default_Block_Size - buf.Len()
		if remainSize < 0 {
			return totalIn, nil
		}
		if remainSize < env.Compress_Reserve_Size {
			flateWrite.Close()
			if totalIn-int64(buf.Len()) <= 0 {
				return totalIn, nil
			}
			remainSize = env.Default_Block_Size - buf.Len()
			if remainSize < 0 {
				return totalIn, nil
			} else {
				remainbs := make([]byte, remainSize)
				num, err = fileEncoder.reader.Read(remainbs)
				if err != nil && err != io.EOF {
					return 0, err
				}
				if num > 0 {
					totalIn = totalIn + int64(num)
					buf.Write(remainbs[0:num])
					data := buf.Bytes()
					data[0] = uint8(num >> 8)
					data[1] = uint8(num)
					fileEncoder.curBlock = NewPlainBlock(data, totalIn)
				} else {
					fileEncoder.curBlock = NewPlainBlock(buf.Bytes(), totalIn)
				}
				if err == io.EOF {
					fileEncoder.finished = true
				}
				fileEncoder.readinTotal = fileEncoder.readinTotal + totalIn
				return 0, nil
			}

		}
	}
	flateWrite.Close()
	if totalIn-int64(buf.Len()) <= 0 {
		return totalIn, nil
	}
	if buf.Len() > env.Default_Block_Size {
		return totalIn, nil
	}
	if totalIn > 0 {
		fileEncoder.curBlock = NewPlainBlock(buf.Bytes(), totalIn)
	}
	fileEncoder.finished = true
	fileEncoder.readinTotal = fileEncoder.readinTotal + totalIn
	return 0, nil
}

func (fileEncoder *FileEncoder) GetLength() int64 {
	return fileEncoder.length
}

func (fileEncoder *FileEncoder) GetVHW() []byte {
	return fileEncoder.vhw
}

func (fileEncoder *FileEncoder) IsFinished() bool {
	return fileEncoder.finished
}

func (fileEncoder *FileEncoder) GetReadinTotal() int64 {
	return fileEncoder.readinTotal
}

func (fileEncoder *FileEncoder) GetReadoutTotal() int64 {
	return fileEncoder.readoutTotal
}

type BufferReader struct {
	r     os.File
	buf   []byte
	count int
	pos   int
}

func NewBufferReader(f os.File, size int) *BufferReader {
	br := new(BufferReader)
	br.r = f
	br.count = 0
	br.pos = 0
	if size > 1024*1024*2 {
		size = 1024 * 1024 * 2
	}
	if size < 1024*4 {
		size = 1024 * 4
	}
	br.buf = make([]byte, size)
	return br
}

func (br *BufferReader) getBufIfOpen() ([]byte, error) {
	if br.buf == nil {
		return nil, errors.New("Stream closed")
	}
	return br.buf, nil
}

func (br *BufferReader) fill() error {
	buffer, err := br.getBufIfOpen()
	if err != nil {
		return err
	}
	num, err1 := br.r.Read(buffer)
	if err1 != nil && err1 != io.EOF {
		return err
	}
	if num > 0 {
		br.count = num
		br.pos = 0
		return nil
	}
	return err1
}

func (br *BufferReader) Read(p []byte) (n int, err error) {
	len := len(p)
	position := 0
	for {
		num, err := br.readNotAll(p[position:len])
		if err != nil && err != io.EOF {
			return 0, err
		}
		position = position + num
		if position >= len {
			return position, err
		} else {
			if err == io.EOF {
				return position, err
			}
		}
	}
}

func (br *BufferReader) readNotAll(p []byte) (n int, err error) {
	if br.count == br.pos {
		err := br.fill()
		if err != nil {
			return 0, err
		}
	}
	len1 := br.count - br.pos
	len2 := len(p)
	if len2 <= len1 {
		copy(p, br.buf[br.pos:br.pos+len2])
		br.pos = br.pos + len2
		return len2, nil
	} else {
		copy(p, br.buf[br.pos:br.count])
		br.pos = br.pos + len1
		return len1, nil
	}
}

func (br *BufferReader) Seek(offset int64, whence int) (int64, error) {
	switch whence {
	case io.SeekStart:
		return 0, errors.New("not Supported")
	case io.SeekCurrent:
		if offset >= 0 {
			remainsize := int64(br.count - br.pos)
			if offset <= remainsize {
				br.pos = br.pos + int(offset)
				return offset, nil
			} else {
				br.pos = 0
				br.count = 0
				return br.r.Seek(offset-remainsize, whence)
			}
		} else {
			newoff := offset * -1
			if newoff <= int64(br.pos) {
				br.pos = br.pos - int(newoff)
				return offset, nil
			} else {
				newoff = offset - int64(br.count) + int64(br.pos)
				br.pos = 0
				br.count = 0
				return br.r.Seek(newoff, whence)
			}
		}
	case io.SeekEnd:
		return 0, errors.New("not Supported")
	default:
		return 0, errors.New("bytes.Reader.Seek: invalid whence")
	}
}

func (br *BufferReader) Close() {
	br.buf = nil
	br.r.Close()
}
