package codec

import (
	"crypto/md5"
	"crypto/sha256"
	"errors"
	"io"
	"os"
)

type Part struct {
	Path   string
	r      *os.File
	length int64
}

func NewPart(path string) (*Part, error) {
	s, err := os.Stat(path)
	if err != nil {
		return nil, err
	} else {
		if s.IsDir() {
			return nil, errors.New("The specified path is a directory.")
		}
	}
	p := &Part{Path: path}
	return p, nil
}

func (br *Part) close() {
	if br.r != nil {
		br.r.Close()
		br.r = nil
	}
}

func (br *Part) fill(rd *MergeReader) error {
	if br.r == nil {
		f, err := os.Open(br.Path)
		if err != nil {
			return err
		}
		br.r = f
	}
	num, err1 := br.r.Read(rd.buf)
	if err1 != nil && err1 != io.EOF {
		return err1
	}
	if num > 0 {
		br.length = br.length + int64(num)
		rd.count = num
		rd.pos = 0
		return nil
	}
	return err1
}

func (br *Part) back(offset int64) error {
	if br.r == nil {
		f, err := os.Open(br.Path)
		if err != nil {
			return err
		}
		br.r = f
		br.length = br.length + offset
		_, err = br.r.Seek(br.length, io.SeekStart)
		if err != nil {
			return err
		}
	} else {
		_, err := br.r.Seek(offset, io.SeekCurrent)
		if err != nil {
			return err
		}
		br.length = br.length + offset
	}
	return nil
}

func (br *Part) forward(offset int64) error {
	if br.r == nil {
		f, err := os.Open(br.Path)
		if err != nil {
			return err
		}
		br.r = f
	}
	if offset > 0 {
		_, err := br.r.Seek(offset, io.SeekStart)
		if err != nil {
			return err
		}
	}
	br.length = offset
	return nil
}

type MergeReader struct {
	Parts        []*Part
	curPart      *Part
	curpartIndex int
	buf          []byte
	count        int
	pos          int
}

func NewMergeReader(ps []string, bufsize int) (*MergeReader, error) {
	size := len(ps)
	if size < 1 {
		return nil, errors.New("Nil path.")
	}
	parts := make([]*Part, size)
	var err error
	for index, p := range ps {
		parts[index], err = NewPart(p)
		if err != nil {
			return nil, err
		}
	}
	m := &MergeReader{Parts: parts}
	m.curpartIndex = -1
	m.count = 0
	m.pos = 0
	if bufsize > 1024*1024*2 {
		bufsize = 1024 * 1024 * 2
	}
	if bufsize < 1024*4 {
		bufsize = 1024 * 4
	}
	m.buf = make([]byte, bufsize)
	return m, nil
}

func (br *MergeReader) Sum() (int64, []byte, []byte, error) {
	defer br.Close()
	sha256Digest := sha256.New()
	md5Digest := md5.New()
	readbuf := make([]byte, 8192)
	var size int64 = 0
	for {
		num, err := br.Read(readbuf)
		if err != nil && err != io.EOF {
			return 0, nil, nil, err
		}
		if num > 0 {
			bs := readbuf[0:num]
			sha256Digest.Write(bs)
			md5Digest.Write(bs)
			size = size + int64(num)
		}
		if err != nil && err == io.EOF {
			break
		}
	}
	return size, sha256Digest.Sum(nil), md5Digest.Sum(nil), nil
}

func (br *MergeReader) readNotAll(p []byte) (n int, err error) {
	if br.count == br.pos {
		for {
			if br.curPart == nil {
				if br.curpartIndex == len(br.Parts)-1 {
					return 0, io.EOF
				}
				br.curpartIndex++
				br.curPart = br.Parts[br.curpartIndex]
				br.curPart.length = 0
			}
			err := br.curPart.fill(br)
			if err != nil {
				if err == io.EOF {
					br.curPart.close()
					br.curPart = nil
					if br.curpartIndex == len(br.Parts)-1 {
						return 0, err
					}
					continue
				}
				return 0, err
			} else {
				break
			}
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

func (br *MergeReader) Seek(offset int64, whence int) (int64, error) {
	switch whence {
	case io.SeekStart:
		return 0, errors.New("not Supported")
	case io.SeekCurrent:
		if offset >= 0 {
			return 0, errors.New("not Supported")
		} else {
			newoff := -offset
			if newoff <= int64(br.pos) {
				br.pos = br.pos - int(newoff)
				return offset, nil
			} else {
				newoff = offset - int64(br.count) + int64(br.pos)
				if br.curPart == nil {
					br.curPart = br.Parts[br.curpartIndex]
				}
				if br.curPart.length+newoff >= 0 {
					err := br.curPart.back(newoff)
					if err != nil {
						return 0, err
					}
				} else {
					for {
						newoff = br.curPart.length + newoff
						if newoff < 0 {
							br.curPart.length = 0
							br.curPart.close()
							br.curpartIndex--
							br.curPart = br.Parts[br.curpartIndex]
						} else {
							err := br.curPart.forward(newoff)
							if err != nil {
								return 0, err
							}
							break
						}
					}
				}
				br.pos = 0
				br.count = 0
			}
		}
	case io.SeekEnd:
		return 0, errors.New("not Supported")
	default:
		return 0, errors.New("bytes.Reader.Seek: invalid whence")
	}
	return 0, nil
}

func (br *MergeReader) Read(p []byte) (n int, err error) {
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

func (br *MergeReader) Close() {
	for _, p := range br.Parts {
		p.close()
		p.length = 0
	}
	br.curPart = nil
	br.curpartIndex = -1
	br.count = 0
	br.pos = 0
}
