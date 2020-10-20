package codec

import (
	"errors"
	"os"
)

type Part struct {
	Path     string
	startpos int64
	length   int64
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

type MergeReader struct {
	Parts []*Part
	r     os.File
	buf   []byte
	count int
	pos   int
}

func NewMergeReader(ps []string) (*MergeReader, error) {
	size := len(ps)
	parts := make([]*Part, size)
	var err error
	for index, p := range ps {
		parts[index], err = NewPart(p)
		if err != nil {
			return nil, err
		}
	}
	m := &MergeReader{Parts: parts}
	return m, nil
}

func (br *MergeReader) Seek(offset int64, whence int) (int64, error) {
	return 0, nil
}

func (br *MergeReader) Read(p []byte) (n int, err error) {
	return 0, nil
}
