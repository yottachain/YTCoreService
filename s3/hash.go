package s3

import (
	"bytes"
	"crypto/md5"
	"encoding/base64"
	"fmt"
	"hash"
	"io"
)

type hashingReader struct {
	inner    io.Reader
	expected []byte
	hash     hash.Hash
	sum      []byte
}

func newHashingReader(inner io.Reader, expectedMD5Base64 string) (*hashingReader, error) {
	var md5Bytes []byte
	var err error
	if expectedMD5Base64 != "" {
		md5Bytes, err = base64.StdEncoding.DecodeString(expectedMD5Base64)
		if err != nil {
			return nil, ErrInvalidDigest
		}
		if len(md5Bytes) != 16 {
			return nil, ErrInvalidDigest
		}
	}
	return &hashingReader{
		inner:    inner,
		expected: md5Bytes,
		hash:     md5.New(),
	}, nil
}

func (h *hashingReader) Sum(into []byte) []byte {
	if h.sum != nil {
		return h.sum
	}
	return h.hash.Sum(into)
}

func (h *hashingReader) Read(p []byte) (n int, err error) {
	n, err = h.inner.Read(p)
	if n != 0 {
		wn, _ := h.hash.Write(p[:n])
		if wn != n {
			return n, fmt.Errorf("short write to hasher")
		}
	}
	if err != nil {
		if err == io.EOF {
			h.sum = h.hash.Sum(nil)
			if h.expected != nil && !bytes.Equal(h.sum, h.expected) {
				return n, ErrBadDigest
			}
		}
		return n, err
	}
	return n, nil
}
