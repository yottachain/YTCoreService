package codec

import (
	"crypto/md5"
	"crypto/sha256"
	"io"
)

func Sum(br io.ReadCloser) (int64, []byte, []byte, error) {
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
