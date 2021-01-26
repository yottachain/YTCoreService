package codec

import (
	"crypto/md5"
	"crypto/sha256"
	"hash"
	"io"
)

func Sum(br io.ReadCloser) (int64, []byte, []byte, error) {
	defer br.Close()
	sha256Digest := sha256.New()
	md5Digest := md5.New()
	ha := []hash.Hash{sha256Digest, md5Digest}
	num, err := CalHashs(ha, br)
	if err != nil {
		return 0, nil, nil, err
	}
	return num, sha256Digest.Sum(nil), md5Digest.Sum(nil), nil
}

func CalHash(ha hash.Hash, br io.ReadCloser) (int64, error) {
	return CalHashs([]hash.Hash{ha}, br)
}

func CalHashs(ha []hash.Hash, br io.ReadCloser) (int64, error) {
	defer br.Close()
	readbuf := make([]byte, 8192)
	var size int64 = 0
	for {
		num, err := br.Read(readbuf)
		if err != nil && err != io.EOF {
			return 0, err
		}
		if num > 0 {
			bs := readbuf[0:num]
			for _, h := range ha {
				h.Write(bs)
			}
			size = size + int64(num)
		}
		if err != nil && err == io.EOF {
			break
		}
	}
	return size, nil
}
