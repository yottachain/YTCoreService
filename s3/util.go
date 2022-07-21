package s3

import (
	"io"
	"io/ioutil"
	"net/http"
	"strconv"
	"strings"
)

func GetAccessKey(r *http.Request) (string, error) {
	Authorization := r.Header.Get("Authorization")
	if Authorization == "" {
		return "", ErrAccessDenied
	}
	publicKey := GetBetweenStr(Authorization, "YTA", "/")
	content := publicKey[3:]
	if len(content) > 50 {
		publicKeyLength := strings.Index(content, ":")
		contentNew := content[:publicKeyLength]
		content = contentNew
	}
	return content, nil
}

func GetBetweenStr(str, start, end string) string {
	n := strings.Index(str, start)
	if n == -1 {
		n = 0
	}
	str = string([]byte(str)[n:])
	m := strings.Index(str, end)
	if m == -1 {
		m = len(str)
	}
	str = string([]byte(str)[:m])
	return str
}

func parseClampedInt(in string, defaultValue, min, max int64) (int64, error) {
	var v int64
	if in == "" {
		v = defaultValue
	} else {
		var err error
		v, err = strconv.ParseInt(in, 10, 0)
		if err != nil {
			return defaultValue, ErrInvalidArgument
		}
	}
	if v < min {
		v = min
	} else if v > max {
		v = max
	}
	return v, nil
}

func ReadAll(r io.Reader, size int64) (b []byte, err error) {
	var n int
	b = make([]byte, size)
	n, err = io.ReadFull(r, b)
	if err == io.ErrUnexpectedEOF {
		return nil, ErrIncompleteBody
	} else if err != nil {
		return nil, err
	}

	if n != int(size) {
		return nil, ErrIncompleteBody
	}

	if extra, err := ioutil.ReadAll(r); err != nil {
		return nil, err
	} else if len(extra) > 0 {
		return nil, ErrIncompleteBody
	}
	return b, nil
}
