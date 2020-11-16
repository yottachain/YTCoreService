package api

import (
	"bufio"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/yottachain/YTCoreService/env"
)

func Append(srcpath, root, path string) error {
	key := srcpath
	if root != "" {
		key = strings.TrimPrefix(srcpath, root)
	}
	exist, err := checkExist(key, path)
	if err != nil {
		return err
	}
	if !exist {
		append(key, path)
	}
	return nil
}

func append(key, path string) error {
	f, err := os.OpenFile(path, os.O_APPEND, 0644)
	if err != nil {
		return err
	}
	defer f.Close()
	WriteKey(key, f)
	return nil
}

func checkExist(key, path string) (bool, error) {
	f, err := os.OpenFile(path, os.O_RDONLY, 0644)
	if err != nil {
		return false, err
	}
	defer f.Close()
	pos, err := ReadInt64(f)
	if err != nil {
		return false, err
	}
	_, err = f.Seek(pos, io.SeekStart)
	if err != nil {
		return false, err
	}
	reader := bufio.NewReader(f)
	for {
		size, err := ReadInt32(reader)
		if err != nil {
			if err == io.EOF {
				break
			}
			return false, err
		}
		bs := make([]byte, size)
		err = ReadFull(reader, bs)
		if err != nil {
			return false, err
		}
		s := string(bs)
		if s == key {
			return true, nil
		}
	}
	return false, nil
}

func ReadInt64(f io.Reader) (int64, error) {
	bs := make([]byte, 8)
	err := ReadFull(f, bs)
	if err != nil {
		return 0, err
	}
	i := env.BytesToId(bs)
	return i, nil
}

func ReadInt32(f io.Reader) (int32, error) {
	bs := make([]byte, 4)
	err := ReadFull(f, bs)
	if err != nil {
		return 0, err
	}
	i := env.BytesToInt32(bs)
	return i, nil
}

func ReadBool(f io.Reader) (bool, error) {
	bs := make([]byte, 1)
	err := ReadFull(f, bs)
	if err != nil {
		return false, err
	}
	if bs[0] == 0x00 {
		return false, nil
	} else {
		return true, nil
	}
}

func ReadFull(r io.Reader, bs []byte) error {
	size := len(bs)
	pos := 0
	for {
		n, err := r.Read(bs[pos:])
		pos = pos + n
		if pos == size {
			break
		}
		if err != nil {
			return err
		}
	}
	return nil
}

func SimpleName(name string) string {
	return filepath.Base(name)
}
