package backup

import "os"

func Append(srcpath, root, path string) error {
	f, err := os.OpenFile(path, os.O_RDWR, 0666)
	if err != nil {
		return err
	}
	defer f.Close()
	return nil
}
