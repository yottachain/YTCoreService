package cache

import (
	"io/ioutil"
	"os"
	"path"
	"strings"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/yottachain/YTCoreService/env"
)

var IS_S3_Server = true

func Delete(paths []string) {
	if paths != nil && IS_S3_Server {
		dir := ""
		for _, p := range paths {
			p = strings.ReplaceAll(p, "\\", "/")
			dir = path.Dir(p)
			if strings.HasPrefix(dir, env.GetS3Cache()) {
				os.Remove(p)
			}
		}
		dir = path.Clean(dir)
		if !strings.HasSuffix(dir, "/") {
			dir = dir + "/"
		}
		if dir != env.GetS3Cache() {
			if strings.HasPrefix(dir, env.GetS3Cache()) {
				os.Remove(dir)
			}
		}
	}
}

const FileExpiredTime = 60 * 60 * 24

var LastCleanTime int64 = 0

func Clear() {
	if time.Now().Unix()-LastCleanTime < FileExpiredTime {
		return
	}
	logrus.Infof("[Cache]Clearing expired files...\n")
	rd, err := ioutil.ReadDir(env.GetS3Cache())
	if err != nil {
		return
	}
	for _, fi := range rd {
		if fi.IsDir() {
			deleteDir(env.GetS3Cache(), fi)
		} else {
			deleteFile(env.GetS3Cache(), fi)
		}
	}
	LastCleanTime = time.Now().Unix()
}

func deleteFile(parent string, f os.FileInfo) bool {
	if f.ModTime().Unix()+FileExpiredTime < time.Now().Unix() {
		err := os.Remove(parent + f.Name())
		if err != nil {
			return false
		} else {
			logrus.Infof("[Cache]Delete bad file %s\n", parent+f.Name())
			return true
		}
	}
	return false
}

func deleteDir(parent string, dir os.FileInfo) bool {
	curpath := parent + dir.Name()
	rd, err := ioutil.ReadDir(curpath)
	if err != nil {
		return false
	}
	deleted := true
	for _, fi := range rd {
		if fi.IsDir() {
			b := deleteDir(curpath+"/", fi)
			if !b {
				deleted = false
			}
		} else {
			b := deleteFile(curpath+"/", fi)
			if !b {
				deleted = false
			}
		}
	}
	if deleted {
		os.Remove(curpath)
		if err != nil {
			return false
		} else {
			return true
		}
	} else {
		return false
	}
}
