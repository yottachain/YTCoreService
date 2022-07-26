package backend

import (
	"fmt"
	"net"
	"net/http"

	"github.com/sirupsen/logrus"
	"github.com/yottachain/YTCoreService/api"
	"github.com/yottachain/YTCoreService/env"
	"github.com/yottachain/YTCoreService/s3"
)

var defauleFs *YTFS

var (
	Object_Timeout int = 60
	SyncFileMin    int
	MaxGetObjNum   int
	MaxListNum     int
)

var ListBucketNum *int32 = new(int32)
var GetObjectNum *int32 = new(int32)
var Object_UP_CH chan int

type YTFS struct{}

func NewYTFS() *YTFS {
	if defauleFs != nil {
		return defauleFs
	}
	fs := &YTFS{}
	api.StartApi()
	InitObjectUpPool()
	return fs
}

func InitObjectUpPool() {
	MaxCreateObjNum := env.GetConfig().GetRangeInt("MaxCreateObjNum", 20, 500, 50)
	Object_Timeout = env.GetConfig().GetRangeInt("ObjectTimeout", 10, 300, 60)
	SyncFileMin = env.GetConfig().GetRangeInt("SyncFileMin", 1, 10, 2) * 1024 * 1024
	Object_UP_CH = make(chan int, MaxCreateObjNum)
	for ii := 0; ii < MaxCreateObjNum; ii++ {
		Object_UP_CH <- 1
	}
	MaxGetObjNum = env.GetConfig().GetRangeInt("MaxGetObjNum", 20, 100, 50)
	MaxListNum = env.GetConfig().GetRangeInt("MaxListNum", 1, 10, 2)
}

var httpserver *http.Server

func StartS3() {
	fs := NewYTFS()
	addr := fmt.Sprintf(":%d", env.S3Port)
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		logrus.Panicf("[S3]Listen %s ERR:%s", addr, err)
	}
	httpserver = &http.Server{Addr: addr, Handler: s3.NewS3(fs).Server()}
	go func() {
		if env.CertFilePath != "" {
			err := httpserver.ServeTLS(listener, env.CertFilePath, env.KeyFilePath)
			if err == nil {
				logrus.Infof("[S3]Start S3 server https port :%d\n", listener.Addr().(*net.TCPAddr).Port)
			} else {
				listener.Close()
				logrus.Infof("[S3]Start S3 server ERR:%s\n", err)
			}
		} else {
			err := httpserver.Serve(listener)
			if err == nil {
				logrus.Infof("[S3]Start S3 server http port :%d\n", listener.Addr().(*net.TCPAddr).Port)
			} else {
				listener.Close()
				logrus.Infof("[S3]Start S3 server ERR:%s\n", err)
			}
		}
	}()
}

func StopS3() {
	if httpserver != nil {
		httpserver.Close()
	}
}
