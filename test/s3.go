package test

import (
	"fmt"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/yottachain/YTCoreService/api"
	"github.com/yottachain/YTCoreService/pkt"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

func S3() {
	initApi()
	//test.DownLoadFile()
	//test.ListBucket()
	//test.ListObj()
}

func DownLoadFile() {
	outpath := "D:/YTSDK.ok.rar"
	dn, errmsg := client.NewDownloadFile("test", "YTSDK.rar", primitive.NilObjectID)
	if errmsg != nil {
		logrus.Panicf("[DownLoadFile]ERR:%s\n", pkt.ToError(errmsg))
	}
	oksign := make(chan int)
	go func() {
		for {
			timeout := time.After(time.Second * 5)
			select {
			case oksign <- 1:
				return
			case <-timeout:
				logrus.Infof("[DownloadFile]Progress:%d\n", dn.GetProgress())
			}
		}
	}()
	_, err := dn.SaveToFile(outpath)
	if err != nil {
		logrus.Errorf("[DownloadFile]ERR:%s.\n", err)
	} else {
		logrus.Infof("[DownloadFile]Progress:%d\n", dn.GetProgress())
		logrus.Infof("[DownloadFile]OK.\n")
	}
	<-oksign
}

func ListBucket() {
	initApi()
	buck := client.NewBucketAccessor()
	ss, err := buck.ListBucket()
	if err != nil {
		logrus.Panicf("[ListBucket]ERR:%s\n", pkt.ToError(err))
	}
	for _, s := range ss {
		logrus.Infof("[ListBucket]:%s\n", s)
	}

	//delete
	err = buck.DeleteBucket("1234")
	if err != nil {
		logrus.Panicf("[ListBucket]ERR:%s\n", pkt.ToError(err))
	}
	obj := client.NewObjectAccessor()
	obj.ListObject("1234", "", "", false, primitive.NilObjectID, 1000)
	ss, err = buck.ListBucket()
	if err != nil {
		logrus.Panicf("[ListBucket]ERR:%s\n", pkt.ToError(err))
	}
	for _, s := range ss {
		logrus.Infof("[ListBucket]:%s\n", s)
	}

	//create
	header := make(map[string]string)
	header["version_status"] = "Enabled"
	meta, err1 := api.BucketMetaMapToBytes(header)
	if err1 != nil {
		logrus.Panicf("[ListBucket]ERR:%s\n", err1)
	}
	err = buck.CreateBucket("1234", meta)
	if err != nil {
		logrus.Panicf("[ListBucket]ERR:%s\n", pkt.ToError(err))
	}
	ss, err = buck.ListBucket()
	if err != nil {
		logrus.Panicf("[ListBucket]ERR:%s\n", pkt.ToError(err))
	}
	for _, s := range ss {
		logrus.Infof("[ListBucket]:%s\n", s)
	}
}

func ListObj() {
	initApi()
	obj := client.NewObjectAccessor()
	items, _ := obj.ListObject("test", "", "", false, primitive.NilObjectID, 1000)
	item := items[100]
	m, _ := api.BytesToFileMetaMap(item.Meta, primitive.NilObjectID)
	fmt.Println(api.LengthKey + ":" + m[api.LengthKey])
	info, _ := client.NewObjectMeta("test", item.FileName, primitive.NilObjectID)
	fmt.Printf("%s:%d\n", api.LengthKey, info.Length)
}
