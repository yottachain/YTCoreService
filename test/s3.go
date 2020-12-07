package test

import (
	"fmt"
	"os"

	"github.com/sirupsen/logrus"
	"github.com/yottachain/YTCoreService/api"
	"github.com/yottachain/YTCoreService/pkt"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

func SyncFile() {
	os.Setenv("YTFS.snlist", "conf/snlistZW.properties")
	api.StartApi()
}

func UploadFile() {
	initApi()
	client.UploadFile("D:/Secop.rar", "test", "Secop2.rar")
	//client.UploadFile("D:/Secop.rar", "test", "Secop.rar")
	//client.UploadFile("D:/YTCoreService_2.0.0.1.gz", "test", "YTCoreService_2.0.0.1.gz")
	//client.UploadFile("D:/YTCoreService_2.0.0.2.gz", "test", "YTCoreService_2.0.0.2.gz")
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
