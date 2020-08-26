package test

import (
	"crypto/sha256"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/aurawing/eos-go/btcsuite/btcutil/base58"
	"github.com/sirupsen/logrus"
	"github.com/yottachain/YTCoreService/api"
	"github.com/yottachain/YTCoreService/env"
	"github.com/yottachain/YTCoreService/pkt"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

const yfnet = true
const testsize = 1024 * 1024 * 1
const spos = 1024*1024*5 + 798
const epos = 1024*1024*8 + 12
const filePath = "d:/virtue.3.4.0.zip"
const savePath = "d:/test"

var data []byte
var client *api.Client

func ListObj() {
	initApi()
	obj := client.NewObjectAccessor()
	items, _ := obj.ListObject("test", "", "", false, primitive.NilObjectID, 1000)
	item := items[100]
	m, _ := api.BytesToFileMetaMap(item.Meta, primitive.NilObjectID)
	fmt.Println(api.LengthKey + ":" + m[api.LengthKey])
}

func UpAndDownBytes() {
	initApi()
	vhw := upload()
	download(vhw)
	downloadRange(vhw)
}

func UpAndDownFile() {
	initApi()
	vhw := uploadFile()
	//download(vhw)
	saveFile(vhw)
}

func DownLoadByKey() {
	initApi()
	dn, errmsg := client.NewDownloadFile("newbucket", "log", primitive.NilObjectID)
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
	dn.SaveToPath(savePath)
	err := dn.SaveToPath(savePath)
	if err != nil {
		logrus.Error("[DownloadFile]ERR:%s.\n", err)
	} else {
		logrus.Infof("[DownloadFile]Progress:%d\n", dn.GetProgress())
		logrus.Infof("[DownloadFile]OK.\n")
	}
	<-oksign
}

func saveFile(vhw []byte) {
	dn, errmsg := client.NewDownloadObject(vhw)
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
	err := dn.SaveToPath(savePath)
	if err != nil {
		logrus.Error("[DownloadFile]ERR:%s.\n", err)
	} else {
		logrus.Infof("[DownloadFile]Progress:%d\n", dn.GetProgress())
		logrus.Infof("[DownloadFile]OK.\n")
	}
	<-oksign
}

func uploadFile() []byte {
	up := client.NewUploadObject()
	oksign := make(chan int)
	go func() {
		for {
			timeout := time.After(time.Second * 5)
			select {
			case oksign <- 1:
				return
			case <-timeout:
				logrus.Infof("[UploadFile]Progress:%d\n", up.GetProgress())
			}
		}
	}()
	vhw, errmsg := up.UploadFile(filePath)
	if errmsg != nil {
		logrus.Panicf("[UploadFile]ERR:%s\n", pkt.ToError(errmsg))
	}
	logrus.Infof("[UploadFile]Progress:%d\n", up.GetProgress())
	logrus.Infof("[UploadFile]OK:%s\n", base58.Encode(vhw))
	<-oksign
	return vhw
}

func initApi() {
	var user string
	var pkey string
	if yfnet {
		os.Setenv("YTFS.snlist", "conf/snlistYF.properties")
		user = "username1234"
		pkey = "5KfbRow4L71fZnnu9XEnkmVqByi6CSmRiADJCx6asRS4TUEkU79"
	} else {
		os.Setenv("YTFS.snlist", "conf/snlistZW.properties")
		//user = "ianmooneyy11"
		//pkey = "5JnLRW1bTRD2bxo93wZ1qnpXfMDHzA97qcQjabnoqgmJTt7kBoH"
		user = "pollyzhang11"
		pkey = "5JVwTWuJWcmXy22f12YzjjpKiiqQyJnqoSjx4Mk2JxtgQYAb3Fw"
	}
	api.StartApi()
	c, err := api.NewClient(user, pkey)
	if err != nil {
		logrus.Panicf("[NewClient]ERR:%s\n", err)
	}
	client = c
	data = env.MakeRandData(testsize)
}

func upload() []byte {
	up := client.NewUploadObject()
	vhw, errmsg := up.UploadBytes(data)
	if errmsg != nil {
		logrus.Panicf("[UploadFile]ERR:%s\n", pkt.ToError(errmsg))
	}
	logrus.Infof("[UploadFile]OK:%s\n", base58.Encode(vhw))
	return vhw
}

func download(vhw []byte) {
	dn, errmsg := client.NewDownloadObject(vhw)
	if errmsg != nil {
		logrus.Panicf("[DownLoadFile]ERR:%s\n", pkt.ToError(errmsg))
	}
	read := dn.Load()
	newvhw, count := readData(read)
	logrus.Infof("[DownloadFile]OK:%s,size:%d\n", base58.Encode(newvhw), count)
}

func readData(read io.Reader) ([]byte, int) {
	readbuf := make([]byte, 8192)
	count := 0
	sha256Digest := sha256.New()
	for {
		num, err := read.Read(readbuf)
		if err != nil && err != io.EOF {
			logrus.Panicf("[DownLoadFile]Read ERR:%s\n", err)
		}
		if num > 0 {
			count = count + num
			bs := readbuf[0:num]
			sha256Digest.Write(bs)
		}
		if err != nil && err == io.EOF {
			break
		}
	}
	return sha256Digest.Sum(nil), count
}

func downloadRange(vhw []byte) {
	if len(data) < epos {
		logrus.Panicf("[DownLoadFile]ERR:%d<%d\n", len(data), epos)
	}
	dn, errmsg := client.NewDownloadObject(vhw)
	if errmsg != nil {
		logrus.Panicf("[DownLoadFile]ERR:%s\n", pkt.ToError(errmsg))
	}
	bs := data[spos:epos]
	sha256Digest := sha256.New()
	sha256Digest.Write(bs)
	logrus.Infof("[DownloadFile]Start download %d--%d,hash:%s,size:%d\n",
		spos, epos, base58.Encode(sha256Digest.Sum(nil)), (epos - spos))
	read := dn.LoadRange(int64(spos), int64(epos))
	newvhw, count := readData(read)
	logrus.Infof("[DownloadFile]Download %d--%d OK,hash:%s,size:%d\n", spos, epos, base58.Encode(newvhw), count)
}
