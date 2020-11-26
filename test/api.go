package test

import (
	"crypto/sha256"
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
const testsize = 1024 * 1024 * 10
const spos = 1024*1024*5 + 798
const epos = 1024*1024*8 + 12
const filePath = "d:/test.rar"
const savePath = "d:/test"

var data []byte
var client *api.Client

func initApi() {
	var user string
	var pkey string
	if yfnet {
		os.Setenv("YTFS.snlist", "conf/snlistYF.properties")
		//user = "username1234"
		//pkey = "5KfbRow4L71fZnnu9XEnkmVqByi6CSmRiADJCx6asRS4TUEkU79"
		user = "devtestuser1"
		pkey = "5KTF2yAamvcaoDu6juAvxT5nxTn3UGfNoY2CJn8VAQ4giAfma2a"
		//user = "devvtest1111"
		//pkey = "5JReF8eeGS53B8prdcrSfTf6dGbvu3QJ6KceE8rLsnRaNMMCYw9"
	} else {
		os.Setenv("YTFS.snlist", "conf/snlistZW.properties")
		user = "ianmooneyy11"
		pkey = "5JnLRW1bTRD2bxo93wZ1qnpXfMDHzA97qcQjabnoqgmJTt7kBoH"
		//user = "nloadzooqwer"
		//pkey = "5KRWqgvdYVomJhobea4AbXpi9nR2wj53Hzy2JgUpAgZAry8WyeG"
	}
	api.StartApi()
	c, err := api.NewClient(user, pkey)
	if err != nil {
		logrus.Panicf("[NewClient]ERR:%s\n", err)
	}
	client = c
	data = env.MakeRandData(testsize)
}

func UpAndDownBytes() {
	initApi()
	vhw, _ := upload()
	download(vhw)
	downloadRange(vhw)
	//client.NewObjectAccessor().DeleteObjectV2(vnu)
}

func UpAndDownFile() {
	initApi()
	vhw := uploadFile()
	//download(vhw)
	saveFile(vhw)
}

func DownLoadByKey() {
	initApi()
	dn, errmsg := client.NewDownloadFile("newjava", "tmpfile_newjava_60978c.txt1", primitive.NilObjectID)
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

var filePaths = []string{"d:/p2p/nnst2_1",
	"d:/p2p/nnst2_2",
	"d:/p2p/nnst2_3",
	"d:/p2p/nnst2_4",
	"d:/p2p/nnst2_5",
	"d:/p2p/nnst2_6"}

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
	errmsg := up.UploadMultiFile(filePaths)
	//vhw, errmsg := up.UploadFile(filePath)
	if errmsg != nil {
		logrus.Panicf("[UploadFile]ERR:%s\n", pkt.ToError(errmsg))
	}
	vhw := up.GetSHA256()
	logrus.Infof("[UploadFile]Progress:%d\n", up.GetProgress())
	logrus.Infof("[UploadFile]OK:%s\n", base58.Encode(vhw))
	<-oksign
	return vhw
}

func upload() ([]byte, primitive.ObjectID) {
	up := client.NewUploadObject()
	errmsg := up.UploadBytes(data)
	if errmsg != nil {
		logrus.Panicf("[UploadFile]ERR:%s\n", pkt.ToError(errmsg))
	}
	vhw := up.GetSHA256()
	logrus.Infof("[UploadFile]OK:%s\n", base58.Encode(vhw))
	return vhw, up.VNU
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
