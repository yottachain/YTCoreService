package test

import (
	"crypto/sha256"
	"io"
	"time"

	"github.com/aurawing/eos-go/btcsuite/btcutil/base58"
	"github.com/sirupsen/logrus"
	"github.com/yottachain/YTCoreService/api"
	"github.com/yottachain/YTCoreService/env"
	"github.com/yottachain/YTCoreService/pkt"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

const testsize = 1024 * 1024 * 10
const spos = 1024*1024*5 + 798
const epos = 1024*1024*8 + 12

var data []byte = env.MakeRandData(testsize)

func UpAndDownBytes() {
	clients := api.GetClients()
	for {
		if len(clients) > 0 {
			break
		} else {
			time.Sleep(time.Duration(1) * time.Second)
			clients = api.GetClients()
		}
	}
	client = clients[0]
	initApi()
	vhw, _ := upload()
	download(vhw)
	downloadRange(vhw)
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
