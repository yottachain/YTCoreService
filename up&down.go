package main

import (
	"bytes"
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

var client *api.Client

var (
	FileSize int64 = 1024*1024*1 + 8192
	Spos     int64 = FileSize * 382 / 1000
	Epos     int64 = Spos * 2
)

func TestApi() {
	env.Console = false
	api.InitApi()
	for {
		cs := api.GetClients()
		if len(cs) > 0 {
			client = cs[0]
			break
		} else {
			time.Sleep(time.Duration(1) * time.Second)
		}
	}
	confpath := env.YTFS_HOME + "conf/test.properties"
	config, err := env.NewConfig(confpath)
	if err != nil {
		logrus.Errorf("[Test]No properties file could be found for ytfs service:%s\n", confpath)
		stdTest()
		return
	}
	stdtest := config.GetBool("STDTest", true)
	ThreadNum := config.GetRangeInt("ThreadNum", 1, 100, 20)
	Loop := config.GetRangeInt("Loop", 1, 100, 5)
	size := config.GetRangeInt("FileSize", 1, 1024*100, 15)
	FileSize = 1024 * int64(size)
	Spos = FileSize * 382 / 1000
	Epos = Spos * 2
	if stdtest {
		stdTest()
	} else {
		loopUp(ThreadNum, Loop)
	}
}

func loopUp(threadnum, loop int) error {
	for ii := 0; ii < threadnum; ii++ {
		go func() {
			for ii := 0; ii < loop; ii++ {
				upload(env.MakeRandData(FileSize))
			}
		}()
	}
	select {}
}

func stdTest() {
	var data []byte = env.MakeRandData(FileSize)
	vhw, _ := upload(data)
	if vhw != nil {
		download(vhw)
		downloadRange(vhw, data)
	}
}

func upload(data []byte) ([]byte, primitive.ObjectID) {
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
		return
	}
	read := dn.Load()
	newvhw, count := readData(read)
	if bytes.Equal(vhw, newvhw) {
		logrus.Infof("[DownloadFile]OK:%s,size:%d\n", base58.Encode(newvhw), count)
	} else {
		logrus.Panicf("[DownloadFile]HASH ERR:%s,size:%d\n", base58.Encode(newvhw), count)
	}
}

func downloadRange(vhw []byte, data []byte) {
	if len(data) < int(Epos) {
		logrus.Panicf("[DownLoadFile]ERR:%d<%d\n", len(data), Epos)
	}
	dn, errmsg := client.NewDownloadObject(vhw)
	if errmsg != nil {
		logrus.Panicf("[DownLoadFile]ERR:%s\n", pkt.ToError(errmsg))
	}
	bs := data[int(Spos):int(Epos)]
	sha256Digest := sha256.New()
	sha256Digest.Write(bs)
	hash := sha256Digest.Sum(nil)
	logrus.Infof("[DownloadFile]Start download %d--%d,hash:%s,size:%d\n",
		Spos, Epos, base58.Encode(hash), (Epos - Spos))
	read := dn.LoadRange(Spos, Epos)
	newvhw, count := readData(read)
	if bytes.Equal(hash, newvhw) {
		logrus.Infof("[DownloadFile]Download %d--%d OK,hash:%s,size:%d\n", Spos, Epos, base58.Encode(newvhw), count)
	} else {
		logrus.Panicf("[DownloadFile]Download %d--%d, hash ERR:%s,size:%d\n", Spos, Epos, base58.Encode(newvhw), count)
	}
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
