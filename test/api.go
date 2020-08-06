package test

import (
	"crypto/sha256"
	"encoding/binary"
	"io"
	"math/rand"
	"os"
	"time"

	"github.com/aurawing/eos-go/btcsuite/btcutil/base58"
	"github.com/sirupsen/logrus"
	"github.com/yottachain/YTCoreService/api"
	"github.com/yottachain/YTCoreService/pkt"
)

var istest bool = false
var testsize int64 = 1024 * 1024 * 9

func UpAndDown() {
	var user string
	var pkey string
	if istest {
		os.Setenv("YTFS.snlist", "conf/snlistYF.properties")
		user = "username1234"
		pkey = "5KfbRow4L71fZnnu9XEnkmVqByi6CSmRiADJCx6asRS4TUEkU79"
	} else {
		os.Setenv("YTFS.snlist", "conf/snlistZW.properties")
		user = "ianmooneyy11"
		pkey = "5JnLRW1bTRD2bxo93wZ1qnpXfMDHzA97qcQjabnoqgmJTt7kBoH"
	}
	api.StartApi()
	c, err := api.NewClient(user, pkey)
	if err != nil {
		logrus.Panicf("[NewClient]ERR:%s\n", err)
	}
	up := c.NewUploadObject()
	srcbs := MakeData(testsize)
	vhw, errmsg := up.UploadBytes(srcbs)
	if errmsg != nil {
		logrus.Panicf("[UploadFile]ERR:%s\n", pkt.ToError(errmsg))
	}
	logrus.Infof("[UploadFile]OK:%s\n", base58.Encode(vhw))

	dn, errmsg := c.NewDownloadObject(vhw)
	if errmsg != nil {
		return
	}

	readbuf := make([]byte, 8192)
	read := dn.Load()
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

	newvhw := sha256Digest.Sum(nil)
	logrus.Infof("[DownloadFile]OK:%s,size:%d\n", base58.Encode(newvhw), count)

	//download
	spos := 1024*1024*5 + 798
	epos := 1024*1024*8 + 12
	if len(srcbs) < epos {
		return
	}

	bs := srcbs[spos:epos]
	sha256Digest = sha256.New()
	sha256Digest.Write(bs)
	logrus.Infof("[DownloadFile]Start download %d--%d,hash:%s,size:%d\n",
		spos, epos, base58.Encode(sha256Digest.Sum(nil)), (epos - spos))

	read = dn.LoadRange(int64(spos), int64(epos))
	count = 0
	sha256Digest = sha256.New()
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
	newvhw = sha256Digest.Sum(nil)
	logrus.Infof("[DownloadFile]Download %d--%d OK,hash:%s,size:%d\n", spos, epos, base58.Encode(newvhw), count)

}

func MakeData(size int64) []byte {
	rand.Seed(time.Now().UnixNano())
	loop := size / 8
	buf := make([]byte, loop*8)
	for ii := int64(0); ii < loop; ii++ {
		binary.BigEndian.PutUint64(buf[ii*8:(ii+1)*8], rand.Uint64())
	}
	return buf
}
