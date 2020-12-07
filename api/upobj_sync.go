package api

import (
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aurawing/eos-go/btcsuite/btcutil/base58"
	"github.com/sirupsen/logrus"
	"github.com/yottachain/YTCoreService/codec"
	"github.com/yottachain/YTCoreService/env"
	"github.com/yottachain/YTCoreService/pkt"
)

type UploadObjectSync struct {
	UploadObject
	decoder *codec.Decoder
}

func NewUploadObjectSync(sha256 []byte) (*UploadObjectSync, *pkt.ErrorMessage) {
	u := &UploadObjectSync{UploadObject: UploadObject{}}
	u.ActiveTime = new(int64)
	u.activesign = make(chan int)
	u.PRO = &UpProgress{Length: new(int64), ReadinLength: new(int64), ReadOutLength: new(int64), WriteLength: new(int64)}
	err := u.createDecoder(sha256)
	if err != nil {
		return nil, pkt.NewErrorMsg(pkt.INVALID_ARGS, err.Error())
	}
	return u, nil
}

func (self *UploadObjectSync) createDecoder(sha256 []byte) error {
	hash := base58.Encode(sha256)
	p := env.GetCache() + hash[0:2] + "/" + hash[2:4] + "/" + hash
	dec, err := codec.NewDecoder(p)
	if err != nil {
		logrus.Errorf("[SyncUpload][%s]NewDecoder err:%s\n", p, err)
		return err
	}
	self.decoder = dec
	c, err := AddClient(dec.UserId, dec.KeyNumber, dec.Sign)
	if err != nil {
		logrus.Errorf("[SyncUpload][%s]AddClient err:%s\n", p, err)
		return err
	}
	self.UClient = c
	return nil
}

func (self *UploadObjectSync) Upload() (reserr *pkt.ErrorMessage) {
	defer func() {
		if r := recover(); r != nil {
			env.TraceError("[SyncUpload]")
			self.ERR.Store(pkt.NewErrorMsg(pkt.SERVER_ERROR, "Unknown error"))
			reserr = pkt.NewErrorMsg(pkt.SERVER_ERROR, "Unknown error")
		}
		self.decoder.Close()
	}()
	atomic.StoreInt64(self.PRO.Length, self.decoder.GetLength())
	err := self.initUpload(self.GetSHA256(), self.GetLength())
	if err != nil {
		return err
	}
	logrus.Infof("[SyncUpload][%s]Start upload object,Path:%s\n", self.VNU.Hex(), self.decoder.GetPath())
	if self.Exist {
		atomic.StoreInt64(self.PRO.ReadinLength, self.decoder.GetLength())
		atomic.StoreInt64(self.PRO.ReadOutLength, self.decoder.GetLength())
		atomic.StoreInt64(self.PRO.WriteLength, self.decoder.GetLength())
		logrus.Infof("[SyncUpload][%s]Already exists.\n", self.VNU.Hex())
	} else {
		wgroup := sync.WaitGroup{}
		atomic.StoreInt64(self.ActiveTime, time.Now().Unix())
		go self.waitcheck()
		var id uint32 = 0
		for {
			b, err := self.decoder.ReadNext()
			if err != nil {
				return pkt.NewErrorMsg(pkt.INVALID_ARGS, err.Error())
			}
			if b == nil {
				break
			}
			if self.ERR.Load() != nil {
				break
			}
			atomic.StoreInt64(self.PRO.ReadinLength, self.decoder.GetReadinTotal())
			atomic.StoreInt64(self.PRO.ReadOutLength, self.decoder.GetReadoutTotal())
			if self.IdExist(id) {
				atomic.AddInt64(self.PRO.WriteLength, b.Length())
				logrus.Infof("[SyncUpload][%s][%d]Block has been uploaded.\n", self.VNU.Hex(), id)
			} else {
				wgroup.Add(1)
				StartUploadBlockSync(int16(id), b, &self.UploadObject, &wgroup)
			}
			id++
		}
		wgroup.Wait()
		<-self.activesign
		var errmsg *pkt.ErrorMessage
		v := self.ERR.Load()
		if v != nil {
			errmsg = v.(*pkt.ErrorMessage)
		} else {
			errmsg = self.complete(self.GetSHA256())
		}
		if errmsg != nil {
			logrus.Errorf("[SyncUpload][%s]Upload ERR:%s\n", self.VNU.Hex(), pkt.ToError(errmsg))
			return errmsg
		} else {
			logrus.Infof("[SyncUpload][%s]Upload object OK.\n", self.VNU.Hex())
		}
	}
	return self.writeMeta()
}

func (self *UploadObjectSync) writeMeta() *pkt.ErrorMessage {
	meta := MetaTobytes(self.GetLength(), self.GetMD5())
	for {
		ss, err := self.decoder.ReadNextKey()
		if err != nil {
			logrus.Errorf("[SyncUpload][%s]Read key from %s ERR:%s.\n", self.VNU.Hex(), self.decoder.GetPath(), err)
			return pkt.NewErrorMsg(pkt.INVALID_ARGS, err.Error())
		}
		if ss == "" {
			if env.StartSync == 2 {
				os.Remove(self.decoder.GetPath())
			}
			return nil
		}
		pos := strings.Index(ss, "/")
		buck := ss[0:pos]
		name := ss[pos+1:]
		errmsg := self.UClient.NewObjectAccessor().CreateObject(buck, name, self.VNU, meta)
		if errmsg != nil {
			logrus.Errorf("[SyncUpload][%s]WriteMeta ERR:%s,%s/%s\n", self.VNU, pkt.ToError(errmsg), buck, name)
			return errmsg
		} else {
			logrus.Infof("[SyncUpload][%s]WriteMeta OK,%s/%s\n", self.VNU, buck, name)
		}
	}
}

func (self *UploadObjectSync) GetLength() int64 {
	if self.decoder != nil {
		return self.decoder.GetLength()
	}
	return 0
}

func (self *UploadObjectSync) GetSHA256() []byte {
	if self.decoder != nil {
		return self.decoder.GetVHW()
	}
	return nil
}

func (self *UploadObjectSync) GetMD5() []byte {
	if self.decoder != nil {
		return self.decoder.GetMD5()
	}
	return nil
}
