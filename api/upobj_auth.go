package api

import (
	"sync"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/yottachain/YTCoreService/env"
	"github.com/yottachain/YTCoreService/pkt"
)

type UploadObjectAuth struct {
	UploadObject
	Info *AuthImporter
}

func NewUploadObjectAuth(c *Client) (*UploadObjectAuth, *pkt.ErrorMessage) {
	u := &UploadObjectAuth{UploadObject: UploadObject{}}
	u.ActiveTime = env.NewAtomInt64(0)
	u.activesign = make(chan int)
	u.UClient = c
	u.Exist = false
	return u, nil
}

func (self *UploadObjectAuth) UploadAuthFile(info *AuthImporter) *pkt.ErrorMessage {
	self.Info = info
	return self.Upload()
}

func (self *UploadObjectAuth) GetLength() int64 {
	if self.Info != nil {
		return self.Info.Length
	}
	return 0
}

func (self *UploadObjectAuth) GetSHA256() []byte {
	if self.Info != nil {
		return self.Info.VHW
	}
	return nil
}

func (self *UploadObjectAuth) GetMD5() []byte {
	return nil
}

func (self *UploadObjectAuth) GetProgress() int32 {
	return 0
}

func (self *UploadObjectAuth) Upload() (reserr *pkt.ErrorMessage) {
	defer func() {
		if r := recover(); r != nil {
			env.TraceError("[AuthUpload]")
			self.ERR.Store(pkt.NewErrorMsg(pkt.SERVER_ERROR, "Unknown error"))
			reserr = pkt.NewErrorMsg(pkt.SERVER_ERROR, "Unknown error")
		}

	}()
	err := self.initUpload(self.Info.VHW, self.Info.Length)
	if err != nil {
		return err
	}
	logrus.Infof("[AuthUpload][%s]Start upload object\n", self.VNU.Hex())
	if self.Exist {
		logrus.Infof("[AuthUpload][%s]Already exists.\n", self.VNU.Hex())
	} else {
		wgroup := sync.WaitGroup{}
		self.ActiveTime.Set(time.Now().Unix())
		go self.waitcheck()
		var id uint32 = 0
		for _, ref := range self.Info.REFS {
			if self.ERR.Load() != nil {
				break
			}
			if self.IdExist(uint32(ref.Id)) {
				logrus.Infof("[AuthUpload][%s][%d]Block has been uploaded.\n", self.VNU.Hex(), id)
			} else {
				wgroup.Add(1)
				StartUploadBlockAuth(ref, &self.UploadObject, &wgroup)
			}
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
			logrus.Errorf("[AuthUpload][%s]Upload object ERR:%s\n", self.VNU.Hex(), pkt.ToError(errmsg))
			return errmsg
		} else {
			logrus.Infof("[AuthUpload][%s]Upload object OK.\n", self.VNU.Hex())
		}
	}
	return nil
}

func (self *UploadObjectAuth) writeMeta() *pkt.ErrorMessage {
	meta := self.Info.Meta
	errmsg := self.UClient.NewObjectAccessor().CreateObject(self.Info.bucketName, self.Info.filename, self.VNU, meta)
	if errmsg != nil {
		logrus.Errorf("[AuthUpload][%s]WriteMeta ERR:%s,%s/%s\n", self.VNU, pkt.ToError(errmsg), self.Info.bucketName, self.Info.filename)
		return errmsg
	} else {
		logrus.Infof("[AuthUpload][%s]WriteMeta OK,%s/%s\n", self.VNU, self.Info.bucketName, self.Info.filename)
	}
	return nil
}
