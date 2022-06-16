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

func (auth *UploadObjectAuth) UploadAuthFile(info *AuthImporter) *pkt.ErrorMessage {
	auth.Info = info
	return auth.Upload()
}

func (auth *UploadObjectAuth) GetLength() int64 {
	if auth.Info != nil {
		return auth.Info.Length
	}
	return 0
}

func (auth *UploadObjectAuth) GetSHA256() []byte {
	if auth.Info != nil {
		return auth.Info.VHW
	}
	return nil
}

func (auth *UploadObjectAuth) GetMD5() []byte {
	return nil
}

func (auth *UploadObjectAuth) GetProgress() int32 {
	return 0
}

func (auth *UploadObjectAuth) Upload() (reserr *pkt.ErrorMessage) {
	defer func() {
		if r := recover(); r != nil {
			env.TraceError("[AuthUpload]")
			auth.ERR.Store(pkt.NewErrorMsg(pkt.SERVER_ERROR, "Unknown error"))
			reserr = pkt.NewErrorMsg(pkt.SERVER_ERROR, "Unknown error")
		}

	}()
	err := auth.initUpload(auth.Info.VHW, auth.Info.Length)
	if err != nil {
		return err
	}
	logrus.Infof("[AuthUpload][%s]Start upload object\n", auth.VNU.Hex())
	if auth.Exist {
		logrus.Infof("[AuthUpload][%s]Already exists.\n", auth.VNU.Hex())
	} else {
		wgroup := sync.WaitGroup{}
		auth.ActiveTime.Set(time.Now().Unix())
		go auth.waitcheck()
		var id uint32 = 0
		for _, ref := range auth.Info.REFS {
			if auth.ERR.Load() != nil {
				break
			}
			if auth.IdExist(uint32(ref.Id)) {
				logrus.Infof("[AuthUpload][%s][%d]Block has been uploaded.\n", auth.VNU.Hex(), id)
			} else {
				wgroup.Add(1)
				StartUploadBlockAuth(ref, &auth.UploadObject, &wgroup)
			}
		}
		wgroup.Wait()
		<-auth.activesign
		var errmsg *pkt.ErrorMessage
		v := auth.ERR.Load()
		if v != nil {
			errmsg = v.(*pkt.ErrorMessage)
		} else {
			errmsg = auth.complete(auth.GetSHA256())
		}
		if errmsg != nil {
			logrus.Errorf("[AuthUpload][%s]Upload object ERR:%s\n", auth.VNU.Hex(), pkt.ToError(errmsg))
			return errmsg
		} else {
			logrus.Infof("[AuthUpload][%s]Upload object OK.\n", auth.VNU.Hex())
		}
	}
	return nil
}

func (auth *UploadObjectAuth) writeMeta() *pkt.ErrorMessage {
	meta := auth.Info.Meta
	errmsg := auth.UClient.NewObjectAccessor().CreateObject(auth.Info.bucketName, auth.Info.filename, auth.VNU, meta)
	if errmsg != nil {
		logrus.Errorf("[AuthUpload][%s]WriteMeta ERR:%s,%s/%s\n", auth.VNU, pkt.ToError(errmsg), auth.Info.bucketName, auth.Info.filename)
		return errmsg
	} else {
		logrus.Infof("[AuthUpload][%s]WriteMeta OK,%s/%s\n", auth.VNU, auth.Info.bucketName, auth.Info.filename)
	}
	return nil
}
