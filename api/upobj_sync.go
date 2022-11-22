package api

import (
	"strings"
	"sync"

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

func NewUploadEncObject(filename string) (*UploadObjectSync, *pkt.ErrorMessage) {
	u := &UploadObjectSync{UploadObject: UploadObject{}}
	u.PRO = &UpProgress{Length: env.NewAtomInt64(0), ReadinLength: env.NewAtomInt64(0), ReadOutLength: env.NewAtomInt64(0), WriteLength: env.NewAtomInt64(0)}
	err := u.createDecoder2(filename)
	if err != nil {
		return nil, pkt.NewErrorMsg(pkt.CODEC_ERROR, err.Error())
	}
	return u, nil
}

func (upload *UploadObjectSync) createDecoder2(filename string) error {
	dec, err := codec.NewDecoder(filename)
	if err != nil {
		logrus.Errorf("[SyncUpload][%s]NewDecoder err:%s\n", filename, err)
		return err
	}
	upload.decoder = dec
	c, err := AddClient(dec.UserId, dec.KeyNumber, dec.StoreNumber, dec.Sign)
	if err != nil {
		logrus.Errorf("[SyncUpload][%s]AddClient err:%s\n", filename, err)
		return err
	}
	upload.UClient = c
	return nil
}

func NewUploadObjectSync(sha256 []byte) (*UploadObjectSync, *pkt.ErrorMessage) {
	u := &UploadObjectSync{UploadObject: UploadObject{}}
	u.PRO = &UpProgress{Length: env.NewAtomInt64(0), ReadinLength: env.NewAtomInt64(0), ReadOutLength: env.NewAtomInt64(0), WriteLength: env.NewAtomInt64(0)}
	err := u.createDecoder(sha256)
	if err != nil {
		return nil, pkt.NewErrorMsg(pkt.CODEC_ERROR, err.Error())
	}
	return u, nil
}

func (upload *UploadObjectSync) createDecoder(sha256 []byte) error {
	hash := base58.Encode(sha256)
	p := env.GetCache() + hash[0:2] + "/" + hash[2:4] + "/" + hash
	dec, err := codec.NewDecoder(p)
	if err != nil {
		logrus.Errorf("[SyncUpload][%s]NewDecoder err:%s\n", p, err)
		return err
	}
	upload.decoder = dec
	c, err := AddClient(dec.UserId, dec.KeyNumber, dec.StoreNumber, dec.Sign)
	if err != nil {
		logrus.Errorf("[SyncUpload][%s]AddClient err:%s\n", p, err)
		return err
	}
	upload.UClient = c
	return nil
}

func (upload *UploadObjectSync) Upload() (reserr *pkt.ErrorMessage) {
	defer func() {
		if r := recover(); r != nil {
			env.TraceError("[SyncUpload]")
			upload.ERR.Store(pkt.NewErrorMsg(pkt.SERVER_ERROR, "Unknown error"))
			reserr = pkt.NewErrorMsg(pkt.SERVER_ERROR, "Unknown error")
		}
		upload.decoder.Close()
	}()
	upload.PRO.Length.Set(upload.decoder.GetLength())
	err := upload.initUpload(upload.GetSHA256(), upload.GetLength())
	if err != nil {
		return err
	}
	logrus.Infof("[SyncUpload][%s]Start upload object,Path:%s\n", upload.VNU.Hex(), upload.decoder.GetPath())
	if upload.Exist {
		upload.PRO.ReadinLength.Set(upload.decoder.GetLength())
		upload.PRO.ReadOutLength.Set(upload.decoder.GetLength())
		upload.PRO.WriteLength.Set(upload.decoder.GetLength())
		logrus.Infof("[SyncUpload][%s]Already exists.\n", upload.VNU.Hex())
	} else {
		wgroup := sync.WaitGroup{}
		var id uint32 = 0
		for {
			b, err := upload.decoder.ReadNext()
			if err != nil {
				return pkt.NewErrorMsg(pkt.CODEC_ERROR, err.Error())
			}
			if b == nil {
				break
			}
			if upload.ERR.Load() != nil {
				break
			}
			upload.PRO.ReadinLength.Set(upload.decoder.GetReadinTotal())
			upload.PRO.ReadOutLength.Set(upload.decoder.GetReadoutTotal())
			if upload.IdExist(id) {
				upload.PRO.WriteLength.Add(b.Length())
				logrus.Infof("[SyncUpload][%s][%d]Block has been uploaded.\n", upload.VNU.Hex(), id)
			} else {
				wgroup.Add(1)
				StartUploadBlockSync(int16(id), b, &upload.UploadObject, &wgroup)
			}
			id++
		}
		wgroup.Wait()
		var errmsg *pkt.ErrorMessage
		v := upload.ERR.Load()
		if v != nil {
			errmsg = v.(*pkt.ErrorMessage)
		} else {
			errmsg = upload.complete(upload.GetSHA256())
		}
		if errmsg != nil {
			logrus.Errorf("[SyncUpload][%s]Upload object %s,ERR:%s\n", upload.VNU.Hex(), upload.decoder.GetPath(), pkt.ToError(errmsg))
			return errmsg
		} else {
			logrus.Infof("[SyncUpload][%s]Upload object %s OK.\n", upload.VNU.Hex(), upload.decoder.GetPath())
		}
	}
	return upload.writeMeta()
}

func (upload *UploadObjectSync) writeMeta() *pkt.ErrorMessage {
	meta := MetaTobytes(upload.GetLength(), upload.GetMD5())
	for {
		ss, err := upload.decoder.ReadNextKey()
		if err != nil {
			logrus.Errorf("[SyncUpload][%s]Read key from %s ERR:%s.\n", upload.VNU.Hex(), upload.decoder.GetPath(), err)
			return pkt.NewErrorMsg(pkt.CODEC_ERROR, err.Error())
		}
		if ss == "" {
			return nil
		}
		pos := strings.Index(ss, "/")
		buck := ss[0:pos]
		name := ss[pos+1:]
		errmsg := upload.UClient.NewObjectAccessor().CreateObject(buck, name, upload.VNU, meta)
		if errmsg != nil {
			logrus.Errorf("[SyncUpload][%s]WriteMeta ERR:%s,%s/%s\n", upload.VNU, pkt.ToError(errmsg), buck, name)
			return errmsg
		} else {
			logrus.Infof("[SyncUpload][%s]WriteMeta OK,%s/%s\n", upload.VNU, buck, name)
		}
	}
}

func (upload *UploadObjectSync) GetPath() string {
	if upload.decoder != nil {
		return upload.decoder.GetPath()
	}
	return ""
}

func (upload *UploadObjectSync) GetLength() int64 {
	if upload.decoder != nil {
		return upload.decoder.GetLength()
	}
	return 0
}

func (upload *UploadObjectSync) GetSHA256() []byte {
	if upload.decoder != nil {
		return upload.decoder.GetVHW()
	}
	return nil
}

func (upload *UploadObjectSync) GetMD5() []byte {
	if upload.decoder != nil {
		return upload.decoder.GetMD5()
	}
	return nil
}
