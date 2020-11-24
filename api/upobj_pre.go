package api

import (
	"bytes"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/sirupsen/logrus"
	"github.com/yottachain/YTCoreService/api/cache"
	"github.com/yottachain/YTCoreService/codec"
	"github.com/yottachain/YTCoreService/env"
	"github.com/yottachain/YTCoreService/net"
	"github.com/yottachain/YTCoreService/pkt"
)

type UploadObjectToDisk struct {
	UploadObject
	Bucket    string
	ObjectKey string
}

func NewUploadObjectToDisk(c *Client, bucketname, objectname string) *UploadObjectToDisk {
	p := &UpProgress{Length: new(int64), ReadinLength: new(int64), ReadOutLength: new(int64), WriteLength: new(int64)}
	return &UploadObjectToDisk{UploadObject{UClient: c, PRO: p}, bucketname, objectname}
}

func (self *UploadObjectToDisk) UploadMultiFile(path []string) *pkt.ErrorMessage {
	enc, err := codec.NewMultiFileEncoder(path)
	if err != nil {
		logrus.Errorf("[NewMultiFileEncoder]ERR:%s\n", err)
		return pkt.NewErrorMsg(pkt.INVALID_ARGS, err.Error())
	}
	self.Encoder = enc
	defer enc.Close()
	return self.Upload()
}

func (self *UploadObjectToDisk) UploadFile(path string) *pkt.ErrorMessage {
	enc, err := codec.NewFileEncoder(path)
	if err != nil {
		logrus.Errorf("[NewFileEncoder]Path:%s,ERR:%s\n", path, err)
		return pkt.NewErrorMsg(pkt.INVALID_ARGS, err.Error())
	}
	self.Encoder = enc
	defer enc.Close()
	return self.Upload()
}

func (self *UploadObjectToDisk) UploadBytes(data []byte) *pkt.ErrorMessage {
	enc, err := codec.NewBytesEncoder(data)
	if err != nil {
		logrus.Errorf("[NewBytesEncoder]ERR:%s\n", err)
		return pkt.NewErrorMsg(pkt.INVALID_ARGS, err.Error())
	}
	self.Encoder = enc
	defer enc.Close()
	return self.Upload()
}

func (self *UploadObjectToDisk) Upload() (reserr *pkt.ErrorMessage) {
	defer func() {
		if r := recover(); r != nil {
			env.TraceError("[UploadObjectToDisk]")
			self.ERR.Store(pkt.NewErrorMsg(pkt.SERVER_ERROR, "Unknown error"))
			reserr = pkt.NewErrorMsg(pkt.SERVER_ERROR, "Unknown error")
		}
	}()
	atomic.StoreInt64(self.PRO.Length, self.Encoder.GetLength())
	s3key := self.Bucket + "/" + self.ObjectKey
	enc := codec.NewEncoder(self.UClient.UserId, self.UClient.KeyNumber, self.UClient.Sign, s3key, self.Encoder, self)
	enc.HandleProgress(self.PRO.ReadinLength, self.PRO.ReadOutLength, self.PRO.WriteLength)
	logrus.Infof("[UploadObjectToDisk][%s]Start encode object...\n", s3key)
	err := enc.Handle(makePath(enc.GetBaseSHA256()))
	if err != nil {
		logrus.Errorf("[UploadObjectToDisk][%s]Handle ERR:%s\n", s3key, err)
		return pkt.NewErrorMsg(pkt.INVALID_ARGS, err.Error())
	}
	err = cache.InsertSyncObject(enc.GetSHA256())
	if err != nil {
		logrus.Errorf("[UploadObjectToDisk][%s]InsertSyncObject ERR:%s\n", s3key, err)
		return pkt.NewErrorMsg(pkt.INVALID_ARGS, err.Error())
	}
	logrus.Infof("[UploadObjectToDisk][%s]Upload object OK.\n", s3key)
	return nil
}

func (self *UploadObjectToDisk) Check(b *codec.PlainBlock) (*codec.EncodedBlock, error) {
	b.Sum()
	SN := net.GetBlockSuperNode(b.VHP)
	req := &pkt.CheckBlockDupReq{
		UserId:    &self.UClient.UserId,
		SignData:  &self.UClient.Sign,
		KeyNumber: &self.UClient.KeyNumber,
		VHP:       b.VHP,
	}
	var resp proto.Message
	for {
		res, errmsg := net.RequestSN(req, SN, "", env.SN_RETRYTIMES, false)
		if errmsg != nil {
			logrus.Warnf("[UploadObjectToDisk][%s/%s]CheckBlockDup ERR:%s\n", self.Bucket, self.ObjectKey, pkt.ToError(errmsg))
			time.Sleep(time.Duration(env.SN_RETRY_WAIT) * time.Second)
		} else {
			resp = res
			break
		}
	}
	dupResp, ok := resp.(*pkt.UploadBlockDupResp)
	if ok {
		keu, vhb := self.CheckBlockDup(dupResp, b)
		if keu != nil {
			return &codec.EncodedBlock{IsDup: true, OriginalSize: b.OriginalSize,
				RealSize: b.Length(), VHP: b.VHP, KEU: keu, VHB: vhb}, nil
		}
	}
	bb, err := self.makeNODupBlock(b)
	if err != nil {
		logrus.Warnf("[UploadObjectToDisk][%s/%s]MakeNODupBlock ERR:%s\n", self.Bucket, self.ObjectKey, err)
		return nil, err
	}
	return bb, nil
}

func (self *UploadObjectToDisk) makeNODupBlock(b *codec.PlainBlock) (*codec.EncodedBlock, error) {
	ks := codec.GenerateRandomKey()
	rsize := b.Length()
	aes := codec.NewBlockAESEncryptor(b, ks)
	eblk, err := aes.Encrypt()
	if err != nil {
		return nil, err
	}
	keu := codec.ECBEncryptNoPad(ks, self.UClient.AESKey)
	ked := codec.ECBEncryptNoPad(ks, b.KD)
	return &codec.EncodedBlock{IsDup: false, OriginalSize: b.OriginalSize,
		RealSize: rsize, VHP: b.VHP, KEU: keu, KED: ked, DATA: eblk.Data}, nil
}

func (self *UploadObjectToDisk) CheckBlockDup(resp *pkt.UploadBlockDupResp, b *codec.PlainBlock) ([]byte, []byte) {
	keds := resp.Keds.KED
	vhbs := resp.Vhbs.VHB
	ars := resp.Ars.AR
	for index, ked := range keds {
		ks := codec.ECBDecryptNoPad(ked, b.KD)
		aes := codec.NewBlockAESEncryptor(b, ks)
		eblk, err := aes.Encrypt()
		if err != nil {
			logrus.Warnf("[UploadObjectToDisk][%s/%s]CheckBlockDup ERR:%s\n", self.Bucket, self.ObjectKey, err)
			return nil, nil
		}
		var vhb []byte
		if eblk.NeedEncode() {
			if ars[index] == codec.AR_RS_MODE {
				logrus.Warnf("[UploadObjectToDisk][%s/%s]CheckBlockDup ERR:RS Not supported\n", self.Bucket, self.ObjectKey)
				return nil, nil
			} else {
				enc := codec.NewErasureEncoder(eblk)
				err = enc.Encode()
				if err != nil {
					logrus.Warnf("[UploadObjectToDisk][%s/%s]CheckBlockDup ERR:%s\n", self.Bucket, self.ObjectKey, err)
					return nil, nil
				}
				vhb = eblk.VHB
			}
		} else {
			err = eblk.MakeVHB()
			if err != nil {
				logrus.Warnf("[UploadObjectToDisk][%s/%s]CheckBlockDup ERR:%s\n", self.Bucket, self.ObjectKey, err)
				return nil, nil
			}
			vhb = eblk.VHB
		}
		if bytes.Equal(vhb, vhbs[index]) {
			return codec.ECBEncryptNoPad(ks, self.UClient.AESKey), vhb
		}
	}
	return nil, nil
}

var pathmap sync.Map

func makePath(hash string) string {
	p := env.GetCache() + hash[0:2] + "/" + hash[2:4]
	_, ok := pathmap.Load(p)
	if !ok {
		os.MkdirAll(p, os.ModePerm)
		pathmap.Store(p, "")
	}
	return p + "/" + hash
}
