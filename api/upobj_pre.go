package api

import (
	"bytes"
	"os"
	"sync"
	"time"

	"github.com/aurawing/eos-go/btcsuite/btcutil/base58"
	"github.com/sirupsen/logrus"
	"github.com/yottachain/YTCoreService/api/cache"
	"github.com/yottachain/YTCoreService/codec"
	"github.com/yottachain/YTCoreService/env"
	"github.com/yottachain/YTCoreService/net"
	"github.com/yottachain/YTCoreService/pkt"
	"google.golang.org/protobuf/proto"
)

type UploadObjectToDisk struct {
	UploadObject
	Bucket    string
	ObjectKey string
	out       string
}

func NewUploadObjectToDisk(c *Client, bucketname, objectname string) *UploadObjectToDisk {
	p := &UpProgress{Length: env.NewAtomInt64(0), ReadinLength: env.NewAtomInt64(0), ReadOutLength: env.NewAtomInt64(0), WriteLength: env.NewAtomInt64(0)}
	return &UploadObjectToDisk{UploadObject{UClient: c, PRO: p}, bucketname, objectname, ""}
}

func (ud *UploadObjectToDisk) UploadMultiFile(path []string) *pkt.ErrorMessage {
	enc, err := codec.NewMultiFileEncoder(path)
	if err != nil {
		logrus.Errorf("[NewMultiFileEncoder]ERR:%s\n", err)
		return pkt.NewErrorMsg(pkt.CODEC_ERROR, err.Error())
	}
	ud.Encoder = enc
	defer enc.Close()
	return ud.Upload()
}

func (ud *UploadObjectToDisk) UploadFile(path string) *pkt.ErrorMessage {
	enc, err := codec.NewFileEncoder(path)
	if err != nil {
		logrus.Errorf("[NewFileEncoder]Path:%s,ERR:%s\n", path, err)
		return pkt.NewErrorMsg(pkt.CODEC_ERROR, err.Error())
	}
	ud.Encoder = enc
	defer enc.Close()
	return ud.Upload()
}

func (ud *UploadObjectToDisk) UploadBytes(data []byte) *pkt.ErrorMessage {
	enc, err := codec.NewBytesEncoder(data)
	if err != nil {
		logrus.Errorf("[NewBytesEncoder]ERR:%s\n", err)
		return pkt.NewErrorMsg(pkt.CODEC_ERROR, err.Error())
	}
	ud.Encoder = enc
	defer enc.Close()
	return ud.Upload()
}

func (ud *UploadObjectToDisk) Upload() (reserr *pkt.ErrorMessage) {
	l := cache.AddSyncList(ud.Encoder.GetVHW())
	defer func() {
		if r := recover(); r != nil {
			env.TraceError("[UploadObjectToDisk]")
			ud.ERR.Store(pkt.NewErrorMsg(pkt.SERVER_ERROR, "Unknown error"))
			reserr = pkt.NewErrorMsg(pkt.SERVER_ERROR, "Unknown error")
		}
		cache.DelSyncList(ud.Encoder.GetVHW(), l)
	}()
	s3key := ud.Bucket + "/" + ud.ObjectKey
	ud.PRO.Length.Set(ud.Encoder.GetLength())
	exist := cache.SyncObjectExists(ud.Encoder.GetVHW())
	p := ud.makePath(base58.Encode(ud.Encoder.GetVHW()))
	if exist {
		ud.PRO.ReadinLength.Set(ud.Encoder.GetLength())
		ud.PRO.ReadOutLength.Set(ud.Encoder.GetLength())
		ud.PRO.WriteLength.Set(ud.Encoder.GetLength())
		codec.Append(s3key, p)
		logrus.Infof("[UploadObjectToDisk][%s]Already exists.\n", s3key)
	} else {
		enc := codec.NewEncoder(ud.UClient.UserId, ud.UClient.SignKey.KeyNumber,
			ud.UClient.StoreKey.KeyNumber, ud.UClient.SignKey.Sign, s3key, ud.Encoder, ud)
		enc.HandleProgress(ud.PRO.ReadinLength, ud.PRO.ReadOutLength, ud.PRO.WriteLength)
		logrus.Infof("[UploadObjectToDisk][%s]Start encode object...\n", s3key)
		err := enc.Handle(p)
		if err != nil {
			logrus.Errorf("[UploadObjectToDisk][%s]Handle ERR:%s\n", s3key, err)
			return err
		}
		inserterr := cache.InsertSyncObject(enc.GetSHA256())
		if inserterr != nil {
			logrus.Errorf("[UploadObjectToDisk][%s]InsertSyncObject ERR:%s\n", s3key, inserterr)
			return pkt.NewErrorMsg(pkt.SERVER_ERROR, inserterr.Error())
		}
		logrus.Infof("[UploadObjectToDisk][%s]Upload object OK.\n", s3key)
	}
	return nil
}

func (ud *UploadObjectToDisk) Check(b *codec.PlainBlock, id int) (*codec.EncodedBlock, *pkt.ErrorMessage) {
	b.Sum()

	req := &pkt.CheckBlockDupReq{
		UserId:    &ud.UClient.UserId,
		SignData:  &ud.UClient.SignKey.Sign,
		KeyNumber: &ud.UClient.SignKey.KeyNumber,
		VHP:       b.VHP,
	}
	var resp proto.Message
	for {
		res, errmsg := net.RequestSN(req)
		if errmsg != nil {
			logrus.Warnf("[UploadObjectToDisk][%s/%s]CheckBlockDup ERR:%s\n", ud.Bucket, ud.ObjectKey, pkt.ToError(errmsg))
			time.Sleep(time.Duration(env.SN_RETRY_WAIT) * time.Second)
		} else {
			resp = res
			break
		}
	}
	dupResp, ok := resp.(*pkt.UploadBlockDupResp)
	if ok {
		keu, vhb := ud.CheckBlockDup(dupResp, b)
		if keu != nil {
			logrus.Infof("[UploadObjectToDisk][%s/%s]Write Block %d:repeat\n", ud.Bucket, ud.ObjectKey, id)
			return &codec.EncodedBlock{IsDup: true, OriginalSize: b.OriginalSize,
				RealSize: b.Length(), VHP: b.VHP, KEU: keu, VHB: vhb}, nil
		}
	}
	bb, err := ud.makeNODupBlock(b)
	if err != nil {
		logrus.Warnf("[UploadObjectToDisk][%s/%s]MakeNODupBlock ERR:%s\n", ud.Bucket, ud.ObjectKey, err)
		return nil, err
	}
	logrus.Infof("[UploadObjectToDisk][%s/%s]Write Block %d:no-repeat\n", ud.Bucket, ud.ObjectKey, id)
	return bb, nil
}

func (ud *UploadObjectToDisk) makeNODupBlock(b *codec.PlainBlock) (*codec.EncodedBlock, *pkt.ErrorMessage) {
	ks := codec.GenerateRandomKey()
	rsize := b.Length()
	aes := codec.NewBlockAESEncryptor(b, ks)
	eblk, err := aes.Encrypt()
	if err != nil {
		return nil, pkt.NewErrorMsg(pkt.INVALID_ARGS, err.Error())
	}
	keu := codec.ECBEncryptNoPad(ks, ud.UClient.StoreKey.AESKey)
	ked := codec.ECBEncryptNoPad(ks, b.KD)
	return &codec.EncodedBlock{IsDup: false, OriginalSize: b.OriginalSize,
		RealSize: rsize, VHP: b.VHP, KEU: keu, KED: ked, DATA: eblk.Data}, nil
}

func (ud *UploadObjectToDisk) CheckBlockDup(resp *pkt.UploadBlockDupResp, b *codec.PlainBlock) ([]byte, []byte) {
	keds := resp.Keds.KED
	vhbs := resp.Vhbs.VHB
	ars := resp.Ars.AR
	for index, ked := range keds {
		ks := codec.ECBDecryptNoPad(ked, b.KD)
		aes := codec.NewBlockAESEncryptor(b, ks)
		eblk, err := aes.Encrypt()
		if err != nil {
			logrus.Warnf("[UploadObjectToDisk][%s/%s]CheckBlockDup ERR:%s\n", ud.Bucket, ud.ObjectKey, err)
			return nil, nil
		}
		var vhb []byte
		if eblk.NeedEncode() {
			if ars[index] == codec.AR_RS_MODE {
				logrus.Warnf("[UploadObjectToDisk][%s/%s]CheckBlockDup ERR:RS Not supported\n", ud.Bucket, ud.ObjectKey)
				return nil, nil
			} else {
				enc := codec.NewErasureEncoder(eblk)
				err = enc.Encode()
				if err != nil {
					logrus.Warnf("[UploadObjectToDisk][%s/%s]CheckBlockDup ERR:%s\n", ud.Bucket, ud.ObjectKey, err)
					return nil, nil
				}
				vhb = eblk.VHB
			}
		} else {
			err = eblk.MakeVHB()
			if err != nil {
				logrus.Warnf("[UploadObjectToDisk][%s/%s]CheckBlockDup ERR:%s\n", ud.Bucket, ud.ObjectKey, err)
				return nil, nil
			}
			vhb = eblk.VHB
		}
		if bytes.Equal(vhb, vhbs[index]) {
			return codec.ECBEncryptNoPad(ks, ud.UClient.StoreKey.AESKey), vhb
		}
	}
	return nil, nil
}

var pathmap sync.Map

func (ud *UploadObjectToDisk) makePath(hash string) string {
	p := env.GetCache() + hash[0:2] + "/" + hash[2:4]
	_, ok := pathmap.Load(p)
	if !ok {
		os.MkdirAll(p, os.ModePerm)
		pathmap.Store(p, "")
	}
	ud.out = p + "/" + hash
	return ud.out
}

func (ud *UploadObjectToDisk) OutPath() string {
	return ud.out
}
