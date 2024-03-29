package api

import (
	"encoding/hex"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/aurawing/eos-go/btcsuite/btcutil/base58"
	"github.com/sirupsen/logrus"
	"github.com/yottachain/YTCoreService/codec"
	"github.com/yottachain/YTCoreService/env"
	"github.com/yottachain/YTCoreService/net"
	"github.com/yottachain/YTCoreService/pkt"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

type UploadObject struct {
	UClient *Client
	Encoder *codec.FileEncoder
	VNU     primitive.ObjectID
	Sign    string
	Stamp   int64
	Blocks  []uint32
	Exist   bool
	ERR     atomic.Value
	PRO     *UpProgress
}

func NewUploadObject(c *Client) *UploadObject {
	p := &UpProgress{Length: env.NewAtomInt64(0), ReadinLength: env.NewAtomInt64(0), ReadOutLength: env.NewAtomInt64(0), WriteLength: env.NewAtomInt64(0)}
	o := &UploadObject{UClient: c, PRO: p}
	return o
}

func (uploadobject *UploadObject) GetLength() int64 {
	if uploadobject.Encoder != nil {
		return uploadobject.Encoder.GetLength()
	}
	return 0
}

func (uploadobject *UploadObject) GetSHA256() []byte {
	if uploadobject.Encoder != nil {
		return uploadobject.Encoder.GetVHW()
	}
	return nil
}

func (uploadobject *UploadObject) GetMD5() []byte {
	if uploadobject.Encoder != nil {
		return uploadobject.Encoder.GetMD5()
	}
	return nil
}

func (uploadobject *UploadObject) UploadMultiFile(path []string) *pkt.ErrorMessage {
	enc, err := codec.NewMultiFileEncoder(path)
	if err != nil {
		logrus.Errorf("[NewMultiFileEncoder]ERR:%s\n", err)
		return pkt.NewErrorMsg(pkt.CODEC_ERROR, err.Error())
	}
	uploadobject.Encoder = enc
	defer enc.Close()
	return uploadobject.Upload()
}

func (uploadobject *UploadObject) UploadFile(path string) *pkt.ErrorMessage {
	enc, err := codec.NewFileEncoder(path)
	if err != nil {
		logrus.Errorf("[NewFileEncoder]Path:%s,ERR:%s\n", path, err)
		return pkt.NewErrorMsg(pkt.CODEC_ERROR, err.Error())
	}
	uploadobject.Encoder = enc
	defer enc.Close()
	return uploadobject.Upload()
}

func (uploadobject *UploadObject) UploadBytes(data []byte) *pkt.ErrorMessage {
	enc, err := codec.NewBytesEncoder(data)
	if err != nil {
		logrus.Errorf("[NewBytesEncoder]ERR:%s\n", err)
		return pkt.NewErrorMsg(pkt.CODEC_ERROR, err.Error())
	}
	uploadobject.Encoder = enc
	defer enc.Close()
	return uploadobject.Upload()
}

func (uploadobject *UploadObject) IdExist(id uint32) bool {
	if uploadobject.Blocks == nil {
		return false
	}
	for _, ii := range uploadobject.Blocks {
		if ii == id {
			return true
		}
	}
	return false
}

func (uploadobject *UploadObject) GetProgress() int32 {
	return uploadobject.PRO.GetProgress()
}

var RunningMap sync.Map

func (uploadobject *UploadObject) Upload() (reserr *pkt.ErrorMessage) {
	key := hex.EncodeToString(uploadobject.GetMD5())
	if obj, has := RunningMap.Load(key); has {
		up := obj.(*UploadObject)
		logrus.Infof("[UploadObject][%s]Uploading...\n", up.VNU.Hex())
		return pkt.NewErrorMsg(pkt.REPEAT_REQ, fmt.Sprintf("Progress:%d", up.PRO.GetProgress()))
	}
	RunningMap.Store(key, uploadobject)
	defer func() {
		RunningMap.Delete(key)
		if r := recover(); r != nil {
			env.TraceError("[UploadObject]")
			reserr = pkt.NewErrorMsg(pkt.SERVER_ERROR, "Unknown error")
			uploadobject.ERR.Store(reserr)
		}
	}()
	uploadobject.PRO.Length.Set(uploadobject.Encoder.GetLength())
	err := uploadobject.initUpload(uploadobject.Encoder.GetVHW(), uploadobject.Encoder.GetLength())
	if err != nil {
		uploadobject.ERR.Store(err)
		return err
	}
	logrus.Infof("[UploadObject][%s]Start upload object...\n", uploadobject.VNU.Hex())
	if uploadobject.Exist {
		uploadobject.PRO.ReadinLength.Set(uploadobject.Encoder.GetLength())
		uploadobject.PRO.ReadOutLength.Set(uploadobject.Encoder.GetLength())
		uploadobject.PRO.WriteLength.Set(uploadobject.Encoder.GetLength())
		logrus.Infof("[UploadObject][%s]Already exists.\n", uploadobject.VNU.Hex())
	} else {
		wgroup := sync.WaitGroup{}
		var id uint32 = 0
		for {
			b, err := uploadobject.Encoder.ReadNext()
			if err != nil {
				return pkt.NewErrorMsg(pkt.CODEC_ERROR, err.Error())
			}
			if b == nil {
				break
			}
			if uploadobject.ERR.Load() != nil {
				break
			}
			uploadobject.PRO.ReadinLength.Set(uploadobject.Encoder.GetReadinTotal())
			uploadobject.PRO.ReadOutLength.Set(uploadobject.Encoder.GetReadoutTotal())
			if uploadobject.IdExist(id) {
				uploadobject.PRO.WriteLength.Add(b.Length())
				logrus.Infof("[UploadObject][%s][%d]Block has been uploaded.\n", uploadobject.VNU.Hex(), id)
			} else {
				wgroup.Add(1)
				StartUploadBlock(int16(id), b, uploadobject, &wgroup)
			}
			id++
		}
		wgroup.Wait()
		var errmsg *pkt.ErrorMessage
		v := uploadobject.ERR.Load()
		if v != nil {
			errmsg = v.(*pkt.ErrorMessage)
		} else {
			errmsg = uploadobject.complete(uploadobject.Encoder.GetVHW())
		}
		if errmsg != nil {
			logrus.Errorf("[UploadObject][%s]Upload ERR:%s\n", uploadobject.VNU.Hex(), pkt.ToError(errmsg))
			return errmsg
		} else {
			logrus.Infof("[UploadObject][%s]Upload object OK.\n", uploadobject.VNU.Hex())
		}
	}
	return nil
}

func (uploadobject *UploadObject) complete(sha []byte) *pkt.ErrorMessage {
	i1, i2, i3, i4 := pkt.ObjectIdParam(uploadobject.VNU)
	vnu := &pkt.UploadObjectEndReqV2_VNU{Timestamp: i1, MachineIdentifier: i2, ProcessIdentifier: i3, Counter: i4}
	req := &pkt.UploadObjectEndReqV2{
		UserId:    &uploadobject.UClient.UserId,
		SignData:  &uploadobject.UClient.SignKey.Sign,
		KeyNumber: &uploadobject.UClient.SignKey.KeyNumber,
		VHW:       sha,
		Vnu:       vnu,
	}
	_, errmsg := net.RequestSN(req)
	if errmsg != nil && errmsg.Code != pkt.INVALID_UPLOAD_ID {
		return errmsg
	}
	return nil
}

func (uploadobject *UploadObject) initUpload(sha []byte, length int64) *pkt.ErrorMessage {
	size := uint64(length)
	req := &pkt.UploadObjectInitReqV2{
		UserId:    &uploadobject.UClient.UserId,
		SignData:  &uploadobject.UClient.SignKey.Sign,
		KeyNumber: &uploadobject.UClient.SignKey.KeyNumber,
		VHW:       sha,
		Length:    &size,
	}
	var initresp *pkt.UploadObjectInitResp
	resp, errmsg := net.RequestSN(req)
	if errmsg != nil {
		logrus.Errorf("[UploadObject][%s]Init ERR:%s\n", base58.Encode(sha), pkt.ToError(errmsg))
		return errmsg
	} else {
		res, OK := resp.(*pkt.UploadObjectInitResp)
		if !OK {
			logrus.Errorf("[UploadObject][%s]Init ERR:RETURN_ERR_MSG\n", base58.Encode(sha))
			return pkt.NewErrorMsg(pkt.INVALID_ARGS, "Return err msg type")
		}
		initresp = res
	}
	if initresp.Vnu == nil || initresp.Vnu.Timestamp == nil || initresp.Vnu.MachineIdentifier == nil || initresp.Vnu.ProcessIdentifier == nil || initresp.Vnu.Counter == nil {
		return pkt.NewErrorMsg(pkt.INVALID_ARGS, "VNU Return Nil")
	}
	uploadobject.VNU = pkt.NewObjectId(*initresp.Vnu.Timestamp, *initresp.Vnu.MachineIdentifier, *initresp.Vnu.ProcessIdentifier, *initresp.Vnu.Counter)
	if initresp.SignArg != nil {
		uploadobject.Sign = *initresp.SignArg
	}
	if initresp.Stamp != nil {
		uploadobject.Stamp = int64(*initresp.Stamp)
	}
	if initresp.Blocks != nil {
		uploadobject.Blocks = initresp.Blocks.Blocks
	}
	if initresp.Repeat != nil {
		uploadobject.Exist = *initresp.Repeat
	}
	return nil
}
