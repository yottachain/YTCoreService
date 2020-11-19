package api

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/mr-tron/base58/base58"
	"github.com/sirupsen/logrus"
	"github.com/yottachain/YTCoreService/codec"
	"github.com/yottachain/YTCoreService/env"
	"github.com/yottachain/YTCoreService/net"
	"github.com/yottachain/YTCoreService/pkt"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

type UploadObject struct {
	UClient    *Client
	Encoder    *codec.FileEncoder
	VNU        primitive.ObjectID
	Sign       string
	Stamp      int64
	Blocks     []uint32
	Exist      bool
	ActiveTime *int64
	ERR        atomic.Value
	activesign chan int
	PRO        *UpProgress
}

type UpProgress struct {
	Length        *int64
	ReadinLength  *int64
	ReadOutLength *int64
	WriteLength   *int64
}

func (self *UpProgress) GetProgress() int32 {
	l1 := atomic.LoadInt64(self.Length)
	l2 := atomic.LoadInt64(self.ReadinLength)
	l3 := atomic.LoadInt64(self.ReadOutLength)
	l4 := atomic.LoadInt64(self.WriteLength)
	if l1 == 0 || l3 == 0 {
		return 0
	}
	p1 := l2 * 100 / l1
	p2 := l4 * 100 / l3
	return int32(p1 * p2 / 100)
}

func NewUploadObject(c *Client) *UploadObject {
	p := &UpProgress{Length: new(int64), ReadinLength: new(int64), ReadOutLength: new(int64), WriteLength: new(int64)}
	o := &UploadObject{UClient: c, ActiveTime: new(int64), activesign: make(chan int), PRO: p}
	return o
}

func (self *UploadObject) GetLength() int64 {
	if self.Encoder != nil {
		return self.Encoder.GetLength()
	}
	return 0
}

func (self *UploadObject) GetSHA256() []byte {
	if self.Encoder != nil {
		return self.Encoder.GetVHW()
	}
	return nil
}

func (self *UploadObject) GetMD5() []byte {
	if self.Encoder != nil {
		return self.Encoder.GetMD5()
	}
	return nil
}

func (self *UploadObject) UploadMultiFile(path []string) *pkt.ErrorMessage {
	enc, err := codec.NewMultiFileEncoder(path)
	if err != nil {
		logrus.Errorf("[NewMultiFileEncoder]ERR:%s\n", err)
		return pkt.NewErrorMsg(pkt.INVALID_ARGS, err.Error())
	}
	self.Encoder = enc
	defer enc.Close()
	return self.Upload()
}

func (self *UploadObject) UploadFile(path string) *pkt.ErrorMessage {
	enc, err := codec.NewFileEncoder(path)
	if err != nil {
		logrus.Errorf("[NewFileEncoder]Path:%s,ERR:%s\n", path, err)
		return pkt.NewErrorMsg(pkt.INVALID_ARGS, err.Error())
	}
	self.Encoder = enc
	defer enc.Close()
	return self.Upload()
}

func (self *UploadObject) UploadBytes(data []byte) *pkt.ErrorMessage {
	enc, err := codec.NewBytesEncoder(data)
	if err != nil {
		logrus.Errorf("[NewBytesEncoder]ERR:%s\n", err)
		return pkt.NewErrorMsg(pkt.INVALID_ARGS, err.Error())
	}
	self.Encoder = enc
	defer enc.Close()
	return self.Upload()
}

func (self *UploadObject) IdExist(id uint32) bool {
	if self.Blocks == nil {
		return false
	}
	for _, ii := range self.Blocks {
		if ii == id {
			return true
		}
	}
	return false
}

func (self *UploadObject) GetProgress() int32 {
	return self.PRO.GetProgress()
}

func (self *UploadObject) Upload() (reserr *pkt.ErrorMessage) {
	defer func() {
		if r := recover(); r != nil {
			env.TraceError("[UploadObject]")
			self.ERR.Store(pkt.NewErrorMsg(pkt.SERVER_ERROR, "Unknown error"))
			reserr = pkt.NewErrorMsg(pkt.SERVER_ERROR, "Unknown error")
		}
	}()
	atomic.StoreInt64(self.PRO.Length, self.Encoder.GetLength())
	err := self.initUpload()
	if err != nil {
		return err
	}
	if self.UClient == nil {
		return pkt.NewErrorMsg(pkt.SERVER_ERROR, "Unknown error")
	}
	logrus.Infof("[UploadObject][%s]Start upload object...\n", self.VNU.Hex())
	if self.Exist {
		atomic.StoreInt64(self.PRO.ReadinLength, self.Encoder.GetLength())
		atomic.StoreInt64(self.PRO.ReadOutLength, self.Encoder.GetLength())
		atomic.StoreInt64(self.PRO.WriteLength, self.Encoder.GetLength())
		logrus.Infof("[UploadObject][%s]Already exists.\n", self.VNU.Hex())
	} else {
		wgroup := sync.WaitGroup{}
		atomic.StoreInt64(self.ActiveTime, time.Now().Unix())
		go self.waitcheck()
		var id uint32 = 0
		for {
			b, err := self.Encoder.ReadNext()
			if err != nil {
				return pkt.NewErrorMsg(pkt.INVALID_ARGS, err.Error())
			}
			if b == nil {
				break
			}
			if self.ERR.Load() != nil {
				break
			}
			atomic.StoreInt64(self.PRO.ReadinLength, self.Encoder.GetReadinTotal())
			atomic.StoreInt64(self.PRO.ReadOutLength, self.Encoder.GetReadoutTotal())
			if self.IdExist(id) {
				atomic.AddInt64(self.PRO.WriteLength, b.Length())
				logrus.Infof("[UploadObject][%s][%d]Block has been uploaded.\n", self.VNU.Hex(), id)
			} else {
				wgroup.Add(1)
				StartUploadBlock(int16(id), b, self, &wgroup)
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
			errmsg = self.complete()
		}
		if errmsg != nil {
			logrus.Errorf("[UploadObject][%s]Upload ERR:%s\n", self.VNU.Hex(), pkt.ToError(errmsg))
			return errmsg
		} else {
			logrus.Infof("[UploadObject][%s]Upload object OK.\n", self.VNU.Hex())
		}
	}
	return nil
}

func (self *UploadObject) waitcheck() {
	for {
		timeout := time.After(time.Second * 30)
		select {
		case self.activesign <- 1:
			close(self.activesign)
			return
		case <-timeout:
			if self.ERR.Load() != nil {
				break
			}
			self.active()
		}
	}
}

func (self *UploadObject) active() {
	lt := atomic.LoadInt64(self.ActiveTime)
	if time.Now().Unix()-lt > 60 {
		i1, i2, i3, i4 := pkt.ObjectIdParam(self.VNU)
		vnu := &pkt.ActiveCacheV2_VNU{Timestamp: i1, MachineIdentifier: i2, ProcessIdentifier: i3, Counter: i4}
		req := &pkt.ActiveCacheV2{
			UserId:    &self.UClient.UserId,
			SignData:  &self.UClient.Sign,
			KeyNumber: &self.UClient.KeyNumber,
			Vnu:       vnu,
		}
		_, err := net.RequestSN(req, self.UClient.SuperNode, self.VNU.Hex(), env.SN_RETRYTIMES, false)
		if err == nil {
			atomic.StoreInt64(self.ActiveTime, time.Now().Unix())
		}
	}
}

func (self *UploadObject) complete() *pkt.ErrorMessage {
	i1, i2, i3, i4 := pkt.ObjectIdParam(self.VNU)
	vnu := &pkt.UploadObjectEndReqV2_VNU{Timestamp: i1, MachineIdentifier: i2, ProcessIdentifier: i3, Counter: i4}
	req := &pkt.UploadObjectEndReqV2{
		UserId:    &self.UClient.UserId,
		SignData:  &self.UClient.Sign,
		KeyNumber: &self.UClient.KeyNumber,
		VHW:       self.Encoder.GetVHW(),
		Vnu:       vnu,
	}
	_, errmsg := net.RequestSN(req, self.UClient.SuperNode, self.VNU.Hex(), env.SN_RETRYTIMES, false)
	if errmsg != nil && errmsg.Code != pkt.INVALID_UPLOAD_ID {
		return errmsg
	}
	return nil
}

func (self *UploadObject) initUpload() *pkt.ErrorMessage {
	size := uint64(self.Encoder.GetLength())
	req := &pkt.UploadObjectInitReqV2{
		UserId:    &self.UClient.UserId,
		SignData:  &self.UClient.Sign,
		KeyNumber: &self.UClient.KeyNumber,
		VHW:       self.Encoder.GetVHW(),
		Length:    &size,
	}
	var initresp *pkt.UploadObjectInitResp
	resp, errmsg := net.RequestSN(req, self.UClient.SuperNode, "", env.SN_RETRYTIMES, false)
	if errmsg != nil {
		logrus.Errorf("[UploadObject][%s]Init ERR:%s\n", base58.Encode(self.Encoder.GetVHW()), pkt.ToError(errmsg))
		return errmsg
	} else {
		res, OK := resp.(*pkt.UploadObjectInitResp)
		if !OK {
			logrus.Errorf("[UploadObject][%s]Init ERR:RETURN_ERR_MSG\n", base58.Encode(self.Encoder.GetVHW()))
			return pkt.NewErrorMsg(pkt.INVALID_ARGS, "Return err msg type")
		}
		initresp = res
	}
	if initresp.Vnu == nil || initresp.Vnu.Timestamp == nil || initresp.Vnu.MachineIdentifier == nil || initresp.Vnu.ProcessIdentifier == nil || initresp.Vnu.Counter == nil {
		return pkt.NewErrorMsg(pkt.INVALID_ARGS, "VNU Return Nil")
	}
	self.VNU = pkt.NewObjectId(*initresp.Vnu.Timestamp, *initresp.Vnu.MachineIdentifier, *initresp.Vnu.ProcessIdentifier, *initresp.Vnu.Counter)
	if initresp.SignArg != nil {
		self.Sign = *initresp.SignArg
	}
	if initresp.Stamp != nil {
		self.Stamp = int64(*initresp.Stamp)
	}
	if initresp.Blocks != nil {
		self.Blocks = initresp.Blocks.Blocks
	}
	if initresp.Repeat != nil {
		self.Exist = *initresp.Repeat
	}
	return nil
}
