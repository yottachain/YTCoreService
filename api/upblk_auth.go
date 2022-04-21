package api

import (
	"fmt"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/yottachain/YTCoreService/env"
	"github.com/yottachain/YTCoreService/net"
	"github.com/yottachain/YTCoreService/pkt"
)

type UploadBlockAuth struct {
	UploadBlock
	REF *pkt.Refer
}

func (self *UploadBlockAuth) DoFinish() {
	if r := recover(); r != nil {
		env.TraceError("[AuthBlock]")
		self.UPOBJ.ERR.Store(pkt.NewErrorMsg(pkt.SERVER_ERROR, "Unknown error"))
	}
	BLOCK_ROUTINE_CH <- 1
	self.WG.Done()
	self.UPOBJ.ActiveTime.Set(time.Now().Unix())
}

func StartUploadBlockAuth(b *pkt.Refer, up *UploadObject, wg *sync.WaitGroup) {
	ub := UploadBlock{
		UPOBJ: up,
		ID:    b.Id,
		WG:    wg,
	}
	authup := &UploadBlockAuth{}
	authup.REF = b
	authup.UploadBlock = ub
	authup.logPrefix = fmt.Sprintf("[%s][%d]", ub.UPOBJ.VNU.Hex(), ub.ID)
	<-BLOCK_ROUTINE_CH
	go authup.upload()
}

func (self *UploadBlockAuth) upload() {
	defer self.DoFinish()
	self.SN = net.GetSuperNode(int(self.REF.SuperID))
	logrus.Infof("[AuthBlock]%sStart upload block to sn %d\n", self.logPrefix, self.SN.ID)
	startTime := time.Now()
	i1, i2, i3, i4 := pkt.ObjectIdParam(self.UPOBJ.VNU)
	vnu := &pkt.UploadBlockAuthReq_VNU{Timestamp: i1, MachineIdentifier: i2, ProcessIdentifier: i3, Counter: i4}
	req := &pkt.UploadBlockAuthReq{
		UserId:    &self.UPOBJ.UClient.UserId,
		SignData:  &self.UPOBJ.UClient.SignKey.Sign,
		KeyNumber: &self.UPOBJ.UClient.SignKey.KeyNumber,
		Vnu:       vnu,
		Refer:     self.REF.Bytes(),
	}
	_, errmsg := net.RequestSN(req, self.SN, self.logPrefix, env.SN_RETRYTIMES, false)
	if errmsg == nil {
		logrus.Infof("[AuthBlock]%sUpload block,VBI:%d,take times %d ms.\n", self.logPrefix,
			self.REF.VBI, time.Now().Sub(startTime).Milliseconds())
	} else {
		self.UPOBJ.ERR.Store(errmsg)
	}
}
