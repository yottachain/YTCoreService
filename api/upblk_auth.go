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

func (auth *UploadBlockAuth) DoFinish() {
	if r := recover(); r != nil {
		env.TraceError("[AuthBlock]")
		auth.UPOBJ.ERR.Store(pkt.NewErrorMsg(pkt.SERVER_ERROR, "Unknown error"))
	}
	BLOCK_MAKE_CH <- 1
	auth.WG.Done()
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
	<-BLOCK_MAKE_CH
	go authup.upload()
}

func (auth *UploadBlockAuth) upload() {
	defer auth.DoFinish()
	logrus.Infof("[AuthBlock]%sStart upload block\n", auth.logPrefix)
	startTime := time.Now()
	i1, i2, i3, i4 := pkt.ObjectIdParam(auth.UPOBJ.VNU)
	vnu := &pkt.UploadBlockAuthReq_VNU{Timestamp: i1, MachineIdentifier: i2, ProcessIdentifier: i3, Counter: i4}
	req := &pkt.UploadBlockAuthReq{
		UserId:    &auth.UPOBJ.UClient.UserId,
		SignData:  &auth.UPOBJ.UClient.SignKey.Sign,
		KeyNumber: &auth.UPOBJ.UClient.SignKey.KeyNumber,
		Vnu:       vnu,
		Refer:     auth.REF.Bytes(),
	}
	_, errmsg := net.RequestSN(req)
	if errmsg == nil {
		logrus.Infof("[AuthBlock]%sUpload block,VBI:%d,take times %d ms.\n", auth.logPrefix,
			auth.REF.VBI, time.Since(startTime).Milliseconds())
	} else {
		auth.UPOBJ.ERR.Store(errmsg)
	}
}
