package api

import (
	"fmt"
	"sync"
	"time"

	"github.com/aurawing/eos-go/btcsuite/btcutil/base58"
	"github.com/sirupsen/logrus"
	"github.com/yottachain/YTCoreService/codec"
	"github.com/yottachain/YTCoreService/env"
	"github.com/yottachain/YTCoreService/net"
	"github.com/yottachain/YTCoreService/pkt"
)

type UploadBlockSync struct {
	UploadBlock
	EncBLK *codec.EncodedBlock
}

func StartUploadBlockSync(id int16, b *codec.EncodedBlock, up *UploadObject, wg *sync.WaitGroup) {
	AddSyncBlockMen(b)
	ub := UploadBlock{
		UPOBJ: up,
		ID:    id,
		WG:    wg,
	}
	syncup := &UploadBlockSync{}
	syncup.EncBLK = b
	syncup.UploadBlock = ub
	syncup.logPrefix = fmt.Sprintf("[%s][%d]", ub.UPOBJ.VNU.Hex(), ub.ID)
	<-BLOCK_ROUTINE_CH
	go syncup.upload()
}

func (self *UploadBlockSync) DoFinish() {
	if r := recover(); r != nil {
		env.TraceError("[SyncBlock]")
		self.UPOBJ.ERR.Store(pkt.NewErrorMsg(pkt.SERVER_ERROR, "Unknown error"))
	}
	BLOCK_ROUTINE_CH <- 1
	self.WG.Done()
	self.UPOBJ.ActiveTime.Set(time.Now().Unix())
	DecSyncBlockMen(self.EncBLK)
	self.UPOBJ.PRO.WriteLength.Add(self.EncBLK.Length())
}

func (self *UploadBlockSync) upload() {
	defer self.DoFinish()
	self.SN = net.GetBlockSuperNode(self.EncBLK.VHP)
	logrus.Infof("[SyncBlock]%sStart upload block to sn %d\n", self.logPrefix, self.SN.ID)
	if self.EncBLK.IsDup {
		self.uploadDup()
	} else {
		eblk := &codec.EncryptedBlock{}
		eblk.Data = self.EncBLK.DATA
		eblk.MakeVHB()
		if self.EncBLK.Length() < env.PL2 {
			self.uploadDB(eblk)
		} else {
			self.STime = time.Now().Unix()
			self.uploadDedup(eblk)
		}
	}
}

func (self *UploadBlockSync) uploadDB(b *codec.EncryptedBlock) {
	startTime := time.Now()
	bid := uint32(self.ID)
	osize := uint64(self.EncBLK.OriginalSize)
	i1, i2, i3, i4 := pkt.ObjectIdParam(self.UPOBJ.VNU)
	vnu := &pkt.UploadBlockDBReqV2_VNU{Timestamp: i1, MachineIdentifier: i2, ProcessIdentifier: i3, Counter: i4}
	req := &pkt.UploadBlockDBReqV2{
		UserId:       &self.UPOBJ.UClient.UserId,
		SignData:     &self.UPOBJ.UClient.SignKey.Sign,
		KeyNumber:    &self.UPOBJ.UClient.SignKey.KeyNumber,
		Id:           &bid,
		Vnu:          vnu,
		VHP:          self.EncBLK.VHP,
		VHB:          b.VHB,
		KEU:          self.EncBLK.KEU,
		KED:          self.EncBLK.KED,
		OriginalSize: &osize,
		Data:         self.EncBLK.DATA,
	}
	if self.UPOBJ.UClient.StoreKey != self.UPOBJ.UClient.SignKey {
		sign, _ := SetStoreNumber(self.UPOBJ.UClient.SignKey.Sign, int32(self.UPOBJ.UClient.StoreKey.KeyNumber))
		req.SignData = &sign
	}
	_, errmsg := net.RequestSN(req, self.SN, self.logPrefix, env.SN_RETRYTIMES, false)
	if errmsg == nil {
		logrus.Infof("[SyncBlock]%sUpload block to DB,VHP:%s,take times %d ms.\n", self.logPrefix,
			base58.Encode(self.EncBLK.VHP), time.Now().Sub(startTime).Milliseconds())
	} else {
		self.UPOBJ.ERR.Store(errmsg)
	}
}

func (self *UploadBlockSync) uploadDup() {
	startTime := time.Now()
	i1, i2, i3, i4 := pkt.ObjectIdParam(self.UPOBJ.VNU)
	v := &pkt.UploadBlockDupReqV2_VNU{Timestamp: i1, MachineIdentifier: i2, ProcessIdentifier: i3, Counter: i4}
	dupReq := &pkt.UploadBlockDupReqV2{
		UserId:    &self.UPOBJ.UClient.UserId,
		SignData:  &self.UPOBJ.UClient.SignKey.Sign,
		KeyNumber: &self.UPOBJ.UClient.SignKey.KeyNumber,
		VHB:       self.EncBLK.VHB,
		KEU:       self.EncBLK.KEU,
	}
	if self.UPOBJ.UClient.StoreKey != self.UPOBJ.UClient.SignKey {
		sign, _ := SetStoreNumber(self.UPOBJ.UClient.SignKey.Sign, int32(self.UPOBJ.UClient.StoreKey.KeyNumber))
		dupReq.SignData = &sign
	}
	bid := uint32(self.ID)
	osize := uint64(self.EncBLK.OriginalSize)
	rsize := uint32(self.EncBLK.RealSize)
	dupReq.Id = &bid
	dupReq.VHP = self.EncBLK.VHP
	dupReq.OriginalSize = &osize
	dupReq.RealSize = &rsize
	dupReq.Vnu = v
	_, errmsg := net.RequestSN(dupReq, self.SN, self.logPrefix, env.SN_RETRYTIMES, false)
	if errmsg == nil {
		logrus.Infof("[SyncBlock]%sBlock is a repetitive block %s,take times %d ms.\n", self.logPrefix,
			base58.Encode(self.EncBLK.VHP), time.Now().Sub(startTime).Milliseconds())
	} else {
		self.UPOBJ.ERR.Store(errmsg)
	}
}

func (self *UploadBlockSync) uploadDedup(eblk *codec.EncryptedBlock) {
	enc := codec.NewErasureEncoder(eblk)
	err := enc.Encode()
	if err != nil {
		logrus.Errorf("[SyncBlock]ErasureEncoder ERR:%s\n", self.logPrefix, err)
		self.UPOBJ.ERR.Store(pkt.NewErrorMsg(pkt.INVALID_ARGS, err.Error()))
		return
	}
	DecSyncBlockMen(self.EncBLK)
	self.EncBLK.DATA = nil
	eblk.Clear()
	length := AddEncoderMem(enc)
	defer DecMen(length)
	self.Queue = NewDNQueue()
	retrytimes := 0
	size := len(enc.Shards)
	rsize := int32(self.EncBLK.RealSize)
	ress := make([]*UploadShardResult, size)
	var ress2 []*UploadShardResult = nil
	if env.LRC2 && !enc.IsCopyShard() {
		ress2 = make([]*UploadShardResult, size)
	}
	var ids []int32
	for {
		blkls, err := self.UploadShards(self.EncBLK.VHP, self.EncBLK.KEU, self.EncBLK.KED, eblk.VHB, enc, &rsize, self.EncBLK.OriginalSize, ress, ress2, ids)
		if err != nil {
			if err.Code == pkt.DN_IN_BLACKLIST {
				ids = blkls
				logrus.Errorf("[SyncBlock]%sWrite shardmetas ERR:DN_IN_BLACKLIST,RetryTimes %d\n", self.logPrefix, retrytimes)
				NotifyAllocNode(true)
				retrytimes++
				continue
			}
			if err.Code == pkt.SERVER_ERROR || err.Msg == "Panic" {
				time.Sleep(time.Duration(60) * time.Second)
				continue
			}
			self.UPOBJ.ERR.Store(err)
		}
		break
	}
}
