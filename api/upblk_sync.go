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
	ub := UploadBlock{
		UPOBJ: up,
		ID:    id,
		WG:    wg,
	}
	syncup := &UploadBlockSync{}
	syncup.EncBLK = b
	syncup.UploadBlock = ub
	syncup.logPrefix = fmt.Sprintf("[%s][%d]", ub.UPOBJ.VNU.Hex(), ub.ID)
	<-BLOCK_MAKE_CH
	go syncup.upload()
}

func (uploadBlock *UploadBlockSync) DoFinish() {
	if r := recover(); r != nil {
		env.TraceError("[SyncBlock]")
		uploadBlock.UPOBJ.ERR.Store(pkt.NewErrorMsg(pkt.SERVER_ERROR, "Unknown error"))
		uploadBlock.WG.Done()
	}
	BLOCK_MAKE_CH <- 1
}

func (uploadBlock *UploadBlockSync) upload() {
	defer uploadBlock.DoFinish()
	uploadBlock.SN = net.GetBlockSuperNode(uploadBlock.EncBLK.VHP)
	logrus.Infof("[SyncBlock]%sStart upload block to sn %d\n", uploadBlock.logPrefix, uploadBlock.SN.ID)
	if uploadBlock.EncBLK.IsDup {
		uploadBlock.uploadDup()
	} else {
		eblk := &codec.EncryptedBlock{}
		eblk.Data = uploadBlock.EncBLK.DATA
		eblk.MakeVHB()
		if uploadBlock.EncBLK.Length() < env.PL2 {
			uploadBlock.uploadDB(eblk)
		} else {
			uploadBlock.STime = time.Now().Unix()
			uploadBlock.uploadDedup(eblk)
		}
	}
}

func (uploadBlock *UploadBlockSync) uploadDB(b *codec.EncryptedBlock) {
	startTime := time.Now()
	bid := uint32(uploadBlock.ID)
	osize := uint64(uploadBlock.EncBLK.OriginalSize)
	i1, i2, i3, i4 := pkt.ObjectIdParam(uploadBlock.UPOBJ.VNU)
	vnu := &pkt.UploadBlockDBReqV2_VNU{Timestamp: i1, MachineIdentifier: i2, ProcessIdentifier: i3, Counter: i4}
	req := &pkt.UploadBlockDBReqV2{
		UserId:       &uploadBlock.UPOBJ.UClient.UserId,
		SignData:     &uploadBlock.UPOBJ.UClient.SignKey.Sign,
		KeyNumber:    &uploadBlock.UPOBJ.UClient.SignKey.KeyNumber,
		Id:           &bid,
		Vnu:          vnu,
		VHP:          uploadBlock.EncBLK.VHP,
		VHB:          b.VHB,
		KEU:          uploadBlock.EncBLK.KEU,
		KED:          uploadBlock.EncBLK.KED,
		OriginalSize: &osize,
		Data:         uploadBlock.EncBLK.DATA,
	}
	if uploadBlock.UPOBJ.UClient.StoreKey != uploadBlock.UPOBJ.UClient.SignKey {
		sign, _ := SetStoreNumber(uploadBlock.UPOBJ.UClient.SignKey.Sign, int32(uploadBlock.UPOBJ.UClient.StoreKey.KeyNumber))
		req.SignData = &sign
	}
	_, errmsg := net.RequestSN(req, uploadBlock.SN, uploadBlock.logPrefix, env.SN_RETRYTIMES, false)
	if errmsg == nil {
		logrus.Infof("[SyncBlock]%sUpload block to DB,VHP:%s,take times %d ms.\n", uploadBlock.logPrefix,
			base58.Encode(uploadBlock.EncBLK.VHP), time.Since(startTime).Milliseconds())
	} else {
		uploadBlock.UPOBJ.ERR.Store(errmsg)
	}
}

func (uploadBlock *UploadBlockSync) uploadDup() {
	startTime := time.Now()
	i1, i2, i3, i4 := pkt.ObjectIdParam(uploadBlock.UPOBJ.VNU)
	v := &pkt.UploadBlockDupReqV2_VNU{Timestamp: i1, MachineIdentifier: i2, ProcessIdentifier: i3, Counter: i4}
	dupReq := &pkt.UploadBlockDupReqV2{
		UserId:    &uploadBlock.UPOBJ.UClient.UserId,
		SignData:  &uploadBlock.UPOBJ.UClient.SignKey.Sign,
		KeyNumber: &uploadBlock.UPOBJ.UClient.SignKey.KeyNumber,
		VHB:       uploadBlock.EncBLK.VHB,
		KEU:       uploadBlock.EncBLK.KEU,
	}
	if uploadBlock.UPOBJ.UClient.StoreKey != uploadBlock.UPOBJ.UClient.SignKey {
		sign, _ := SetStoreNumber(uploadBlock.UPOBJ.UClient.SignKey.Sign, int32(uploadBlock.UPOBJ.UClient.StoreKey.KeyNumber))
		dupReq.SignData = &sign
	}
	bid := uint32(uploadBlock.ID)
	osize := uint64(uploadBlock.EncBLK.OriginalSize)
	rsize := uint32(uploadBlock.EncBLK.RealSize)
	dupReq.Id = &bid
	dupReq.VHP = uploadBlock.EncBLK.VHP
	dupReq.OriginalSize = &osize
	dupReq.RealSize = &rsize
	dupReq.Vnu = v
	_, errmsg := net.RequestSN(dupReq, uploadBlock.SN, uploadBlock.logPrefix, env.SN_RETRYTIMES, false)
	if errmsg == nil {
		logrus.Infof("[SyncBlock]%sBlock is a repetitive block %s,take times %d ms.\n", uploadBlock.logPrefix,
			base58.Encode(uploadBlock.EncBLK.VHP), time.Since(startTime).Milliseconds())
	} else {
		uploadBlock.UPOBJ.ERR.Store(errmsg)
	}
}

func (uploadBlock *UploadBlockSync) uploadDedup(eblk *codec.EncryptedBlock) {
	enc := codec.NewErasureEncoder(eblk)
	err := enc.Encode()
	if err != nil {
		logrus.Errorf("[SyncBlock]ErasureEncoder ERR:%s\n", uploadBlock.logPrefix, err)
		uploadBlock.UPOBJ.ERR.Store(pkt.NewErrorMsg(pkt.INVALID_ARGS, err.Error()))
		return
	}
	blksize := uploadBlock.EncBLK.Length()
	uploadBlock.EncBLK.DATA = nil
	eblk.Clear()
	uploadBlock.Queue = NewDNQueue()
	retrytimes := 0
	size := len(enc.Shards)
	rsize := int32(uploadBlock.EncBLK.RealSize)
	ress := make([]*UploadShardResult, size)
	var ress2 []*UploadShardResult = nil
	if !enc.IsCopyShard() && env.LRC2 {
		ress2 = make([]*UploadShardResult, size)
	}
	<-BLOCK_ROUTINE_CH
	startedSign := make(chan int, 1)
	go func() {
		defer func() {
			if r := recover(); r != nil {
				env.TraceError("[UploadBlock]")
				uploadBlock.UPOBJ.ERR.Store(pkt.NewErrorMsg(pkt.SERVER_ERROR, "Unknown error"))
			}
			BLOCK_ROUTINE_CH <- 1
			uploadBlock.WG.Done()
			uploadBlock.UPOBJ.ActiveTime.Set(time.Now().Unix())
			uploadBlock.UPOBJ.PRO.WriteLength.Add(blksize)
		}()
		var ids []int32
		for {
			blkls, err := uploadBlock.UploadShards(uploadBlock.EncBLK.VHP, uploadBlock.EncBLK.KEU, uploadBlock.EncBLK.KED, eblk.VHB, enc, &rsize, uploadBlock.EncBLK.OriginalSize, ress, ress2, ids, startedSign)
			if err != nil {
				if err.Code == pkt.DN_IN_BLACKLIST {
					ids = blkls
					logrus.Errorf("[UploadBlock]%sWrite shardmetas ERR:DN_IN_BLACKLIST,RetryTimes %d\n", uploadBlock.logPrefix, retrytimes)
					retrytimes++
					if env.ThrowErr {
						uploadBlock.UPOBJ.ERR.Store(err)
						break
					}
					continue
				}
				if err.Code == pkt.SERVER_ERROR || err.Msg == "Panic" {
					time.Sleep(time.Duration(60) * time.Second)
					continue
				}
				uploadBlock.UPOBJ.ERR.Store(err)
			}
			break
		}
	}()
	<-startedSign
}
