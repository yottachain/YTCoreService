package api

import (
	"bytes"
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

var BLOCK_MAKE_CH chan int

func InitBlockRoutinePool() {
	BLOCK_MAKE_CH = make(chan int, env.MakeBlockThreadNum)
	for ii := 0; ii < env.MakeBlockThreadNum; ii++ {
		BLOCK_MAKE_CH <- 1
	}
	BLOCK_ROUTINE_CH = make(chan int, env.UploadBlockThreadNum)
	for ii := 0; ii < env.UploadBlockThreadNum; ii++ {
		BLOCK_ROUTINE_CH <- 1
	}
}

func StartUploadBlock(id int16, b *codec.PlainBlock, up *UploadObject, wg *sync.WaitGroup) {
	ub := &UploadBlock{
		UPOBJ:  up,
		ID:     id,
		BLK:    b,
		WG:     wg,
		Length: b.Length(),
	}
	ub.logPrefix = fmt.Sprintf("[%s][%d]", ub.UPOBJ.VNU.Hex(), ub.ID)
	<-BLOCK_MAKE_CH
	go ub.upload()
}

type UploadBlock struct {
	ID        int16
	BLK       *codec.PlainBlock
	UPOBJ     *UploadObject
	Queue     *DNQueue
	logPrefix string
	WG        *sync.WaitGroup
	STime     int64
	Length    int64
}

func (uploadBlock *UploadBlock) DoFinish() {
	if r := recover(); r != nil {
		env.TraceError("[UploadBlock]")
		uploadBlock.UPOBJ.ERR.Store(pkt.NewErrorMsg(pkt.SERVER_ERROR, "Unknown error"))
	}
	BLOCK_MAKE_CH <- 1
	if uploadBlock.WG != nil {
		uploadBlock.WG.Done()
		uploadBlock.UPOBJ.PRO.WriteLength.Add(uploadBlock.Length)
	}
}

func (uploadBlock *UploadBlock) upload() {
	defer uploadBlock.DoFinish()
	err := uploadBlock.BLK.Sum()
	if err != nil {
		uploadBlock.UPOBJ.ERR.Store(pkt.NewErrorMsg(pkt.INVALID_ARGS, err.Error()))
		return
	}
	logrus.Infof("[UploadBlock]%sStart upload block,chan size %d/%d\n",
		uploadBlock.logPrefix,
		cap(BLOCK_MAKE_CH)-len(BLOCK_MAKE_CH), cap(BLOCK_ROUTINE_CH)-len(BLOCK_ROUTINE_CH))
	startTime := time.Now()
	bid := uint32(uploadBlock.ID)
	i1, i2, i3, i4 := pkt.ObjectIdParam(uploadBlock.UPOBJ.VNU)
	vnu := &pkt.UploadBlockInitReqV2_VNU{Timestamp: i1, MachineIdentifier: i2, ProcessIdentifier: i3, Counter: i4}
	//compare s3 flag
	compareFlag := true
	req := &pkt.UploadBlockInitReqV2{
		UserId:      &uploadBlock.UPOBJ.UClient.UserId,
		SignData:    &uploadBlock.UPOBJ.UClient.SignKey.Sign,
		KeyNumber:   &uploadBlock.UPOBJ.UClient.SignKey.KeyNumber,
		VHP:         uploadBlock.BLK.VHP,
		Id:          &bid,
		Vnu:         vnu,
		Version:     &env.Version,
		CompareFlag: &compareFlag,
	}
	resp, errmsg := net.RequestSN(req)
	if errmsg != nil {
		uploadBlock.UPOBJ.ERR.Store(errmsg)
		return
	}
	logrus.Infof("[UploadBlock]%sBlock is initialized,take times %d ms.\n", uploadBlock.logPrefix, time.Since(startTime).Milliseconds())
	dupResp, ok := resp.(*pkt.UploadBlockDupResp)
	if ok {
		osize := uint64(uploadBlock.BLK.OriginalSize)
		rsize := uint32(len(uploadBlock.BLK.Data))
		dupReq := uploadBlock.CheckBlockDup(dupResp)
		v := &pkt.UploadBlockDupReqV2_VNU{Timestamp: i1, MachineIdentifier: i2, ProcessIdentifier: i3, Counter: i4}
		if dupReq != nil {
			startTime = time.Now()
			dupReq.Id = &bid
			dupReq.VHP = uploadBlock.BLK.VHP
			dupReq.OriginalSize = &osize
			dupReq.RealSize = &rsize
			dupReq.Vnu = v
			_, errmsg = net.RequestSN(dupReq)
			if errmsg != nil {
				uploadBlock.UPOBJ.ERR.Store(errmsg)
			} else {
				logrus.Infof("[UploadBlock]%sBlock is a repetitive block %s,take times %d ms.\n", uploadBlock.logPrefix,
					base58.Encode(uploadBlock.BLK.VHP), time.Since(startTime).Milliseconds())
			}
		} else {
			uploadBlock.STime = int64(*dupResp.StartTime)
			uploadBlock.UploadBlockDB()
		}
		return
	}
	undupResp, ok := resp.(*pkt.UploadBlockInitResp)
	if ok {
		uploadBlock.STime = int64(*undupResp.StartTime)
		uploadBlock.UploadBlockDB()
		return
	}
	uploadBlock.UPOBJ.ERR.Store(pkt.NewErrorMsg(pkt.INVALID_ARGS, "Return err msg type"))
}

func (uploadBlock *UploadBlock) UploadBlockDB() {
	if uploadBlock.BLK.InMemory() {
		ks := codec.GenerateRandomKey()
		aes := codec.NewBlockAESEncryptor(uploadBlock.BLK, ks)
		eblk, err := aes.Encrypt()
		if err != nil {
			uploadBlock.UPOBJ.ERR.Store(pkt.NewErrorMsg(pkt.INVALID_ARGS, err.Error()))
			return
		}
		err = eblk.MakeVHB()
		if err != nil {
			uploadBlock.UPOBJ.ERR.Store(pkt.NewErrorMsg(pkt.INVALID_ARGS, err.Error()))
			return
		}
		startTime := time.Now()
		bid := uint32(uploadBlock.ID)
		osize := uint64(uploadBlock.BLK.OriginalSize)
		i1, i2, i3, i4 := pkt.ObjectIdParam(uploadBlock.UPOBJ.VNU)
		vnu := &pkt.UploadBlockDBReqV2_VNU{Timestamp: i1, MachineIdentifier: i2, ProcessIdentifier: i3, Counter: i4}
		req := &pkt.UploadBlockDBReqV2{
			UserId:       &uploadBlock.UPOBJ.UClient.UserId,
			SignData:     &uploadBlock.UPOBJ.UClient.SignKey.Sign,
			KeyNumber:    &uploadBlock.UPOBJ.UClient.SignKey.KeyNumber,
			Id:           &bid,
			Vnu:          vnu,
			VHP:          uploadBlock.BLK.VHP,
			VHB:          eblk.VHB,
			KEU:          codec.ECBEncryptNoPad(ks, uploadBlock.UPOBJ.UClient.StoreKey.AESKey),
			KED:          codec.ECBEncryptNoPad(ks, uploadBlock.BLK.KD),
			OriginalSize: &osize,
			Data:         eblk.Data,
		}
		if uploadBlock.UPOBJ.UClient.StoreKey != uploadBlock.UPOBJ.UClient.SignKey {
			sign, _ := SetStoreNumber(uploadBlock.UPOBJ.UClient.SignKey.Sign, int32(uploadBlock.UPOBJ.UClient.StoreKey.KeyNumber))
			req.SignData = &sign
		}
		_, errmsg := net.RequestSN(req)
		if errmsg != nil {
			uploadBlock.UPOBJ.ERR.Store(errmsg)
		} else {
			logrus.Infof("[UploadBlock]%sUpload block to DB,VHP:%s,take times %d ms.\n", uploadBlock.logPrefix,
				base58.Encode(uploadBlock.BLK.VHP), time.Since(startTime).Milliseconds())
		}
	} else {
		uploadBlock.UploadBlockDedup()
	}
}

func (uploadBlock *UploadBlock) CheckBlockDup(resp *pkt.UploadBlockDupResp) *pkt.UploadBlockDupReqV2 {
	keds := resp.Keds.KED
	vhbs := resp.Vhbs.VHB
	ars := resp.Ars.AR
	for index, ked := range keds {
		ks := codec.ECBDecryptNoPad(ked, uploadBlock.BLK.KD)
		aes := codec.NewBlockAESEncryptor(uploadBlock.BLK, ks)
		eblk, err := aes.Encrypt()
		if err != nil {
			logrus.Warnf("[UploadBlock]%sCheckBlockDup ERR:%s\n", uploadBlock.logPrefix, err)
			return nil
		}
		var vhb []byte
		if eblk.NeedEncode() {
			if ars[index] == codec.AR_RS_MODE {
				logrus.Warnf("[UploadBlock]%sCheckBlockDup ERR:RS Not supported\n", uploadBlock.logPrefix)
				return nil
			} else {
				enc := codec.NewErasureEncoder(eblk)
				err = enc.Encode()
				if err != nil {
					logrus.Warnf("[UploadBlock]%sCheckBlockDup ERR:%s\n", uploadBlock.logPrefix, err)
					return nil
				}
				vhb = eblk.VHB
			}
		} else {
			err = eblk.MakeVHB()
			if err != nil {
				logrus.Warnf("[UploadBlock]%sCheckBlockDup ERR:%s\n", uploadBlock.logPrefix, err)
				return nil
			}
			vhb = eblk.VHB
		}
		if bytes.Equal(vhb, vhbs[index]) {
			keu := codec.ECBEncryptNoPad(ks, uploadBlock.UPOBJ.UClient.StoreKey.AESKey)
			req := &pkt.UploadBlockDupReqV2{
				UserId:    &uploadBlock.UPOBJ.UClient.UserId,
				SignData:  &uploadBlock.UPOBJ.UClient.SignKey.Sign,
				KeyNumber: &uploadBlock.UPOBJ.UClient.SignKey.KeyNumber,
				VHB:       vhb,
				KEU:       keu,
			}
			if uploadBlock.UPOBJ.UClient.StoreKey != uploadBlock.UPOBJ.UClient.SignKey {
				sign, _ := SetStoreNumber(uploadBlock.UPOBJ.UClient.SignKey.Sign, int32(uploadBlock.UPOBJ.UClient.StoreKey.KeyNumber))
				req.SignData = &sign
			}
			return req
		}
	}
	return nil
}

func (uploadBlock *UploadBlock) UploadBlockDedup() {
	ks := codec.GenerateRandomKey()
	rsize := int32(len(uploadBlock.BLK.Data))
	aes := codec.NewBlockAESEncryptor(uploadBlock.BLK, ks)
	eblk, err := aes.Encrypt()
	if err != nil {
		uploadBlock.UPOBJ.ERR.Store(pkt.NewErrorMsg(pkt.INVALID_ARGS, err.Error()))
		return
	}
	enc := codec.NewErasureEncoder(eblk)
	err = enc.Encode()
	if err != nil {
		logrus.Errorf("[UploadBlock]ErasureEncoder ERR:%s\n", uploadBlock.logPrefix, err)
		uploadBlock.UPOBJ.ERR.Store(pkt.NewErrorMsg(pkt.INVALID_ARGS, err.Error()))
		return
	}
	uploadBlock.BLK.Clear()
	eblk.Clear()
	uploadBlock.Queue = NewDNQueue()
	retrytimes := 0
	size := len(enc.Shards)
	ress := make([]*UploadShardResult, size)
	keu := codec.ECBEncryptNoPad(ks, uploadBlock.UPOBJ.UClient.StoreKey.AESKey)
	ked := codec.ECBEncryptNoPad(ks, uploadBlock.BLK.KD)
	useex := false
	var ress2 []*UploadShardResult = nil
	if !enc.IsCopyShard() && size > env.LRCMinShardNum {
		bakcount := size * env.ExtraPercent / 100
		if env.BlkTimeout == 0 {
			ress2 = make([]*UploadShardResult, bakcount)
		} else {
			ress2 = make([]*UploadShardResult, size)
			useex = true
		}
	}
	finishWg := uploadBlock.WG
	uploadBlock.WG = nil
	<-BLOCK_ROUTINE_CH
	startedSign := make(chan int, 1)
	go func() {
		defer func() {
			if r := recover(); r != nil {
				env.TraceError("[UploadBlock]")
				uploadBlock.UPOBJ.ERR.Store(pkt.NewErrorMsg(pkt.SERVER_ERROR, "Unknown error"))
			}
			BLOCK_ROUTINE_CH <- 1
			finishWg.Done()
			uploadBlock.UPOBJ.PRO.WriteLength.Add(uploadBlock.Length)
		}()
		var ids []int32
		for {
			var blkls []int32
			var err *pkt.ErrorMessage = nil
			if useex {
				blkls, err = uploadBlock.UploadShardsEx(uploadBlock.BLK.VHP, keu, ked, eblk.VHB, enc, &rsize, uploadBlock.BLK.OriginalSize, ress, ress2, ids, startedSign)
			} else {
				blkls, err = uploadBlock.UploadShards(uploadBlock.BLK.VHP, keu, ked, eblk.VHB, enc, &rsize, uploadBlock.BLK.OriginalSize, ress, ress2, ids, startedSign)
			}
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

//compare seq
func ToUploadBlockEndReqV2_ShardSeqList(res []*UploadShardResult) []*pkt.UploadBlockEndReqV2_ShardSeqList {
	shardSeqList := make([]*pkt.UploadBlockEndReqV2_ShardSeqList, len(res))
	for index, r := range res {
		shardSeqList[index] = &pkt.UploadBlockEndReqV2_ShardSeqList{
			Seq: &r.Seq,
		}
	}
	return shardSeqList
}

//compare seq lrc2
func ToUploadBlockEndReqV3_ShardSeqList(res []*UploadShardResult) []*pkt.UploadBlockEndReqV3_ShardSeqList {
	shardSeqList := make([]*pkt.UploadBlockEndReqV3_ShardSeqList, len(res))
	for index, r := range res {
		shardSeqList[index] = &pkt.UploadBlockEndReqV3_ShardSeqList{
			Seq: &r.Seq,
		}
	}
	return shardSeqList
}

//compare seq lrc2
func ToUploadBlockEndReqV3_ShardSeqList2(res2 []*UploadShardResult) []*pkt.UploadBlockEndReqV3_ShardSeqList2 {
	shardSeqList2 := make([]*pkt.UploadBlockEndReqV3_ShardSeqList2, len(res2))
	for index, r := range res2 {
		shardSeqList2[index] = &pkt.UploadBlockEndReqV3_ShardSeqList2{
			Seq: &r.Seq,
		}
	}
	return shardSeqList2
}
