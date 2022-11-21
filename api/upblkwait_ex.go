package api

import (
	"sync/atomic"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/yottachain/YTCoreService/codec"
	"github.com/yottachain/YTCoreService/env"
	"github.com/yottachain/YTCoreService/net"
	"github.com/yottachain/YTCoreService/pkt"
)

func (uploadBlock *UploadBlock) UploadShardsEx(vhp, keu, ked, vhb []byte, enc *codec.ErasureEncoder, rsize *int32,
	originalSize int64, ress []*UploadShardResult, ress2 []*UploadShardResult, ids []int32, startedsign chan int) ([]int32, *pkt.ErrorMessage) {
	size := len(enc.Shards)
	startTime := time.Now()
	count := 0
	for _, res := range ress {
		if res == nil {
			count++
		}
	}
	uploads := NewUpLoad(uploadBlock.logPrefix, ress, ress2, count)
	ShardRoutineLock.Lock()
	for index, shd := range enc.Shards {
		if ress[index] == nil {
			StartUploadShardEx(uploadBlock, shd, int32(index), uploads, ids, false)
		}
	}
	ShardRoutineLock.Unlock()

	startedsign <- 1
	er := uploads.WaitUpload()
	if er != nil {
		return nil, pkt.NewErrorMsg(pkt.SERVER_ERROR, "Panic")
	}
	if uploads.waitnum > 0 {
		uploads.RLock()
		var bakids []int
		for index, res := range ress {
			if res == nil {
				bakids = append(bakids, index)
			}
		}
		uploads.RUnlock()
		baknum := len(bakids)
		if baknum > 0 {
			ShardRoutineLock.Lock()
			for _, id := range bakids {
				if !uploads.IsCancle() {
					atomic.AddInt32(&uploads.waitnum, 1)
					StartUploadShardEx(uploadBlock, enc.Shards[id], int32(id), uploads, ids, false)
				} else {
					break
				}
			}
			ShardRoutineLock.Unlock()
		}
		if env.ThrowErr {
			for _, shd := range enc.Shards {
				if shd.IsCopyShard() {
					shd.Clear()
					break
				}
				shd.Clear()
			}
		}
		er := uploads.WaitComplete()
		if er != nil {
			return nil, pkt.NewErrorMsg(pkt.SERVER_ERROR, "Panic")
		}
	}
	times := time.Since(startTime).Milliseconds()
	logrus.Infof("[UploadBlock]%sUpload block OK,shardcount %d/%d,take times %d ms.\n", uploadBlock.logPrefix, uploads.baknum+count, size, times)
	startTime = time.Now()
	uid := int32(uploadBlock.UPOBJ.UClient.UserId)
	kn := int32(uploadBlock.UPOBJ.UClient.SignKey.KeyNumber)
	bid := int32(uploadBlock.ID)
	osize := int64(originalSize)
	var ar int32 = enc.DataCount
	var errmsg *pkt.ErrorMessage
	vnu := uploadBlock.UPOBJ.VNU.Hex()
	var vbi *int64 = &uploadBlock.STime
	if uploadBlock.STime == 0 {
		vbi = nil
	}
	req := &pkt.UploadBlockEndReqV3{
		UserId:       &uid,
		SignData:     &uploadBlock.UPOBJ.UClient.SignKey.Sign,
		KeyNumber:    &kn,
		Id:           &bid,
		VHP:          vhp,
		VHB:          vhb,
		KEU:          keu,
		KED:          ked,
		VNU:          &vnu,
		OriginalSize: &osize,
		RealSize:     rsize,
		AR:           &ar,
		Oklist:       ToUploadBlockEndReqV3_OkListEx(ress, ress2),
		Vbi:          vbi,
	}
	if uploadBlock.UPOBJ.UClient.StoreKey != uploadBlock.UPOBJ.UClient.SignKey {
		sign, _ := SetStoreNumber(uploadBlock.UPOBJ.UClient.SignKey.Sign, int32(uploadBlock.UPOBJ.UClient.StoreKey.KeyNumber))
		req.SignData = &sign
	}
	_, errmsg = net.RequestSN(req)
	if errmsg != nil {
		var ids []int32
		if errmsg.Code == pkt.DN_IN_BLACKLIST {
			ids = uploadBlock.CheckErrorMessage(ress, ress2, errmsg.Msg)
		}
		return ids, errmsg
	} else {
		logrus.Infof("[UploadBlock]%sWrite shardmetas OK,take times %d ms.\n", uploadBlock.logPrefix, time.Since(startTime).Milliseconds())
		return nil, nil
	}
}

func ToUploadBlockEndReqV3_OkListEx(res []*UploadShardResult, res2 []*UploadShardResult) []*pkt.UploadBlockEndReqV3_OkList {
	oklist := make([]*pkt.UploadBlockEndReqV3_OkList, len(res))
	for index, r := range res {
		oklist[index] = &pkt.UploadBlockEndReqV3_OkList{
			SHARDID: &r.SHARDID,
			NODEID:  &r.NODE.Id,
			VHF:     r.VHF,
			DNSIGN:  &r.DNSIGN,
		}
		if res2[index] != nil {
			oklist[index].NODEID2 = &res2[index].NODE.Id
			oklist[index].DNSIGN2 = &res2[index].DNSIGN
		}
	}
	return oklist
}
