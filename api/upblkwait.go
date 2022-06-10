package api

import (
	"encoding/json"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/yottachain/YTCoreService/codec"
	"github.com/yottachain/YTCoreService/env"
	"github.com/yottachain/YTCoreService/net"
	"github.com/yottachain/YTCoreService/pkt"
)

var BLOCK_ROUTINE_CH chan int

func (uploadBlock *UploadBlock) UploadShards(vhp, keu, ked, vhb []byte, enc *codec.ErasureEncoder, rsize *int32,
	originalSize int64, ress []*UploadShardResult, ress2 []*UploadShardResult, ids []int32, startedsign chan int) ([]int32, *pkt.ErrorMessage) {
	size := len(enc.Shards)
	startTime := time.Now()
	count := 0
	for _, res := range ress {
		if res == nil {
			count++
		}
	}
	bakcount := 0
	waitcount := 0
	if ress2 != nil {
		bakcount = size * env.ExtraPercent / 100
		for _, res := range ress2 {
			if res != nil {
				bakcount--
			} else {
				waitcount++
			}
		}
		waitcount = waitcount - bakcount
	}
	uploads := NewUpLoad(uploadBlock.logPrefix, ress, ress2, count, bakcount, waitcount)
	ShardRoutineLock.Lock()
	for index, shd := range enc.Shards {
		if ress[index] == nil {
			StartUploadShard(uploadBlock, shd, int32(index), uploads, ids, false)
		}
	}
	if ress2 != nil {
		for index, shd := range enc.Shards {
			if ress2[index] == nil {
				if !uploads.IsCancle() {
					StartUploadShard(uploadBlock, shd, int32(index), uploads, ids, true)
				}
			}
		}
	}
	ShardRoutineLock.Unlock()
	startedsign <- 1
	er := uploads.WaitUpload()
	if er != nil {
		return nil, pkt.NewErrorMsg(pkt.SERVER_ERROR, "Panic")
	}
	times := time.Since(startTime).Milliseconds()
	logrus.Infof("[UploadBlock]%sUpload block OK,shardcount %d/%d,take times %d ms.\n", uploadBlock.logPrefix, uploads.Count(), size, times)
	AddBlockOK(times)
	startTime = time.Now()
	uid := int32(uploadBlock.UPOBJ.UClient.UserId)
	kn := int32(uploadBlock.UPOBJ.UClient.SignKey.KeyNumber)
	bid := int32(uploadBlock.ID)
	osize := int64(originalSize)
	var ar int32 = 0
	if enc.IsCopyShard() {
		ar = codec.AR_COPY_MODE
	} else {
		ar = enc.DataCount
	}
	var errmsg *pkt.ErrorMessage
	if ress2 == nil || enc.IsCopyShard() {
		i1, i2, i3, i4 := pkt.ObjectIdParam(uploadBlock.UPOBJ.VNU)
		vnu := &pkt.UploadBlockEndReqV2_VNU{Timestamp: i1, MachineIdentifier: i2, ProcessIdentifier: i3, Counter: i4}
		uploads.RLock()
		req := &pkt.UploadBlockEndReqV2{
			UserId:       &uid,
			SignData:     &uploadBlock.UPOBJ.UClient.SignKey.Sign,
			KeyNumber:    &kn,
			Id:           &bid,
			VHP:          vhp,
			VHB:          vhb,
			KEU:          keu,
			KED:          ked,
			Vnu:          vnu,
			OriginalSize: &osize,
			RealSize:     rsize,
			AR:           &ar,
			Oklist:       ToUploadBlockEndReqV2_OkList(ress),
			Vbi:          &uploadBlock.STime,
		}
		uploads.RUnlock()
		if uploadBlock.UPOBJ.UClient.StoreKey != uploadBlock.UPOBJ.UClient.SignKey {
			sign, _ := SetStoreNumber(uploadBlock.UPOBJ.UClient.SignKey.Sign, int32(uploadBlock.UPOBJ.UClient.StoreKey.KeyNumber))
			req.SignData = &sign
		}
		_, errmsg = net.RequestSN(req, uploadBlock.SN, uploadBlock.logPrefix, env.SN_RETRYTIMES, false)
	} else {
		vnu := uploadBlock.UPOBJ.VNU.Hex()
		uploads.RLock()
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
			Oklist:       ToUploadBlockEndReqV3_OkList(ress, ress2),
			Vbi:          &uploadBlock.STime,
		}
		uploads.RUnlock()
		if uploadBlock.UPOBJ.UClient.StoreKey != uploadBlock.UPOBJ.UClient.SignKey {
			sign, _ := SetStoreNumber(uploadBlock.UPOBJ.UClient.SignKey.Sign, int32(uploadBlock.UPOBJ.UClient.StoreKey.KeyNumber))
			req.SignData = &sign
		}
		_, errmsg = net.RequestSN(req, uploadBlock.SN, uploadBlock.logPrefix, env.SN_RETRYTIMES, false)
	}
	if errmsg != nil {
		var ids []int32
		if errmsg.Code == pkt.DN_IN_BLACKLIST {
			ids = uploadBlock.CheckErrorMessage(ress, ress2, errmsg.Msg)
		}
		return ids, errmsg
	} else {
		logrus.Infof("[UploadBlock]%sWrite shardmetas OK,take times %d ms.\n", uploadBlock.logPrefix, time.Now().Sub(startTime).Milliseconds())
		return nil, nil
	}
}

func (uploadBlock *UploadBlock) CheckErrorMessage(ress, ress2 []*UploadShardResult, jsonstr string) []int32 {
	if jsonstr != "" {
		ids := []int32{}
		err := json.Unmarshal([]byte(jsonstr), &ids)
		if err == nil {
			for index, res := range ress {
				if env.IsExistInArray(res.NODE.Id, ids) {
					logrus.Warnf("[UploadBlock]%sFind DN_IN_BLACKLIST ERR:%d\n", uploadBlock.logPrefix, res.NODE.Id)
					ress[index] = nil
					AddError(res.NODE.Id)
				}
			}
			for index, res := range ress2 {
				if env.IsExistInArray(res.NODE.Id, ids) {
					logrus.Warnf("[UploadBlock]%sFind DN_IN_BLACKLIST ERR:%d\n", uploadBlock.logPrefix, res.NODE.Id)
					ress2[index] = nil
					AddError(res.NODE.Id)
				}
			}
			return ids
		}
	}
	for index := range ress {
		ress[index] = nil
	}
	for index := range ress2 {
		ress2[index] = nil
	}
	return nil
}

func ToUploadBlockEndReqV2_OkList(res []*UploadShardResult) []*pkt.UploadBlockEndReqV2_OkList {
	oklist := make([]*pkt.UploadBlockEndReqV2_OkList, len(res))
	for index, r := range res {
		oklist[index] = &pkt.UploadBlockEndReqV2_OkList{
			SHARDID: &r.SHARDID,
			NODEID:  &r.NODE.Id,
			VHF:     r.VHF,
			DNSIGN:  &r.DNSIGN,
		}
	}
	return oklist
}

func ToUploadBlockEndReqV3_OkList(res []*UploadShardResult, res2 []*UploadShardResult) []*pkt.UploadBlockEndReqV3_OkList {
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

func SetStoreNumber(signdata string, storenumber int32) (string, error) {
	type SignData struct {
		Number int32
		Sign   string
	}
	data := &SignData{Number: storenumber, Sign: signdata}
	bs, err := json.Marshal(data)
	if err != nil {
		return "", err
	} else {
		return string(bs), nil
	}
}
