package api

import (
	"encoding/json"
	"sync"
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
	isCopyShard := enc.IsCopyShard()
	wgroup := sync.WaitGroup{}
	num := 0
	ShardRoutineLock.Lock()
	for index, shd := range enc.Shards {
		if ress[index] == nil {
			wgroup.Add(1)
			ress[index] = StartUploadShard(uploadBlock, shd, int32(index), &wgroup, ids, false)
			num++
		}
	}
	for index, res := range ress2 {
		if res == nil {
			wgroup.Add(1)
			ress2[index] = StartUploadShard(uploadBlock, enc.Shards[index], int32(index), &wgroup, ids, true)
			num++
		}
	}
	ShardRoutineLock.Unlock()
	startedsign <- 1
	if env.ThrowErr {
		for _, shd := range enc.Shards {
			if shd.IsCopyShard() {
				shd.Clear()
				break
			}
			shd.Clear()
		}
	}
	wgroup.Wait()
	if uploadBlock.CheckSendShardPanic(ress, ress2) {
		return nil, pkt.NewErrorMsg(pkt.SERVER_ERROR, "Panic")
	}
	times := time.Since(startTime).Milliseconds()
	logrus.Infof("[UploadBlock]%sUpload block OK,shardcount %d/%d,take times %d ms.\n", uploadBlock.logPrefix, num, size, times)
	startTime = time.Now()
	uid := int32(uploadBlock.UPOBJ.UClient.UserId)
	kn := int32(uploadBlock.UPOBJ.UClient.SignKey.KeyNumber)
	bid := int32(uploadBlock.ID)
	osize := int64(originalSize)
	var ar int32 = 0
	if isCopyShard {
		ar = codec.AR_COPY_MODE
	} else {
		ar = enc.DataCount
	}
	var errmsg *pkt.ErrorMessage
	if ress2 == nil || isCopyShard {
		i1, i2, i3, i4 := pkt.ObjectIdParam(uploadBlock.UPOBJ.VNU)
		vnu := &pkt.UploadBlockEndReqV2_VNU{Timestamp: i1, MachineIdentifier: i2, ProcessIdentifier: i3, Counter: i4}
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
		if uploadBlock.UPOBJ.UClient.StoreKey != uploadBlock.UPOBJ.UClient.SignKey {
			sign, _ := SetStoreNumber(uploadBlock.UPOBJ.UClient.SignKey.Sign, int32(uploadBlock.UPOBJ.UClient.StoreKey.KeyNumber))
			req.SignData = &sign
		}
		_, errmsg = net.RequestSN(req)
	} else {
		vnu := uploadBlock.UPOBJ.VNU.Hex()
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
		if uploadBlock.UPOBJ.UClient.StoreKey != uploadBlock.UPOBJ.UClient.SignKey {
			sign, _ := SetStoreNumber(uploadBlock.UPOBJ.UClient.SignKey.Sign, int32(uploadBlock.UPOBJ.UClient.StoreKey.KeyNumber))
			req.SignData = &sign
		}
		_, errmsg = net.RequestSN(req)
	}
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
					res2size := len(ress2)
					if index < res2size && ress2[index] != nil {
						if env.IsExistInArray(ress2[index].NODE.Id, ids) {
							logrus.Warnf("[UploadBlock]%sFind DN_IN_BLACKLIST ERR:%d\n", uploadBlock.logPrefix, ress2[index].NODE.Id)
							AddError(ress2[index].NODE.Id)
							ress2[index] = nil
						} else {
							ress[index] = ress2[index]
							ress2[index] = nil
						}
					}
				}
			}
			for index, res := range ress2 {
				if res != nil && env.IsExistInArray(res.NODE.Id, ids) {
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

func (uploadBlock *UploadBlock) CheckSendShardPanic(ress []*UploadShardResult, ress2 []*UploadShardResult) bool {
	for index, res := range ress {
		if res.NODE == nil {
			ress[index] = nil
			return true
		}
	}
	for index, res := range ress2 {
		if res.NODE == nil {
			ress[index] = nil
			return true
		}
	}
	return false
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
	size := len(res2)
	for index, r := range res {
		oklist[index] = &pkt.UploadBlockEndReqV3_OkList{
			SHARDID: &r.SHARDID,
			NODEID:  &r.NODE.Id,
			VHF:     r.VHF,
			DNSIGN:  &r.DNSIGN,
		}
		if index < size {
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
