package api

import (
	"bytes"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/aurawing/eos-go/btcsuite/btcutil/base58"
	"github.com/sirupsen/logrus"
	"github.com/yottachain/YTCoreService/codec"
	"github.com/yottachain/YTCoreService/env"
	"github.com/yottachain/YTCoreService/net"
	"github.com/yottachain/YTCoreService/pkt"
	"github.com/yottachain/YTDNMgmt"
)

var BLOCK_ROUTINE_CH chan int

func InitBlockRoutinePool() {
	BLOCK_ROUTINE_CH = make(chan int, env.UploadBlockThreadNum)
	for ii := 0; ii < env.UploadBlockThreadNum; ii++ {
		BLOCK_ROUTINE_CH <- 1
	}
}

func StartUploadBlock(id int16, b *codec.PlainBlock, up *UploadObject, wg *sync.WaitGroup) {
	ub := &UploadBlock{
		UPOBJ: up,
		ID:    id,
		BLK:   b,
		WG:    wg,
	}
	ub.logPrefix = fmt.Sprintf("[%s][%d]", ub.UPOBJ.VNU.Hex(), ub.ID)
	<-BLOCK_ROUTINE_CH
	go ub.upload()
}

type UploadBlock struct {
	ID        int16
	BLK       *codec.PlainBlock
	UPOBJ     *UploadObject
	Queue     *DNQueue
	logPrefix string
	SN        *YTDNMgmt.SuperNode
	WG        *sync.WaitGroup
	STime     int64
}

func (self *UploadBlock) DoFinish(size int64) {
	if r := recover(); r != nil {
		env.TraceError("[UploadBlock]")
		self.UPOBJ.ERR.Store(pkt.NewErrorMsg(pkt.SERVER_ERROR, "Unknown error"))
	}
	BLOCK_ROUTINE_CH <- 1
	self.WG.Done()
	self.UPOBJ.ActiveTime.Set(time.Now().Unix())
	self.UPOBJ.PRO.WriteLength.Add(size)
}

func (self *UploadBlock) upload() {
	size := self.BLK.Length()
	defer self.DoFinish(size)
	err := self.BLK.Sum()
	if err != nil {
		self.UPOBJ.ERR.Store(pkt.NewErrorMsg(pkt.INVALID_ARGS, err.Error()))
		return
	}
	self.SN = net.GetBlockSuperNode(self.BLK.VHP)
	logrus.Infof("[UploadBlock]%sStart upload block to sn %d\n", self.logPrefix, self.SN.ID)
	startTime := time.Now()
	bid := uint32(self.ID)
	i1, i2, i3, i4 := pkt.ObjectIdParam(self.UPOBJ.VNU)
	vnu := &pkt.UploadBlockInitReqV2_VNU{Timestamp: i1, MachineIdentifier: i2, ProcessIdentifier: i3, Counter: i4}
	req := &pkt.UploadBlockInitReqV2{
		UserId:    &self.UPOBJ.UClient.UserId,
		SignData:  &self.UPOBJ.UClient.SignKey.Sign,
		KeyNumber: &self.UPOBJ.UClient.SignKey.KeyNumber,
		VHP:       self.BLK.VHP,
		Id:        &bid,
		Vnu:       vnu,
		Version:   &env.VersionID,
	}
	resp, errmsg := net.RequestSN(req, self.SN, self.logPrefix, env.SN_RETRYTIMES, false)
	if errmsg != nil {
		self.UPOBJ.ERR.Store(errmsg)
		return
	}
	logrus.Infof("[UploadBlock]%sBlock is initialized at sn %d,take times %d ms.\n", self.logPrefix, self.SN.ID, time.Now().Sub(startTime).Milliseconds())
	dupResp, ok := resp.(*pkt.UploadBlockDupResp)
	if ok {
		osize := uint64(self.BLK.OriginalSize)
		rsize := uint32(len(self.BLK.Data))
		dupReq := self.CheckBlockDup(dupResp)
		v := &pkt.UploadBlockDupReqV2_VNU{Timestamp: i1, MachineIdentifier: i2, ProcessIdentifier: i3, Counter: i4}
		if dupReq != nil {
			startTime = time.Now()
			dupReq.Id = &bid
			dupReq.VHP = self.BLK.VHP
			dupReq.OriginalSize = &osize
			dupReq.RealSize = &rsize
			dupReq.Vnu = v
			_, errmsg = net.RequestSN(dupReq, self.SN, self.logPrefix, env.SN_RETRYTIMES, false)
			if errmsg != nil {
				self.UPOBJ.ERR.Store(errmsg)
			} else {
				logrus.Infof("[UploadBlock]%sBlock is a repetitive block %s,take times %d ms.\n", self.logPrefix,
					base58.Encode(self.BLK.VHP), time.Now().Sub(startTime).Milliseconds())
			}
		} else {
			self.STime = int64(*dupResp.StartTime)
			self.UploadBlockDB()
		}
		return
	}
	undupResp, ok := resp.(*pkt.UploadBlockInitResp)
	if ok {
		self.STime = int64(*undupResp.StartTime)
		self.UploadBlockDB()
		return
	}
	self.UPOBJ.ERR.Store(pkt.NewErrorMsg(pkt.INVALID_ARGS, "Return err msg type"))
}

func (self *UploadBlock) UploadBlockDB() {
	if self.BLK.InMemory() {
		ks := codec.GenerateRandomKey()
		aes := codec.NewBlockAESEncryptor(self.BLK, ks)
		eblk, err := aes.Encrypt()
		if err != nil {
			self.UPOBJ.ERR.Store(pkt.NewErrorMsg(pkt.INVALID_ARGS, err.Error()))
			return
		}
		err = eblk.MakeVHB()
		if err != nil {
			self.UPOBJ.ERR.Store(pkt.NewErrorMsg(pkt.INVALID_ARGS, err.Error()))
			return
		}
		startTime := time.Now()
		bid := uint32(self.ID)
		osize := uint64(self.BLK.OriginalSize)
		i1, i2, i3, i4 := pkt.ObjectIdParam(self.UPOBJ.VNU)
		vnu := &pkt.UploadBlockDBReqV2_VNU{Timestamp: i1, MachineIdentifier: i2, ProcessIdentifier: i3, Counter: i4}
		req := &pkt.UploadBlockDBReqV2{
			UserId:       &self.UPOBJ.UClient.UserId,
			SignData:     &self.UPOBJ.UClient.SignKey.Sign,
			KeyNumber:    &self.UPOBJ.UClient.SignKey.KeyNumber,
			Id:           &bid,
			Vnu:          vnu,
			VHP:          self.BLK.VHP,
			VHB:          eblk.VHB,
			KEU:          codec.ECBEncryptNoPad(ks, self.UPOBJ.UClient.StoreKey.AESKey),
			KED:          codec.ECBEncryptNoPad(ks, self.BLK.KD),
			OriginalSize: &osize,
			Data:         eblk.Data,
		}
		if self.UPOBJ.UClient.StoreKey != self.UPOBJ.UClient.SignKey {
			sign, _ := SetStoreNumber(self.UPOBJ.UClient.SignKey.Sign, int32(self.UPOBJ.UClient.StoreKey.KeyNumber))
			req.SignData = &sign
		}
		_, errmsg := net.RequestSN(req, self.SN, self.logPrefix, env.SN_RETRYTIMES, false)
		if errmsg != nil {
			self.UPOBJ.ERR.Store(errmsg)
		} else {
			logrus.Infof("[UploadBlock]%sUpload block to DB,VHP:%s,take times %d ms.\n", self.logPrefix,
				base58.Encode(self.BLK.VHP), time.Now().Sub(startTime).Milliseconds())
		}
	} else {
		self.UploadBlockDedup()
	}
}

func (self *UploadBlock) CheckBlockDup(resp *pkt.UploadBlockDupResp) *pkt.UploadBlockDupReqV2 {
	keds := resp.Keds.KED
	vhbs := resp.Vhbs.VHB
	ars := resp.Ars.AR
	for index, ked := range keds {
		ks := codec.ECBDecryptNoPad(ked, self.BLK.KD)
		aes := codec.NewBlockAESEncryptor(self.BLK, ks)
		eblk, err := aes.Encrypt()
		if err != nil {
			logrus.Warnf("[UploadBlock]%sCheckBlockDup ERR:%s\n", self.logPrefix, err)
			return nil
		}
		var vhb []byte
		if eblk.NeedEncode() {
			if ars[index] == codec.AR_RS_MODE {
				logrus.Warnf("[UploadBlock]%sCheckBlockDup ERR:RS Not supported\n", self.logPrefix)
				return nil
			} else {
				enc := codec.NewErasureEncoder(eblk)
				err = enc.Encode()
				if err != nil {
					logrus.Warnf("[UploadBlock]%sCheckBlockDup ERR:%s\n", self.logPrefix, err)
					return nil
				}
				vhb = eblk.VHB
			}
		} else {
			err = eblk.MakeVHB()
			if err != nil {
				logrus.Warnf("[UploadBlock]%sCheckBlockDup ERR:%s\n", self.logPrefix, err)
				return nil
			}
			vhb = eblk.VHB
		}
		if bytes.Equal(vhb, vhbs[index]) {
			keu := codec.ECBEncryptNoPad(ks, self.UPOBJ.UClient.StoreKey.AESKey)
			req := &pkt.UploadBlockDupReqV2{
				UserId:    &self.UPOBJ.UClient.UserId,
				SignData:  &self.UPOBJ.UClient.SignKey.Sign,
				KeyNumber: &self.UPOBJ.UClient.SignKey.KeyNumber,
				VHB:       vhb,
				KEU:       keu,
			}
			if self.UPOBJ.UClient.StoreKey != self.UPOBJ.UClient.SignKey {
				sign, _ := SetStoreNumber(self.UPOBJ.UClient.SignKey.Sign, int32(self.UPOBJ.UClient.StoreKey.KeyNumber))
				req.SignData = &sign
			}
			return req
		}
	}
	return nil
}

func (self *UploadBlock) UploadBlockDedup() {
	ks := codec.GenerateRandomKey()
	rsize := int32(len(self.BLK.Data))
	aes := codec.NewBlockAESEncryptor(self.BLK, ks)
	eblk, err := aes.Encrypt()
	if err != nil {
		self.UPOBJ.ERR.Store(pkt.NewErrorMsg(pkt.INVALID_ARGS, err.Error()))
		return
	}
	enc := codec.NewErasureEncoder(eblk)
	err = enc.Encode()
	if err != nil {
		logrus.Errorf("[UploadBlock]ErasureEncoder ERR:%s\n", self.logPrefix, err)
		self.UPOBJ.ERR.Store(pkt.NewErrorMsg(pkt.INVALID_ARGS, err.Error()))
		return
	}
	self.BLK.Clear()
	eblk.Clear()
	self.Queue = NewDNQueue()
	retrytimes := 0
	size := len(enc.Shards)
	ress := make([]*UploadShardResult, size)
	keu := codec.ECBEncryptNoPad(ks, self.UPOBJ.UClient.StoreKey.AESKey)
	ked := codec.ECBEncryptNoPad(ks, self.BLK.KD)
	var ress2 []*UploadShardResult = nil
	if env.LRC2 {
		ress2 = make([]*UploadShardResult, size)
	}
	var ids []int32
	for {
		blkls, err := self.UploadShards(self.BLK.VHP, keu, ked, eblk.VHB, enc, &rsize, self.BLK.OriginalSize, ress, ress2, ids)
		if err != nil {
			if err.Code == pkt.DN_IN_BLACKLIST {
				ids = blkls
				logrus.Errorf("[UploadBlock]%sWrite shardmetas ERR:DN_IN_BLACKLIST,RetryTimes %d\n", self.logPrefix, retrytimes)
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

func (self *UploadBlock) UploadShards(vhp, keu, ked, vhb []byte, enc *codec.ErasureEncoder, rsize *int32,
	originalSize int64, ress []*UploadShardResult, ress2 []*UploadShardResult, ids []int32) ([]int32, *pkt.ErrorMessage) {
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
	uploads := NewUpLoad(self.logPrefix, ress, ress2, count, bakcount, waitcount)
	for index, shd := range enc.Shards {
		if ress[index] == nil {
			StartUploadShard(self, shd, int32(index), uploads, ids, false)
		}
	}
	if ress2 != nil {
		for index, shd := range enc.Shards {
			if ress2[index] == nil {
				StartUploadShard(self, shd, int32(index), uploads, ids, true)
			}
		}
	}
	er := uploads.WaitUpload(enc.IsCopyShard())
	if er != nil {
		return nil, pkt.NewErrorMsg(pkt.SERVER_ERROR, "Panic")
	}
	times := time.Since(startTime).Milliseconds()
	logrus.Infof("[UploadBlock]%sUpload block OK,shardcount %d/%d,take times %d ms.\n", self.logPrefix, uploads.Count(enc.IsCopyShard()), size, times)
	AddBlockOK(times)
	startTime = time.Now()
	uid := int32(self.UPOBJ.UClient.UserId)
	kn := int32(self.UPOBJ.UClient.SignKey.KeyNumber)
	bid := int32(self.ID)
	osize := int64(originalSize)
	var ar int32 = 0
	if enc.IsCopyShard() {
		ar = codec.AR_COPY_MODE
	} else {
		ar = enc.DataCount
	}
	var errmsg *pkt.ErrorMessage
	if ress2 == nil || enc.IsCopyShard() {
		i1, i2, i3, i4 := pkt.ObjectIdParam(self.UPOBJ.VNU)
		vnu := &pkt.UploadBlockEndReqV2_VNU{Timestamp: i1, MachineIdentifier: i2, ProcessIdentifier: i3, Counter: i4}
		uploads.RLock()
		req := &pkt.UploadBlockEndReqV2{
			UserId:       &uid,
			SignData:     &self.UPOBJ.UClient.SignKey.Sign,
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
			Vbi:          &self.STime,
		}
		uploads.RUnlock()
		if self.UPOBJ.UClient.StoreKey != self.UPOBJ.UClient.SignKey {
			sign, _ := SetStoreNumber(self.UPOBJ.UClient.SignKey.Sign, int32(self.UPOBJ.UClient.StoreKey.KeyNumber))
			req.SignData = &sign
		}
		_, errmsg = net.RequestSN(req, self.SN, self.logPrefix, env.SN_RETRYTIMES, false)
	} else {
		vnu := self.UPOBJ.VNU.Hex()
		uploads.RLock()
		req := &pkt.UploadBlockEndReqV3{
			UserId:       &uid,
			SignData:     &self.UPOBJ.UClient.SignKey.Sign,
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
			Vbi:          &self.STime,
		}
		uploads.RUnlock()
		if self.UPOBJ.UClient.StoreKey != self.UPOBJ.UClient.SignKey {
			sign, _ := SetStoreNumber(self.UPOBJ.UClient.SignKey.Sign, int32(self.UPOBJ.UClient.StoreKey.KeyNumber))
			req.SignData = &sign
		}
		_, errmsg = net.RequestSN(req, self.SN, self.logPrefix, env.SN_RETRYTIMES, false)
	}
	if errmsg != nil {
		var ids []int32
		if errmsg.Code == pkt.DN_IN_BLACKLIST {
			ids = self.CheckErrorMessage(ress, ress2, errmsg.Msg)
		}
		return ids, errmsg
	} else {
		logrus.Infof("[UploadBlock]%sWrite shardmetas OK,take times %d ms.\n", self.logPrefix, time.Now().Sub(startTime).Milliseconds())
		return nil, nil
	}
}

func (self *UploadBlock) CheckErrorMessage(ress, ress2 []*UploadShardResult, jsonstr string) []int32 {
	if jsonstr != "" {
		ids := []int32{}
		err := json.Unmarshal([]byte(jsonstr), &ids)
		if err == nil {
			for index, res := range ress {
				if env.IsExistInArray(res.NODE.Id, ids) {
					logrus.Warnf("[UploadBlock]%sFind DN_IN_BLACKLIST ERR:%d\n", self.logPrefix, res.NODE.Id)
					ress[index] = nil
					AddError(res.NODE.Id)
				}
			}
			for index, res := range ress2 {
				if env.IsExistInArray(res.NODE.Id, ids) {
					logrus.Warnf("[UploadBlock]%sFind DN_IN_BLACKLIST ERR:%d\n", self.logPrefix, res.NODE.Id)
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
