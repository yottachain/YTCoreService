package api

import (
	"bytes"
	"fmt"
	"sync"
	"sync/atomic"
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
	AddBlockMen(&b.Block)
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

func (self *UploadBlock) DoFinish() {
	BLOCK_ROUTINE_CH <- 1
	self.WG.Done()
	atomic.StoreInt64(self.UPOBJ.ActiveTime, time.Now().Unix())
	DecBlockMen(&self.BLK.Block)
	atomic.AddInt64(self.UPOBJ.PRO.WriteLength, self.BLK.Length())
	if r := recover(); r != nil {
		logrus.Errorf("[UploadBlock]%sERR:%s\n", self.logPrefix, r)
		self.UPOBJ.ERR.Store(pkt.NewErrorMsg(pkt.SERVER_ERROR, "Unknown error"))
	}
}

func (self *UploadBlock) upload() {
	defer self.DoFinish()
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
		SignData:  &self.UPOBJ.UClient.Sign,
		KeyNumber: &self.UPOBJ.UClient.KeyNumber,
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
			SignData:     &self.UPOBJ.UClient.Sign,
			KeyNumber:    &self.UPOBJ.UClient.KeyNumber,
			Id:           &bid,
			Vnu:          vnu,
			VHP:          self.BLK.VHP,
			VHB:          eblk.VHB,
			KEU:          codec.ECBEncryptNoPad(ks, self.UPOBJ.UClient.AESKey),
			KED:          codec.ECBEncryptNoPad(ks, self.BLK.KD),
			OriginalSize: &osize,
			Data:         eblk.Data,
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
			keu := codec.ECBEncryptNoPad(ks, self.UPOBJ.UClient.AESKey)
			req := &pkt.UploadBlockDupReqV2{
				UserId:    &self.UPOBJ.UClient.UserId,
				SignData:  &self.UPOBJ.UClient.Sign,
				KeyNumber: &self.UPOBJ.UClient.KeyNumber,
				VHB:       vhb,
				KEU:       keu,
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
		self.UPOBJ.ERR.Store(pkt.NewErrorMsg(pkt.INVALID_ARGS, err.Error()))
		return
	}
	DecBlockMen(&self.BLK.Block)
	self.BLK.Clear()
	eblk.Clear()
	length := AddEncoderMem(enc)
	defer DecMen(length)
	self.Queue = NewDNQueue()
	retrytimes := 0
	for {
		err := self.UploadShards(ks, eblk.VHB, enc, &rsize)
		if err != nil {
			if err.Code == pkt.DN_IN_BLACKLIST {
				logrus.Errorf("[UploadBlock]%sWrite shardmetas ERR:DN_IN_BLACKLIST,RetryTimes %d\n", self.logPrefix, retrytimes)
				NotifyAllocNode(true)
				retrytimes++
				continue
			}
			self.UPOBJ.ERR.Store(err)
		}
		break
	}
}

func (self *UploadBlock) UploadShards(ks []byte, vhb []byte, enc *codec.ErasureEncoder, rsize *int32) *pkt.ErrorMessage {
	size := len(enc.Shards)
	ress := make([]*UploadShardResult, size)
	startTime := time.Now()
	wgroup := sync.WaitGroup{}
	wgroup.Add(size)
	for index, shd := range enc.Shards {
		ress[index] = StartUploadShard(self, shd, int32(index), &wgroup)
	}
	wgroup.Wait()
	logrus.Infof("[UploadBlock]%sUpload block OK,shardcount %d,take times %d ms.\n", self.logPrefix, size, time.Now().Sub(startTime).Milliseconds())
	startTime = time.Now()
	uid := int32(self.UPOBJ.UClient.UserId)
	kn := int32(self.UPOBJ.UClient.KeyNumber)
	bid := int32(self.ID)
	osize := int64(self.BLK.OriginalSize)
	i1, i2, i3, i4 := pkt.ObjectIdParam(self.UPOBJ.VNU)
	vnu := &pkt.UploadBlockEndReqV2_VNU{Timestamp: i1, MachineIdentifier: i2, ProcessIdentifier: i3, Counter: i4}
	var ar int32 = 0
	if enc.IsCopyShard() {
		ar = codec.AR_COPY_MODE
	} else {
		ar = enc.DataCount
	}
	req := &pkt.UploadBlockEndReqV2{
		UserId:       &uid,
		SignData:     &self.UPOBJ.UClient.Sign,
		KeyNumber:    &kn,
		Id:           &bid,
		VHP:          self.BLK.VHP,
		VHB:          vhb,
		KEU:          codec.ECBEncryptNoPad(ks, self.UPOBJ.UClient.AESKey),
		KED:          codec.ECBEncryptNoPad(ks, self.BLK.KD),
		Vnu:          vnu,
		OriginalSize: &osize,
		RealSize:     rsize,
		AR:           &ar,
		Oklist:       ToUploadBlockEndReqV2_OkList(ress),
	}
	_, errmsg := net.RequestSN(req, self.SN, self.logPrefix, env.SN_RETRYTIMES, false)
	if errmsg != nil {
		return errmsg
	} else {
		logrus.Infof("[UploadBlock]%sWrite shardmetas OK,take times %d ms.\n", self.logPrefix, time.Now().Sub(startTime).Milliseconds())
		return nil
	}
}

func ToUploadBlockEndReqV2_OkList(res []*UploadShardResult) []*pkt.UploadBlockEndReqV2_OkList {
	oklist := make([]*pkt.UploadBlockEndReqV2_OkList, len(res))
	for index, r := range res {
		oklist[index] = &pkt.UploadBlockEndReqV2_OkList{
			SHARDID: &r.SHARDID,
			NODEID:  &r.NODEID,
			VHF:     r.VHF,
			DNSIGN:  &r.DNSIGN,
		}
	}
	return oklist
}
