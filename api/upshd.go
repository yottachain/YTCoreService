package api

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aurawing/eos-go/btcsuite/btcutil/base58"
	"github.com/sirupsen/logrus"
	"github.com/yottachain/YTCoreService/codec"
	"github.com/yottachain/YTCoreService/env"
	"github.com/yottachain/YTCoreService/net"
	"github.com/yottachain/YTCoreService/pkt"
)

var SHARD_UP_CH chan int

func InitShardUpPool() {
	SHARD_UP_CH = make(chan int, env.UploadShardThreadNum)
	for ii := 0; ii < env.UploadShardThreadNum; ii++ {
		SHARD_UP_CH <- 1
	}
}

func StartUploadShard(upblk *UploadBlock, shd *codec.Shard, shdid int32, us *UpLoadShards, ids []int32, lrc2 bool) {
	upshd := &UploadShard{uploadBlock: upblk, shard: shd, shardId: shdid, retrytimes: 0, parent: us}
	if lrc2 {
		upshd.logPrefix = fmt.Sprintf("[%s][%d][%d-1]", upblk.UPOBJ.VNU.Hex(), upblk.ID, shdid)
	} else {
		upshd.logPrefix = fmt.Sprintf("[%s][%d][%d-0]", upblk.UPOBJ.VNU.Hex(), upblk.ID, shdid)
	}
	upshd.res = &UploadShardResult{SHARDID: shdid, VHF: shd.VHF}
	upshd.blkList = ids
	<-SHARD_UP_CH
	go upshd.DoSend()
}

const DN_RES_OK = 0
const DN_RES_NETIOERR = 400
const DN_RES_BAD_REQUEST = 100
const DN_RES_NO_SPACE = 101
const DN_RES_VNF_EXISTS = 102
const DN_RES_CACHE_FILL = 105

type UpLoadShards struct {
	sync.RWMutex
	cancel    *int32
	logPrefix string
	okSign    chan int
	bakSign   chan int
	bakcount  int
	waitcount int
	ress      []*UploadShardResult
	ress2     []*UploadShardResult
	count     int
}

func NewUpLoad(logpre string, ress []*UploadShardResult, ress2 []*UploadShardResult, chansize, chansize2, chansize3 int) *UpLoadShards {
	dns := &UpLoadShards{cancel: new(int32), logPrefix: logpre}
	dns.okSign = make(chan int, chansize)
	dns.ress = ress
	dns.ress2 = ress2
	dns.bakcount = chansize2
	dns.waitcount = chansize3
	if chansize2 > 0 {
		dns.bakSign = make(chan int, chansize2+chansize3)
	}
	*dns.cancel = 0
	return dns
}

func (upLoadShards *UpLoadShards) WaitUpload(iscopymode bool) error {
	startTime := time.Now().Unix()
	size := len(upLoadShards.ress)
	for ii := 0; ii < size; ii++ {
		sign := <-upLoadShards.okSign
		if sign < 0 {
			return errors.New("")
		}
	}
	if iscopymode {
		atomic.StoreInt32(upLoadShards.cancel, 1)
		return nil
	}
	for ii := 0; ii < upLoadShards.bakcount; ii++ {
		sign := <-upLoadShards.bakSign
		if sign < 0 {
			return errors.New("")
		}
	}
	t := int64(env.BlkTimeout) - (time.Now().Unix() - startTime)
	if t <= 0 {
		atomic.StoreInt32(upLoadShards.cancel, 1)
		return nil
	}
	timeout := time.After(time.Second * time.Duration(t))
	for ii := 0; ii < upLoadShards.waitcount; ii++ {
		select {
		case <-upLoadShards.bakSign:
		case <-timeout:
			atomic.StoreInt32(upLoadShards.cancel, 1)
			return nil
		}
	}
	atomic.StoreInt32(upLoadShards.cancel, 1)
	return nil
}

func (upLoadShards *UpLoadShards) Count(iscopymode bool) int {
	upLoadShards.RLock()
	defer upLoadShards.RUnlock()
	if iscopymode {
		return len(upLoadShards.ress)
	} else {
		return upLoadShards.count
	}
}

func (upLoadShards *UpLoadShards) OnResponse(rec *UploadShardResult) {
	upLoadShards.Lock()
	defer upLoadShards.Unlock()
	if upLoadShards.ress[rec.SHARDID] == nil {
		if rec.NODE == nil {
			upLoadShards.okSign <- -1
		} else {
			upLoadShards.ress[rec.SHARDID] = rec
			upLoadShards.okSign <- 1
			upLoadShards.count++
		}
	} else {
		if rec.NODE == nil {
			upLoadShards.bakSign <- -1
		} else {
			upLoadShards.ress2[rec.SHARDID] = rec
			upLoadShards.bakSign <- 1
			upLoadShards.count++
		}
	}
}

func (upLoadShards *UpLoadShards) IsCancle() bool {
	return atomic.LoadInt32(upLoadShards.cancel) == 1
}

type UploadShardResult struct {
	SHARDID int32
	NODE    *NodeStat
	VHF     []byte
	DNSIGN  string
}

type UploadShard struct {
	uploadBlock *UploadBlock
	shard       *codec.Shard
	shardId     int32
	logPrefix   string
	res         *UploadShardResult
	retrytimes  uint32
	blkList     []int32
	parent      *UpLoadShards
}

func (us *UploadShard) DoFinish() {
	if r := recover(); r != nil {
		env.TraceError("[UploadShard]")
	}
	SHARD_UP_CH <- 1
	us.parent.OnResponse(us.res)
}

func (us *UploadShard) MakeRequest(ns *NodeStatWOK) *pkt.UploadShardReq {
	return &pkt.UploadShardReq{
		SHARDID:  us.shardId,
		BPDID:    ns.NodeInfo.SnId(),
		BPDSIGN:  []byte(ns.NodeInfo.sign),
		DAT:      us.shard.Data,
		VHF:      us.shard.VHF,
		USERSIGN: []byte(us.uploadBlock.UPOBJ.Sign),
		HASHID:   us.uploadBlock.STime + int64(us.shardId),
	}
}

func (us *UploadShard) GetToken(node *NodeStatWOK) (int, *pkt.GetNodeCapacityResp, error) {
	logrus.Debugf("[UploadShard]%sGetToken from %d......\n", us.logPrefix, node.NodeInfo.Id)
	ctlreq := &pkt.GetNodeCapacityReq{StartTime: uint64(us.uploadBlock.STime),
		RetryTimes: uint32(us.retrytimes)}
	times := 0
	for {
		msg, err := net.CallDN(ctlreq, &node.NodeInfo.Node, us.logPrefix, int64(env.GetTokenTimeout))
		times++
		if err != nil {
			if strings.Contains(err.Msg, "no handler") {
				AddError(node.NodeInfo.Id)
			}
			node.NodeInfo.SetERR()
			return times, nil, errors.New(err.Msg)
		} else {
			resp, ok := msg.(*pkt.GetNodeCapacityResp)
			if !ok {
				node.NodeInfo.SetERR()
				return times, nil, errors.New("RESP_INVALID_MSG")
			}
			if resp.Writable && resp.AllocId != "" {
				return times, resp, nil
			} else {
				if times >= env.UploadShardRetryTimes || us.parent.IsCancle() {
					return times, nil, errors.New("NO_TOKEN")
				}
			}
		}
	}
}

func (us *UploadShard) SendShard(node *NodeStatWOK, req *pkt.UploadShardReq) (*pkt.UploadShard2CResp, error) {
	logrus.Debugf("[UploadShard]%sSendShard %s to %d......\n", us.logPrefix, base58.Encode(req.VHF), node.NodeInfo.Id)
	msg, err := net.RequestDN(req, &node.NodeInfo.Node, us.logPrefix)
	if err != nil {
		if strings.Contains(err.Msg, "no handler") {
			AddError(node.NodeInfo.Id)
		}
		node.NodeInfo.SetERR()
		return nil, errors.New(err.Msg)
	} else {
		resp, ok := msg.(*pkt.UploadShard2CResp)
		if !ok {
			node.NodeInfo.SetERR()
			return nil, errors.New("RETURN ERR MSGTYPE")
		} else {
			if resp.RES == DN_RES_OK || resp.RES == DN_RES_VNF_EXISTS {
				return resp, nil
			} else {
				node.NodeInfo.SetERR()
				if resp.RES == DN_RES_NO_SPACE {
					AddError(node.NodeInfo.Id)
				}
				return nil, fmt.Errorf("RETURN ERR %d", resp.RES)
			}
		}
	}
}

func (us *UploadShard) DoSend() {
	defer us.DoFinish()
	node := us.uploadBlock.Queue.GetNodeStatExcluld(us.blkList)
	for {
		startTime := time.Now()
		req := us.MakeRequest(node)
		rtimes, ctlresp, err := us.GetToken(node)
		ctrtimes := time.Since(startTime).Milliseconds()
		if err != nil {
			us.retrytimes++
			node.DecCount()
			if us.parent.IsCancle() {
				logrus.Errorf("[UploadShard]%sGetNodeCapacity:%s,%s to %d,retry %d times,take times %d ms\n",
					us.logPrefix, err, base58.Encode(req.VHF), node.NodeInfo.Id, rtimes, ctrtimes)
				break
			}
			n := us.uploadBlock.Queue.GetNodeStatExcluld(us.blkList)
			logrus.Errorf("[UploadShard]%sGetNodeCapacity:%s,%s to %d,retry %d times,take times %d ms,retry next node %d\n",
				us.logPrefix, err, base58.Encode(req.VHF), node.NodeInfo.Id, rtimes, ctrtimes, n.NodeInfo.Id)
			node = n
			continue
		}
		node.NodeInfo.SetOK(ctrtimes)
		if us.parent.IsCancle() {
			break
		}
		req.AllocId = ctlresp.AllocId
		resp, err1 := us.SendShard(node, req)
		times := time.Since(startTime).Milliseconds()
		if err1 != nil {
			us.retrytimes++
			node.DecCount()
			if us.parent.IsCancle() {
				logrus.Errorf("[UploadShard]%sSendShard:%s,%s to %d,Gettoken retry %d times,take times %d ms\n",
					us.logPrefix, err1, base58.Encode(req.VHF), node.NodeInfo.Id, rtimes, times)
				break
			}
			n := us.uploadBlock.Queue.GetNodeStatExcluld(us.blkList)
			logrus.Errorf("[UploadShard]%sSendShard:%s,%s to %d,Gettoken retry %d times,take times %d ms,retry next node %d\n",
				us.logPrefix, err1, base58.Encode(req.VHF), node.NodeInfo.Id, rtimes, times, n.NodeInfo.Id)
			node = n
			continue
		}
		us.res.DNSIGN = resp.DNSIGN
		us.res.NODE = node.NodeInfo
		logrus.Infof("[UploadShard]%sSendShard:RETURN OK %d,%s to %d,Gettoken retry %d times,take times %d/%d ms\n",
			us.logPrefix, resp.RES, base58.Encode(req.VHF), node.NodeInfo.Id, rtimes, ctrtimes, times)
		AddShardOK(times)
		break
	}
}
