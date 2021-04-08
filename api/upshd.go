package api

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/aurawing/eos-go/btcsuite/btcutil/base58"
	"github.com/sirupsen/logrus"
	"github.com/yottachain/YTCoreService/codec"
	"github.com/yottachain/YTCoreService/elk"
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

func StartUploadShard(upblk *UploadBlock, shd *codec.Shard, shdid int32, wg *sync.WaitGroup, ids []int32) *UploadShardResult {
	upshd := &UploadShard{uploadBlock: upblk, shard: shd, shardId: shdid, retrytimes: 0, WG: wg}
	upshd.logPrefix = fmt.Sprintf("[%s][%d][%d]", upblk.UPOBJ.VNU.Hex(), upblk.ID, shdid)
	upshd.res = &UploadShardResult{SHARDID: shdid, VHF: shd.VHF}
	upshd.blkList = ids
	<-SHARD_UP_CH
	go upshd.DoSend()
	return upshd.res
}

const DN_RES_OK = 0
const DN_RES_NETIOERR = 400
const DN_RES_BAD_REQUEST = 100
const DN_RES_NO_SPACE = 101
const DN_RES_VNF_EXISTS = 102
const DN_RES_CACHE_FILL = 105

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
	WG          *sync.WaitGroup
}

func (self *UploadShard) DoFinish() {
	env.TracePanic("[UploadShard]")
	SHARD_UP_CH <- 1
	self.WG.Done()
}

func (self *UploadShard) MakeRequest(ns *NodeStatWOK) *pkt.UploadShardReq {
	return &pkt.UploadShardReq{
		SHARDID:  self.shardId,
		BPDID:    ns.NodeInfo.SnId(),
		BPDSIGN:  []byte(ns.NodeInfo.sign),
		DAT:      self.shard.Data,
		VHF:      self.shard.VHF,
		USERSIGN: []byte(self.uploadBlock.UPOBJ.Sign),
	}
}

func (self *UploadShard) GetToken(node *NodeStatWOK) (int, *pkt.GetNodeCapacityResp, error) {
	ctlreq := &pkt.GetNodeCapacityReq{StartTime: uint64(self.uploadBlock.STime),
		RetryTimes: uint32(self.retrytimes)}
	times := 0
	for {
		msg, err := net.RequestDN(ctlreq, &node.NodeInfo.Node, self.logPrefix, true)
		times++
		if err != nil {
			node.NodeInfo.SetERR()
			return times, nil, errors.New("COMM_ERROR")
		} else {
			resp, ok := msg.(*pkt.GetNodeCapacityResp)
			if !ok {
				node.NodeInfo.SetERR()
				return times, nil, errors.New("RESP_INVALID_MSG")
			}
			if resp.Writable && resp.AllocId != "" {
				return times, resp, nil
			} else {
				if times >= env.UploadShardRetryTimes {
					return times, nil, errors.New("NO_TOKEN")
				}
			}
		}
	}
}

func (self *UploadShard) SendShard(node *NodeStatWOK, req *pkt.UploadShardReq) (*pkt.UploadShard2CResp, error) {
	msg, err := net.RequestDN(req, &node.NodeInfo.Node, self.logPrefix, false)
	if err != nil {
		node.NodeInfo.SetERR()
		return nil, errors.New("COMM_ERROR")
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

func (self *UploadShard) DoSend() {
	defer self.DoFinish()
	node := self.uploadBlock.Queue.GetNodeStatExcluld(self.blkList)
	for {
		startTime := time.Now()
		req := self.MakeRequest(node)
		rtimes, ctlresp, err := self.GetToken(node)
		ctrtimes := time.Now().Sub(startTime).Milliseconds()
		if err != nil {
			self.retrytimes++
			node.DecCount()
			n := self.uploadBlock.Queue.GetNodeStatExcluld(self.blkList)
			logrus.Errorf("[UploadShard]%sGetNodeCapacity:%s,%s to %d,retry %d times,take times %d ms,retry next node %d\n",
				self.logPrefix, err, base58.Encode(req.VHF), node.NodeInfo.Id, rtimes, ctrtimes, n.NodeInfo.Id)
			node = n
			continue
		}
		//node.NodeInfo.SetOK(ctrtimes / int64(rtimes))
		node.NodeInfo.SetOK(ctrtimes)
		req.AllocId = ctlresp.AllocId
		startSendTime := time.Now()
		resp, err1 := self.SendShard(node, req)
		sendTimes := time.Now().Sub(startSendTime).Milliseconds()
		times := time.Now().Sub(startTime).Milliseconds()

		stat := &elk.ElkLog{GetTokenTimes: ctrtimes/int64(rtimes), UpShardTimes: sendTimes, Id:node.NodeInfo.Id, Time:time.Now().Unix()}
		self.uploadBlock.UPOBJ.Eclinet1.AddLogAsync(stat)

		if err1 != nil {
			self.retrytimes++
			node.DecCount()
			n := self.uploadBlock.Queue.GetNodeStatExcluld(self.blkList)
			logrus.Errorf("[UploadShard]%sSendShard:%s,%s to %d,Gettoken retry %d times,take times %d ms,retry next node %d\n",
				self.logPrefix, err1, base58.Encode(req.VHF), node.NodeInfo.Id, rtimes, times, n.NodeInfo.Id)
			node = n
			continue
		}
		self.res.DNSIGN = resp.DNSIGN
		self.res.NODE = node.NodeInfo
		logrus.Infof("[UploadShard]%sSendShard:RETURN OK %d,%s to %d,Gettoken retry %d times,take times %d/%d ms\n",
			self.logPrefix, resp.RES, base58.Encode(req.VHF), node.NodeInfo.Id, rtimes, ctrtimes, times)
		break
	}
}
