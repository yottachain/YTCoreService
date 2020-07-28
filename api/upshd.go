package api

import (
	"errors"
	"fmt"
	"time"

	"github.com/mr-tron/base58/base58"
	"github.com/sirupsen/logrus"
	"github.com/yottachain/YTCoreService/codec"
	"github.com/yottachain/YTCoreService/env"
	"github.com/yottachain/YTCoreService/net"
	"github.com/yottachain/YTCoreService/pkt"
)

var SHARD_ROUTINE_CH chan int

func InitShardRoutinePool() {
	SHARD_ROUTINE_CH = make(chan int, env.UploadShardThreadNum)
	for ii := 0; ii < env.UploadShardThreadNum; ii++ {
		SHARD_ROUTINE_CH <- 1
	}
}

func StartUploadShard(upblk *UploadBlock, shd *codec.Shard, shdid int32) *UploadShardResult {
	upshd := &UploadShard{uploadBlock: upblk, shard: shd, shardId: shdid, retrytimes: 0}
	upshd.logPrefix = fmt.Sprintf("[%s][%d][%d]", upblk.VNU.Hex(), upblk.ID, shdid)
	upshd.res = &UploadShardResult{SHARDID: shdid, VHF: shd.VHF}
	<-SHARD_ROUTINE_CH
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
	NODEID  int32
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
}

func (self *UploadShard) DoFinish() {
	SHARD_ROUTINE_CH <- 1
	if r := recover(); r != nil {
		logrus.Tracef("%sERR:%s\n", self.logPrefix, r)
	}
}

func (self *UploadShard) MakeRequest(ns *NodeStat) *pkt.UploadShardReq {
	return &pkt.UploadShardReq{
		SHARDID:  self.shardId,
		BPDID:    ns.SnId(),
		BPDSIGN:  []byte(ns.sign),
		DAT:      self.shard.Data,
		VHF:      self.shard.VHF,
		USERSIGN: []byte(self.uploadBlock.Sign),
	}
}

func (self *UploadShard) GetToken(node *NodeStat) (*pkt.GetNodeCapacityResp, int, error) {
	ctlreq := &pkt.GetNodeCapacityReq{StartTime: uint64(self.uploadBlock.STime),
		RetryTimes: uint32(self.retrytimes)}
	for ii := 0; ii < env.TokenRetryTimes; ii++ {
		msg, err := net.RequestDN(ctlreq, &node.Node, self.logPrefix)
		if err != nil {
			node.SetERR()
			return nil, ii, errors.New("COMM_ERROR")
		} else {
			resp, ok := msg.(*pkt.GetNodeCapacityResp)
			if !ok {
				logrus.Warnf("%sUpload.GetNodeCapacity:RESP_INVALID_MSG,to %d\n", self.logPrefix, node.Id)
				continue
			}
			if resp.Writable && resp.AllocId != "" {
				return resp, ii, nil
			} else {
				logrus.Warnf("%sUpload.GetNodeCapacity:NO_TOKEN,to %d\n", self.logPrefix, node.Id)
				continue
			}
		}
	}
	node.SetBusy()
	return nil, env.TokenRetryTimes, errors.New("NO_TOKEN")
}

func (self *UploadShard) SendShard(node *NodeStat, req *pkt.UploadShardReq) (*pkt.UploadShard2CResp, error) {
	msg, err := net.RequestDN(req, &node.Node, self.logPrefix)
	if err != nil {
		node.SetERR()
		return nil, errors.New("COMM_ERROR")
	} else {
		resp, ok := msg.(*pkt.UploadShard2CResp)
		if !ok {
			node.SetERR()
			return nil, errors.New("RETURN ERR MSGTYPE")
		} else {
			if resp.RES == DN_RES_OK || resp.RES == DN_RES_VNF_EXISTS {
				return resp, nil
			} else {
				node.SetERR()
				if resp.RES == DN_RES_NO_SPACE {
					AddError(node.Id)
				}
				return nil, fmt.Errorf("RETURN ERR %d", resp.RES)
			}
		}
	}
}

func (self *UploadShard) DoSend() {
	defer self.DoFinish()
	node := self.uploadBlock.Queue.GetNodeStat()
	for {
		startTime := time.Now()
		req := self.MakeRequest(node)
		ctlresp, retrys, err := self.GetToken(node)
		ctrtimes := time.Now().Sub(startTime).Milliseconds()
		if err != nil {
			self.retrytimes++
			self.uploadBlock.Queue.DecCount(node)
			n := self.uploadBlock.Queue.GetNodeStat()
			logrus.Errorf("%sUpload.GetNodeCapacity:%s,%s to %d,Request %d times,take times %d ms,retry next node %d\n",
				self.logPrefix, err, base58.Encode(req.VHF), node.Id, retrys, ctrtimes, n.Id)
			node = n
			continue
		}
		req.AllocId = ctlresp.AllocId
		resp, err1 := self.SendShard(node, req)
		times := time.Now().Sub(startTime).Milliseconds()
		if err1 != nil {
			self.retrytimes++
			self.uploadBlock.Queue.DecCount(node)
			n := self.uploadBlock.Queue.GetNodeStat()
			logrus.Errorf("%sUpload.SendShard:%s,%s to %d,take times %d ms,retry next node %d\n",
				self.logPrefix, err1, base58.Encode(req.VHF), node.Id, times, n.Id)
			node = n
			continue
		}
		node.SetOK(times)
		self.res.DNSIGN = resp.DNSIGN
		self.res.NODEID = node.Id
		logrus.Infof("%sUpload.SendShard:RETURN OK %d,%s to %d,take times %d ms\n",
			self.logPrefix, resp.RES, base58.Encode(req.VHF), node.Id, times)
		DecMem(self.shard)
		break
	}
}
