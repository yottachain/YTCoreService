package api

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/mr-tron/base58/base58"
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

func StartUploadShard(upblk *UploadBlock, shd *codec.Shard, shdid int32, wg *sync.WaitGroup) *UploadShardResult {
	upshd := &UploadShard{uploadBlock: upblk, shard: shd, shardId: shdid, retrytimes: 0, WG: wg}
	upshd.logPrefix = fmt.Sprintf("[%s][%d][%d]", upblk.UPOBJ.VNU.Hex(), upblk.ID, shdid)
	upshd.res = &UploadShardResult{SHARDID: shdid, VHF: shd.VHF}
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
	WG          *sync.WaitGroup
}

func (self *UploadShard) DoFinish() {
	SHARD_UP_CH <- 1
	self.WG.Done()
	if r := recover(); r != nil {
		logrus.Errorf("[UploadShard]%sERR:%s\n", self.logPrefix, r)
	}
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

func (self *UploadShard) GetToken(node *NodeStatWOK) (*pkt.GetNodeCapacityResp, error) {
	ctlreq := &pkt.GetNodeCapacityReq{StartTime: uint64(self.uploadBlock.STime),
		RetryTimes: uint32(self.retrytimes)}
	msg, err := net.RequestDN(ctlreq, &node.NodeInfo.Node, self.logPrefix)
	if err != nil {
		node.NodeInfo.SetERR()
		return nil, errors.New("COMM_ERROR")
	} else {
		resp, ok := msg.(*pkt.GetNodeCapacityResp)
		if !ok {
			node.NodeInfo.SetERR()
			return nil, errors.New("RESP_INVALID_MSG")
		}
		if resp.Writable && resp.AllocId != "" {
			return resp, nil
		} else {
			node.NodeInfo.SetBusy()
			return nil, errors.New("NO_TOKEN")
		}
	}
}

func (self *UploadShard) SendShard(node *NodeStatWOK, req *pkt.UploadShardReq) (*pkt.UploadShard2CResp, error) {
	msg, err := net.RequestDN(req, &node.NodeInfo.Node, self.logPrefix)
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
	node := self.uploadBlock.Queue.GetNodeStat()
	for {
		startTime := time.Now()
		req := self.MakeRequest(node)
		ctlresp, err := self.GetToken(node)
		ctrtimes := time.Now().Sub(startTime).Milliseconds()
		if err != nil {
			self.retrytimes++
			node.DecCount()
			n := self.uploadBlock.Queue.GetNodeStat()
			logrus.Debugf("[UploadShard]%sGetNodeCapacity:%s,%s to %d,take times %d ms,retry next node %d\n",
				self.logPrefix, err, base58.Encode(req.VHF), node.NodeInfo.Id, ctrtimes, n.NodeInfo.Id)
			node = n
			continue
		}
		req.AllocId = ctlresp.AllocId
		resp, err1 := self.SendShard(node, req)
		times := time.Now().Sub(startTime).Milliseconds()
		if err1 != nil {
			self.retrytimes++
			node.DecCount()
			n := self.uploadBlock.Queue.GetNodeStat()
			logrus.Errorf("[UploadShard]%sSendShard:%s,%s to %d,take times %d ms,retry next node %d\n",
				self.logPrefix, err1, base58.Encode(req.VHF), node.NodeInfo.Id, times, n.NodeInfo.Id)
			node = n
			continue
		}
		node.NodeInfo.SetOK(times)
		self.res.DNSIGN = resp.DNSIGN
		self.res.NODEID = node.NodeInfo.Id
		logrus.Infof("[UploadShard]%sSendShard:RETURN OK %d,%s to %d,take times %d/%d ms\n",
			self.logPrefix, resp.RES, base58.Encode(req.VHF), node.NodeInfo.Id, ctrtimes, times)
		break
	}
}