package api

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/aurawing/eos-go/btcsuite/btcutil/base58"
	"github.com/sirupsen/logrus"
	"github.com/yottachain/YTCoreService/codec"
	"github.com/yottachain/YTCoreService/env"
	"github.com/yottachain/YTCoreService/net"
	"github.com/yottachain/YTCoreService/pkt"
)

var SHARD_UP_CH chan int
var ShardRoutineLock sync.RWMutex

func InitShardUpPool() {
	SHARD_UP_CH = make(chan int, env.UploadShardThreadNum)
	for ii := 0; ii < env.UploadShardThreadNum; ii++ {
		SHARD_UP_CH <- 1
	}
	InitSendStat()
}

func StartUploadShard(upblk *UploadBlock, shd *codec.Shard, shdid int32, wg *sync.WaitGroup, ids []int32, lrc2 bool) *UploadShardResult {
	upshd := &UploadShard{uploadBlock: upblk, shardData: shd.Data, shardVHF: shd.VHF, shardId: shdid, retrytimes: 0, WG: wg}
	if lrc2 {
		upshd.logPrefix = fmt.Sprintf("[%s][%d][%d-1]", upblk.UPOBJ.VNU.Hex(), upblk.ID, shdid)
	} else {
		upshd.logPrefix = fmt.Sprintf("[%s][%d][%d-0]", upblk.UPOBJ.VNU.Hex(), upblk.ID, shdid)
	}
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
const DN_RES_BUSY = 112

type UploadShardResult struct {
	SHARDID int32
	NODE    *NodeStat
	VHF     []byte
	DNSIGN  string
}

type UploadShard struct {
	uploadBlock *UploadBlock
	shardData   []byte
	shardVHF    []byte
	shardId     int32
	logPrefix   string
	res         *UploadShardResult
	retrytimes  uint32
	blkList     []int32
	WG          *sync.WaitGroup
}

func (us *UploadShard) DoFinish() {
	if r := recover(); r != nil {
		env.TraceError("[UploadShard]")
	}
	SHARD_UP_CH <- 1
	us.WG.Done()
}

func (us *UploadShard) MakeRequest(ns *NodeStatWOK) *pkt.UploadShardReq {
	return &pkt.UploadShardReq{
		SHARDID:  us.shardId,
		BPDSIGN:  []byte(ns.NodeInfo.sign),
		DAT:      us.shardData,
		VHF:      us.shardVHF,
		USERSIGN: []byte(us.uploadBlock.UPOBJ.Sign),
		HASHID:   us.uploadBlock.STime + int64(us.shardId),
	}
}

func (us *UploadShard) GetToken(node *NodeStatWOK) (int, *pkt.GetNodeCapacityResp, error) {
	//logrus.Tracef("[UploadShard]%sGetToken from %d......\n", us.logPrefix, node.NodeInfo.Id)
	ctlreq := &pkt.GetNodeCapacityReq{StartTime: uint64(us.uploadBlock.STime),
		RetryTimes: uint32(us.retrytimes)}
	times := 0
	for {
		msg, err := net.RequestDN(ctlreq, &node.NodeInfo.Node, true)
		times++
		if err != nil {
			if strings.Contains(err.Msg, "no handler") {
				AddError(node.NodeInfo.Id)
			}
			node.NodeInfo.SetERR(err.Code == pkt.COMM_ERROR)
			return times, nil, errors.New(err.Msg)
		} else {
			resp, ok := msg.(*pkt.GetNodeCapacityResp)
			if !ok {
				node.NodeInfo.SetERR(false)
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

func (us *UploadShard) SendShard(node *NodeStatWOK, req *pkt.UploadShardReq) (*pkt.UploadShard2CResp, *pkt.ErrorMessage) {
	//logrus.Tracef("[UploadShard]%sSendShard %s to %d......\n", us.logPrefix, base58.Encode(req.VHF), node.NodeInfo.Id)
	msg, err := net.RequestDN(req, &node.NodeInfo.Node, false)
	if err != nil {
		if strings.Contains(err.Msg, "no handler") {
			AddError(node.NodeInfo.Id)
		}
		return nil, err
	} else {
		resp, ok := msg.(*pkt.UploadShard2CResp)
		if !ok {
			return nil, pkt.NewErrorMsg(pkt.SERVER_ERROR, "RETURN ERR MSGTYPE")
		} else {
			if resp.RES == DN_RES_OK || resp.RES == DN_RES_VNF_EXISTS {
				return resp, nil
			} else {
				if resp.RES == DN_RES_NO_SPACE {
					AddError(node.NodeInfo.Id)
				}
				return nil, pkt.NewErrorMsg(pkt.SERVER_ERROR, fmt.Sprintf("RETURN ERR %d", resp.RES))
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
		rtimes := 0
		ctrtimes := 0
		if env.UploadShardRetryTimes > 0 {
			rtimes, ctlresp, err := us.GetToken(node)
			ctrtimes := time.Since(startTime).Milliseconds()
			if err != nil {
				us.retrytimes++
				node.DecCount()
				n := us.uploadBlock.Queue.GetNodeStatExcluld(us.blkList)
				logrus.Infof("[UploadShard]%sGetNodeCapacity:%s,%s to %d,retry %d times,take times %d ms,retry next node %d\n",
					us.logPrefix, err, base58.Encode(req.VHF), node.NodeInfo.Id, rtimes, ctrtimes, n.NodeInfo.Id)
				node = n
				continue
			}
			req.AllocId = ctlresp.AllocId
		}
		resp, err1 := us.SendShard(node, req)
		times := time.Since(startTime).Milliseconds()
		if err1 != nil {
			node.NodeInfo.SetERR(err1.Code == pkt.COMM_ERROR)
			us.retrytimes++
			node.DecCount()
			n := us.uploadBlock.Queue.GetNodeStatExcluld(us.blkList)
			logrus.Infof("[UploadShard]%sSendShard:%s,%s to %d,Gettoken retry %d times,take times %d/%d ms,retry next node %d\n",
				us.logPrefix, err1, base58.Encode(req.VHF), node.NodeInfo.Id, rtimes, ctrtimes, times, n.NodeInfo.Id)
			node = n
			continue
		} else {
			node.NodeInfo.SetOK(times - int64(ctrtimes))
		}
		us.res.DNSIGN = resp.DNSIGN
		us.res.NODE = node.NodeInfo
		logrus.Debugf("[UploadShard]%sSendShard:RETURN OK %d,%s to %d,Gettoken retry %d times,take times %d/%d ms\n",
			us.logPrefix, resp.RES, base58.Encode(req.VHF), node.NodeInfo.Id, rtimes, ctrtimes, times)
		break
	}
}
