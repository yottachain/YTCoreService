package api

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/aurawing/eos-go/btcsuite/btcutil/base58"
	"github.com/sirupsen/logrus"
	"github.com/yottachain/YTCoreService/codec"
	"github.com/yottachain/YTCoreService/env"
	"github.com/yottachain/YTCoreService/net"
	"github.com/yottachain/YTCoreService/pkt"
)

func StartUploadShardEx(upblk *UploadBlock, shd *codec.Shard, shdid int32, us *UpLoadShards, ids []int32, lrc2 bool) {
	upshd := &UploadShardEx{uploadBlock: upblk, shardData: shd.Data, shardVHF: shd.VHF, shardId: shdid, retrytimes: 0, parent: us}
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

type UploadShardEx struct {
	uploadBlock *UploadBlock
	shardData   []byte
	shardVHF    []byte
	shardId     int32
	logPrefix   string
	res         *UploadShardResult
	retrytimes  uint32
	blkList     []int32
	parent      *UpLoadShards
}

func (us *UploadShardEx) DoFinish() {
	if r := recover(); r != nil {
		env.TraceError("[UploadShard]")
	}
	SHARD_UP_CH <- 1
	us.parent.OnResponse(us.res)
}

func (us *UploadShardEx) MakeRequest(ns *NodeStatWOK) *pkt.UploadShardReq {
	return &pkt.UploadShardReq{
		SHARDID:  us.shardId,
		BPDID:    ns.NodeInfo.SnId(),
		BPDSIGN:  []byte(ns.NodeInfo.sign),
		DAT:      us.shardData,
		VHF:      us.shardVHF,
		USERSIGN: []byte(us.uploadBlock.UPOBJ.Sign),
		HASHID:   us.uploadBlock.STime + int64(us.shardId),
	}
}

func (us *UploadShardEx) GetToken(node *NodeStatWOK) (int, *pkt.GetNodeCapacityResp, error) {
	logrus.Tracef("[UploadShard]%sGetToken from %d......\n", us.logPrefix, node.NodeInfo.Id)
	ctlreq := &pkt.GetNodeCapacityReq{StartTime: uint64(us.uploadBlock.STime),
		RetryTimes: uint32(us.retrytimes)}
	times := 0
	for {
		msg, err := net.RequestDN(ctlreq, &node.NodeInfo.Node)
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

func (us *UploadShardEx) SendShard(node *NodeStatWOK, req *pkt.UploadShardReq) (*pkt.UploadShard2CResp, error) {
	logrus.Tracef("[UploadShard]%sSendShard %s to %d......\n", us.logPrefix, base58.Encode(req.VHF), node.NodeInfo.Id)
	msg, err := net.RequestDN(req, &node.NodeInfo.Node)
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

func (us *UploadShardEx) DoSend() {
	defer us.DoFinish()
	node := us.uploadBlock.Queue.GetNodeStatExcluld(us.blkList)
	for {
		if us.parent.IsCancle() {
			break
		}
		startTime := time.Now()
		req := us.MakeRequest(node)
		rtimes, ctlresp, err := us.GetToken(node)
		ctrtimes := time.Since(startTime).Milliseconds()
		if err != nil {
			us.retrytimes++
			node.DecCount()
			if us.parent.IsCancle() {
				logrus.Infof("[UploadShard]%sGetNodeCapacity:%s,%s to %d,retry %d times,take times %d ms\n",
					us.logPrefix, err, base58.Encode(req.VHF), node.NodeInfo.Id, rtimes, ctrtimes)
				break
			}
			n := us.uploadBlock.Queue.GetNodeStatExcluld(us.blkList)
			logrus.Infof("[UploadShard]%sGetNodeCapacity:%s,%s to %d,retry %d times,take times %d ms,retry next node %d\n",
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
				logrus.Infof("[UploadShard]%sSendShard:%s,%s to %d,Gettoken retry %d times,take times %d ms\n",
					us.logPrefix, err1, base58.Encode(req.VHF), node.NodeInfo.Id, rtimes, times)
				break
			}
			n := us.uploadBlock.Queue.GetNodeStatExcluld(us.blkList)
			logrus.Infof("[UploadShard]%sSendShard:%s,%s to %d,Gettoken retry %d times,take times %d ms,retry next node %d\n",
				us.logPrefix, err1, base58.Encode(req.VHF), node.NodeInfo.Id, rtimes, times, n.NodeInfo.Id)
			node = n
			continue
		}
		us.res.DNSIGN = resp.DNSIGN
		us.res.NODE = node.NodeInfo
		logrus.Debugf("[UploadShard]%sSendShard:RETURN OK %d,%s to %d,Gettoken retry %d times,take times %d/%d ms\n",
			us.logPrefix, resp.RES, base58.Encode(req.VHF), node.NodeInfo.Id, rtimes, ctrtimes, times)
		break
	}
}