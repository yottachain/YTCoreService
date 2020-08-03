package api

import (
	"crypto/sha256"
	"pkg/bytes"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/mr-tron/base58"
	"github.com/sirupsen/logrus"
	"github.com/yottachain/YTCoreService/env"
	"github.com/yottachain/YTCoreService/net"
	"github.com/yottachain/YTCoreService/pkt"
)

var SHARD_DOWN_CH chan int

func InitShardDownPool() {
	SHARD_DOWN_CH = make(chan int, env.DownloadThread)
	for ii := 0; ii < env.DownloadThread; ii++ {
		SHARD_DOWN_CH <- 1
	}
}

type DownLoadShardInfo struct {
	NodeInfo   *net.Node
	VHF        []byte
	RetryTimes int
	logPrefix  string
	Data       []byte
}

func NewDownLoadShardInfo(n *pkt.DownloadBlockInitResp_NList_Ns, v []byte, s string, rt int) *DownLoadShardInfo {
	if n.Id == nil || n.Nodeid == nil || n.Pubkey == nil || n.Addrs == nil || len(n.Addrs) == 0 {
		logrus.Errorf("[DownloadShard]%DownLoad ERR,Nodeinfo is null\n", s, base58.Encode(v))
		return nil
	}
	node := &net.Node{Id: *n.Id, Nodeid: *n.Nodeid, Pubkey: *n.Pubkey, Addrs: n.Addrs}
	info := &DownLoadShardInfo{NodeInfo: node, VHF: v, logPrefix: s, RetryTimes: rt}
	return info
}

func (me *DownLoadShardInfo) Verify(data []byte) {
	if data == nil || len(data) == 0 {
		logrus.Errorf("[DownloadShard]%sVerify shard ERR,%s Non-existent,from %d\n",
			me.logPrefix, base58.Encode(me.VHF), me.NodeInfo.Id)
		return
	}
	size := len(data)
	if size < env.PFL {
		logrus.Errorf("[DownloadShard]%sVerify shard ERR,%s Invalid data,len=%d,from %d\n",
			me.logPrefix, base58.Encode(me.VHF), size, me.NodeInfo.Id)
		return
	}
	if size > env.PFL {
		data = data[0:env.PFL]
	}
	sha256Digest := sha256.New()
	sha256Digest.Write(data)
	newvhf := sha256Digest.Sum(nil)
	if bytes.Equal(me.VHF, newvhf) {
		me.Data = data
		logrus.Infof("[DownloadShard]%sDownload OK,%s from %d\n", me.logPrefix, base58.Encode(me.VHF), me.NodeInfo.Id)
	} else {
		logrus.Errorf("[DownloadShard]%sVerify shard inconsistency ERR,Request %s return %s data,from %d\n",
			me.logPrefix, base58.Encode(me.VHF), base58.Encode(newvhf), me.NodeInfo.Id)
	}
}

func (me *DownLoadShardInfo) Download() {
	req := &pkt.DownloadShardReq{VHF: me.VHF}
	times := 0
	var msg proto.Message
	for {
		m, err := net.RequestDN(req, me.NodeInfo, me.logPrefix)
		if err != nil {
			logrus.Errorf("[DownloadShard]%sDownload ERR,%s from %d\n", me.logPrefix, base58.Encode(me.VHF), me.NodeInfo.Id)
			times++
			if times >= me.RetryTimes {
				return
			}
			time.Sleep(time.Duration(env.DN_RETRY_WAIT) * time.Second)
		} else {
			msg = m
			break
		}
	}
	resp, ok := msg.(*pkt.DownloadShardResp)
	if !ok {
		logrus.Errorf("[DownloadShard]%sRETURN ERR MSG,%s from %d\n", me.logPrefix, base58.Encode(me.VHF), me.NodeInfo.Id)
		return
	}
	me.Verify(resp.Data)
}
