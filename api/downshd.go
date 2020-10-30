package api

import (
	"bytes"
	"crypto/md5"
	"errors"
	"io/ioutil"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/mr-tron/base58"
	"github.com/sirupsen/logrus"
	"github.com/yottachain/YTCoreService/codec"
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

type DownLoadShards struct {
	sync.RWMutex
	coder     *codec.ErasureDecoder
	cancel    *int32
	logPrefix string
	okSign    chan int
	ERR       atomic.Value
}

func NewDownLoad(logpre string, chansize int) *DownLoadShards {
	dns := &DownLoadShards{cancel: new(int32), logPrefix: logpre}
	if chansize > 0 {
		dns.okSign = make(chan int, chansize)
	}
	*dns.cancel = 0
	return dns
}

func (me *DownLoadShards) WaitDownload(chansize int) (*codec.EncryptedBlock, error) {
	for ii := 0; ii < chansize; ii++ {
		<-me.okSign
		if me.IsCancle() {
			break
		}
	}
	err := me.ERR.Load()
	if err != nil {
		return nil, err.(error)
	}
	if me.coder.IsOK() {
		return me.coder.GetEncryptedBlock(), nil
	} else {
		return nil, errors.New("shards is not enough")
	}
}

func (me *DownLoadShards) IsCancle() bool {
	return atomic.LoadInt32(me.cancel) == 1
}

func (me *DownLoadShards) CreateErasureDecoder(size int64, chansize int) error {
	me.okSign = make(chan int, chansize)
	c, err := codec.NewErasureDecoder(size)
	if err != nil {
		return err
	}
	me.coder = c
	return nil
}

func (me *DownLoadShards) OnResponse(data []byte) {
	me.Lock()
	defer me.Unlock()
	if me.coder == nil {
		return
	}
	b, err := me.coder.AddShard(data)
	if err != nil {
		logrus.Errorf("[DownloadShard]ErasureDecoder AddShard ERR:%s\n", me.logPrefix, err)
		me.ERR.Store(err)
		atomic.StoreInt32(me.cancel, 1)
		return
	}
	if b {
		atomic.StoreInt32(me.cancel, 1)
	}
}

type DownLoadShardInfo struct {
	DWNS       *DownLoadShards
	NodeInfo   *net.Node
	VHF        []byte
	RetryTimes int
	Path       string
}

func NewDownLoadShardInfo(n *pkt.DownloadBlockInitResp_NList_Ns, v []byte, rt int, d *DownLoadShards, path string) *DownLoadShardInfo {
	if n.Id == nil || n.Nodeid == nil || n.Pubkey == nil || n.Addrs == nil || len(n.Addrs) == 0 {
		logrus.Errorf("[DownloadShard]%DownLoad ERR,Nodeinfo is null\n", d.logPrefix, base58.Encode(v))
		return nil
	}
	if v == nil || len(v) != 16 {
		logrus.Errorf("[DownloadShard]%DownLoad ERR,VHF is null\n", d.logPrefix, base58.Encode(v))
		return nil
	}
	node := &net.Node{Id: *n.Id, Nodeid: *n.Nodeid, Pubkey: *n.Pubkey, Addrs: n.Addrs}
	info := &DownLoadShardInfo{DWNS: d, NodeInfo: node, VHF: v, RetryTimes: rt, Path: path}
	return info
}

func (me *DownLoadShardInfo) Verify(data []byte) []byte {
	if data == nil || len(data) == 0 {
		logrus.Errorf("[DownloadShard]%sVerify shard ERR,%s Non-existent,from %d\n",
			me.DWNS.logPrefix, base58.Encode(me.VHF), me.NodeInfo.Id)
		return nil
	}
	size := len(data)
	if size < env.PFL {
		logrus.Errorf("[DownloadShard]%sVerify shard %s ERR,Invalid data len %d,from %d\n",
			me.DWNS.logPrefix, base58.Encode(me.VHF), size, me.NodeInfo.Id)
		return nil
	}
	if size > env.PFL {
		data = data[0:env.PFL]
	}
	md5Digest := md5.New()
	md5Digest.Write(data)
	newvhf := md5Digest.Sum(nil)
	if bytes.Equal(me.VHF, newvhf) {
		logrus.Infof("[DownloadShard]%sDownload %s OK,from %d\n", me.DWNS.logPrefix, base58.Encode(me.VHF), me.NodeInfo.Id)
		me.DWNS.OnResponse(data)
		if me.Path != "" {
			ioutil.WriteFile(me.Path+strconv.Itoa(int(data[0])), data, 0644)
		}
		return data
	} else {
		logrus.Errorf("[DownloadShard]%sVerify shard inconsistency ERR,Request %s return %s data,from %d\n",
			me.DWNS.logPrefix, base58.Encode(me.VHF), base58.Encode(newvhf), me.NodeInfo.Id)
		return nil
	}
}

func (self *DownLoadShardInfo) DoFinish() {
	env.TracePanic("[DownloadShard]")
	SHARD_DOWN_CH <- 1
	self.DWNS.okSign <- 0
}

func (me *DownLoadShardInfo) Download() []byte {
	defer me.DoFinish()
	req := &pkt.DownloadShardReq{VHF: me.VHF}
	times := 0
	var msg proto.Message
	for {
		m, err := net.RequestDN(req, me.NodeInfo, me.DWNS.logPrefix)
		if err != nil {
			if atomic.LoadInt32(me.DWNS.cancel) == 1 {
				return nil
			}
			logrus.Errorf("[DownloadShard]%sDownload ERR,%s from %d\n", me.DWNS.logPrefix, base58.Encode(me.VHF), me.NodeInfo.Id)
			if err.Code == pkt.CONN_ERROR {
				time.Sleep(time.Duration(1) * time.Second)
			} else {
				times++
			}
			if times >= me.RetryTimes {
				return nil
			}
		} else {
			msg = m
			break
		}
	}
	resp, ok := msg.(*pkt.DownloadShardResp)
	if !ok {
		logrus.Errorf("[DownloadShard]%sRETURN ERR MSG,%s from %d\n", me.DWNS.logPrefix, base58.Encode(me.VHF), me.NodeInfo.Id)
		return nil
	}
	return me.Verify(resp.Data)
}
