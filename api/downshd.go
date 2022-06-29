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
	coder      *codec.ErasureDecoder
	cancel     *int32
	logPrefix  string
	okSign     chan int
	ERR        atomic.Value
	shardcount int
}

func NewDownLoad(logpre string, chansize int, ids int) *DownLoadShards {
	dns := &DownLoadShards{cancel: new(int32), logPrefix: logpre}
	if chansize > 0 {
		dns.okSign = make(chan int, chansize)
	}
	*dns.cancel = 0
	dns.shardcount = ids
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
	NodeInfo2  *net.Node
	VHF        []byte
	RetryTimes int
	Path       string
}

func NewDownLoadShardInfo2(n *pkt.DownloadBlockInitResp2_Ns, n2 *pkt.DownloadBlockInitResp2_Ns, v []byte, rt int, d *DownLoadShards, path string) *DownLoadShardInfo {
	if v == nil || len(v) != 16 {
		logrus.Errorf("[DownloadShard]%sDownLoad %s ERR,VHF is null\n", d.logPrefix, base58.Encode(v))
		return nil
	}
	var node1, node2 *net.Node
	if n != nil {
		if n.Id == nil || n.Nodeid == nil || n.Pubkey == nil || n.Addrs == nil || len(n.Addrs) == 0 {
			logrus.Errorf("[DownloadShard]%sDownLoad %s ERR,Nodeinfo is null:%s\n", d.logPrefix, base58.Encode(v), n)
			return nil
		}
		node1 = &net.Node{Id: *n.Id, Nodeid: *n.Nodeid, Pubkey: *n.Pubkey, Addrs: n.Addrs}
	}
	if n2 != nil {
		if n2.Id == nil || n2.Nodeid == nil || n2.Pubkey == nil || n2.Addrs == nil || len(n2.Addrs) == 0 {
			logrus.Errorf("[DownloadShard]%sDownLoad %s ERR,Nodeinfo2 is null:%s\n", d.logPrefix, base58.Encode(v), n2)
			return nil
		}
		node2 = &net.Node{Id: *n2.Id, Nodeid: *n2.Nodeid, Pubkey: *n2.Pubkey, Addrs: n2.Addrs}
	}
	if node1 == nil && node2 != nil {
		return &DownLoadShardInfo{DWNS: d, NodeInfo: node2, VHF: v, RetryTimes: rt, Path: path}
	}
	if node1 != nil && node2 == nil {
		return &DownLoadShardInfo{DWNS: d, NodeInfo: node1, VHF: v, RetryTimes: rt, Path: path}
	}
	if node1 != nil && node2 != nil {
		return &DownLoadShardInfo{DWNS: d, NodeInfo: node1, NodeInfo2: node2, VHF: v, RetryTimes: rt, Path: path}
	}
	return nil
}

func NewDownLoadShardInfo(n *pkt.DownloadBlockInitResp_NList_Ns, v []byte, rt int, d *DownLoadShards, path string) *DownLoadShardInfo {
	if n.Id == nil || n.Nodeid == nil || n.Pubkey == nil || n.Addrs == nil || len(n.Addrs) == 0 {
		logrus.Errorf("[DownloadShard]%sDownLoad %s ERR,Nodeinfo is null:%s\n", d.logPrefix, base58.Encode(v), n)
		return nil
	}
	if v == nil || len(v) != 16 {
		logrus.Errorf("[DownloadShard]%sDownLoad %s ERR,VHF is null\n", d.logPrefix, base58.Encode(v))
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
	if me.DWNS.shardcount != 0 {
		index := data[0]
		if me.DWNS.shardcount == 17 {
			if index == 3 {
				logrus.Warnf("[DownloadShard]Activate LRC bug %d/%d\n", index, me.DWNS.shardcount)
				return nil
			}
		} else {
			if index == 4 || index == 5 {
				logrus.Warnf("[DownloadShard]Activate LRC bug %d/%d\n", index, me.DWNS.shardcount)
				return nil
			}
		}
	}
	md5Digest := md5.New()
	md5Digest.Write(data)
	newvhf := md5Digest.Sum(nil)
	if bytes.Equal(me.VHF, newvhf) {
		logrus.Debugf("[DownloadShard]%sDownload %s OK,from %d\n", me.DWNS.logPrefix, base58.Encode(me.VHF), me.NodeInfo.Id)
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

func (me *DownLoadShardInfo) DoFinish() {
	if r := recover(); r != nil {
		env.TraceError("[DownloadShard]")
	}
	SHARD_DOWN_CH <- 1
	me.DWNS.okSign <- 0
}

func (me *DownLoadShardInfo) Download() []byte {
	defer me.DoFinish()
	req := &pkt.DownloadShardReq{VHF: me.VHF}
	times := 0
	var msg proto.Message
	for {
		m, err := net.RequestDN(req, me.NodeInfo)
		if err != nil {
			logrus.Infof("[DownloadShard]%sDownload ERR:%s,%s from %d\n", me.DWNS.logPrefix, err.Msg, base58.Encode(me.VHF), me.NodeInfo.Id)
			if atomic.LoadInt32(me.DWNS.cancel) == 1 {
				return nil
			}
			if me.NodeInfo2 != nil {
				m, err = net.RequestDN(req, me.NodeInfo2)
				if err == nil {
					msg = m
					break
				} else {
					logrus.Infof("[DownloadShard]%sDownload ERR:%s,%s from %d\n", me.DWNS.logPrefix, err.Msg, base58.Encode(me.VHF), me.NodeInfo2.Id)
					if atomic.LoadInt32(me.DWNS.cancel) == 1 {
						return nil
					}
				}
			}
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
