package api

import (
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/yottachain/YTCoreService/env"
	"github.com/yottachain/YTCoreService/net"
)

type NodeList struct {
	sync.RWMutex
	list       map[int32]*NodeStat
	updateTime int64
	resetSign  *int32
}

func (n *NodeList) UpdateNodeList(ns map[int32]*NodeStat) {
	reset := atomic.LoadInt32(n.resetSign)
	if reset == 0 {
		if time.Now().Unix()-n.updateTime > int64(30*60) {
			reset = 1
		}
	}
	n.Lock()
	if reset == 0 {
		for k, v := range n.list {
			if len(ns) >= env.PNN {
				break
			}
			_, ok := ns[k]
			if !ok {
				ns[k] = v
			}
		}
	} else {
		atomic.StoreInt32(n.resetSign, 0)
		n.updateTime = time.Now().Unix()
	}
	n.list = ns
	n.Unlock()
}

func (n *NodeList) OrderNodeList() []*NodeStat {
	if env.ALLOC_MODE == 0 {
		return n.SortNodeList()
	} else if env.ALLOC_MODE == -1 {
		return n.ShuffleNodeList()
	} else {
		return n.P2pOrderNodeList()
	}
}

func (n *NodeList) GetNodeList() []*NodeStat {
	var nodes []*NodeStat
	n.RLock()
	for _, n := range n.list {
		nodes = append(nodes, n)
	}
	n.RUnlock()
	return nodes
}

func (n *NodeList) P2pOrderNodeList() []*NodeStat {
	nodes := n.GetNodeList()
	nmap := make(map[string]*NodeStat)
	var iids []string
	for _, n := range nodes {
		nmap[n.Nodeid] = n
		iids = append(iids, n.Nodeid)
	}
	oids := iids
	//cliM "github.com/yottachain/YTHost/ClientManage"
	/*
		startTime := time.Now()
		randlen := int(float32(3*env.ALLOC_MODE) / 17)
		peerAddrs,err := cliM.Manager.GetOptNodes(iids, env.ALLOC_MODE, randlen)
		interval := time.Now().Sub(startTime).Milliseconds()
		if err!=nil{
			logrus.Errorf("[GetOptNodes]Err:%s,take times %d ms\n",err,interval)
		}
		oids= cliM.PA2ids(peerAddrs...)
		logrus.Infof("[GetOptNodes]OK,%d/%d,take times %d ms\n",len(oids),len(iids),interval)
	*/
	nnodes := []*NodeStat{}
	for _, id := range oids {
		n, ok := nmap[id]
		if ok {
			nnodes = append(nnodes, n)
		}
	}
	if len(nnodes) == 0 {
		logrus.Errorf("[GetOptNodes]Return 0 nodes\n")
		s := &NodeStatOrder{Nodes: nodes, RandMode: false}
		sort.Sort(s)
		return nodes
	} else {
		return nnodes
	}
}

func (n *NodeList) ShuffleNodeList() []*NodeStat {
	nodes := n.GetNodeList()
	s := &NodeStatOrder{Nodes: nodes, RandMode: true}
	sort.Sort(s)
	return nodes
}

func (n *NodeList) SortNodeList() []*NodeStat {
	nodes := n.GetNodeList()
	s := &NodeStatOrder{Nodes: nodes, RandMode: false}
	sort.Sort(s)
	return nodes
}

type NodeStat struct {
	net.Node
	okDelayTimes *int64
	okTimes      *int64
	errTimes     *int64
	busyTimes    *int64
	resetTime    int64
	snid         int32
	timestamp    int64
	sign         string
}

var BUSYTIMES int64 = int64(net.Writetimeout)
var ERRTIMES int64 = BUSYTIMES + 10000

func NewNodeStat(id int32, timestamp int64, sign string) *NodeStat {
	ns := &NodeStat{okDelayTimes: new(int64), okTimes: new(int64), errTimes: new(int64), busyTimes: new(int64)}
	*ns.okDelayTimes = 0
	*ns.okTimes = 0
	*ns.errTimes = 0
	*ns.busyTimes = 0
	ns.resetTime = time.Now().Unix()
	ns.snid = id
	ns.timestamp = timestamp
	ns.sign = sign
	return ns
}

func (n *NodeStat) SetNodeInfo(node *net.Node) {
	n.Id = node.Id
	n.Nodeid = node.Nodeid
	n.Addrs = node.Addrs
	n.Pubkey = node.Pubkey
}

func (n *NodeStat) SnId() int32 {
	return n.snid
}

func (n *NodeStat) SetERR() {
	atomic.AddInt64(n.errTimes, 1)
}

func (n *NodeStat) SetBusy() {
	atomic.AddInt64(n.busyTimes, 1)
}

func (n *NodeStat) SetOK(t int64) {
	atomic.AddInt64(n.okTimes, 1)
	atomic.AddInt64(n.okDelayTimes, t)
}

func (n *NodeStat) RandDelayTimes(size int) int {
	return rand.Intn(size * 100)
}

func (n *NodeStat) GetDelayTimes() int64 {
	oktimes := atomic.LoadInt64(n.okDelayTimes)
	count := atomic.LoadInt64(n.okTimes)
	errcount := atomic.LoadInt64(n.errTimes)
	busycount := atomic.LoadInt64(n.busyTimes)
	if count == 0 {
		if errcount == 0 && busycount == 0 {
			return 0
		} else {
			return (BUSYTIMES*busycount + ERRTIMES*errcount) / (busycount + errcount)
		}
	} else {
		times := oktimes / count
		if times > ERRTIMES {
			return times
		} else {
			return (oktimes + ERRTIMES*errcount + BUSYTIMES*busycount) / (count + errcount + busycount)
		}
	}
}

type NodeStatOrder struct {
	Nodes    []*NodeStat
	RandMode bool
}

func (ns NodeStatOrder) Len() int {
	return len(ns.Nodes)
}

func (ns NodeStatOrder) Swap(i, j int) {
	ns.Nodes[i], ns.Nodes[j] = ns.Nodes[j], ns.Nodes[i]
}

func (ns NodeStatOrder) Less(i, j int) bool {
	if ns.RandMode {
		size := ns.Len()
		i1 := ns.Nodes[i].RandDelayTimes(size)
		i2 := ns.Nodes[j].RandDelayTimes(size)
		return i1 < i2
	} else {
		return ns.Nodes[i].GetDelayTimes() < ns.Nodes[j].GetDelayTimes()
	}
}
