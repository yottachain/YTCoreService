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

type DNQueue struct {
	sync.RWMutex
	nodes   chan *NodeStat
	closing *int32
	oklist  map[int32]int32
}

func NewDNQueue() *DNQueue {
	queue := &DNQueue{closing: new(int32), oklist: make(map[int32]int32)}
	queue.nodes = make(chan *NodeStat, env.Max_Shard_Count)
	go queue.PutNodeStatLoop()
	return queue
}

func (q *DNQueue) AddCount(n *NodeStat) bool {
	q.Lock()
	defer q.Unlock()
	count, ok := q.oklist[n.Id]
	if ok {
		if count >= int32(env.ShardNumPerNode) {
			return false
		} else {
			q.oklist[n.Id] = count + 1
		}
	} else {
		q.oklist[n.Id] = 1
	}
	return true
}

func (q *DNQueue) DecCount(n *NodeStat) {
	q.Lock()
	count, ok := q.oklist[n.Id]
	if ok {
		q.oklist[n.Id] = count - 1
	}
	q.Unlock()
}

func (q *DNQueue) Close() {
	atomic.StoreInt32(q.closing, 1)
}

func (q *DNQueue) PutNodeStatLoop() {
	for {
		sign := q.PutNodeStat()
		if sign == -1 {
			break
		} else if sign == 0 {
			logrus.Warnf("[PutNodeStat]Number of DN not enough,size:%d\n", DNList.Len())
			time.Sleep(time.Duration(15) * time.Second)
		}
	}
}

func (q *DNQueue) PutNodeStat() int {
	sign := 0
	nodes := DNList.OrderNodeList()
	for _, n := range nodes {
		if !q.AddCount(n) {
			continue
		}
		timeout := time.After(time.Second * 1)
		select {
		case q.nodes <- n:
			sign++
			break
		case <-timeout:
			if atomic.LoadInt32(q.closing) == 1 {
				close(q.nodes)
				return -1
			}
		}
	}
	return sign
}

func (q *DNQueue) GetNodeStat() *NodeStat {
	for {
		n := <-q.nodes
		if q.AddCount(n) {
			return n
		}
	}
}

type NodeList struct {
	sync.RWMutex
	list       map[int32]*NodeStat
	updateTime int64
	resetSign  *int32
}

func (n *NodeList) UpdateNodeList(ns map[int32]*NodeStat) {
	reset := atomic.LoadInt32(n.resetSign)
	if reset == 0 {
		if time.Now().Unix()-n.updateTime > int64(30*60) && n.updateTime > 0 {
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
		logrus.Infof("[UpdateNodeList]Finish clearing old nodes,Number of current nodes: %d\n", len(ns))
	}
	n.list = ns
	n.Unlock()
}

func (n *NodeList) Len() int {
	n.RLock()
	defer n.RUnlock()
	return len(n.list)
}

func (n *NodeList) OrderNodeList() []*NodeStat {
	for {
		var ls []*NodeStat
		if env.ALLOC_MODE == 0 {
			ls = n.SortNodeList()
		} else if env.ALLOC_MODE == -1 {
			ls = n.ShuffleNodeList()
		} else {
			ls = n.P2pOrderNodeList()
		}
		if ls == nil || len(ls) == 0 {
			logrus.Warnf("[OrderNodeList]DN list is empty.\n")
			time.Sleep(time.Duration(15) * time.Second)
		} else {
			return ls
		}
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
