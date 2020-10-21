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

type idWeight struct {
	id int32
	weight float64
}

type idWeightsAsc []idWeight
func (p idWeightsAsc) Len() int { return len(p) }
func (p idWeightsAsc) Less(i, j int) bool {
	return p[i].weight < p[j].weight
}
// 交换数据
func (p idWeightsAsc) Swap(i, j int) { p[i], p[j] = p[j], p[i] }


type idWeightsDes []idWeight
func (p idWeightsDes) Len() int { return len(p) }
func (p idWeightsDes) Less(i, j int) bool {
	return p[i].weight > p[j].weight
}
// 交换数据
func (p idWeightsDes) Swap(i, j int) { p[i], p[j] = p[j], p[i] }

type NodeStatWOK struct {
	NodeInfo *NodeStat
	OKTimes  *int32
}

func (q *NodeStatWOK) AddCount() {
	atomic.AddInt32(q.OKTimes, 1)
}

func (q *NodeStatWOK) DecCount() {
	atomic.AddInt32(q.OKTimes, -1)
}

type DNQueue struct {
	sync.RWMutex
	nodemap map[int32]*NodeStatWOK
	queue   []*NodeStatWOK
	limit   int
	pos     int
}

func NewDNQueue() *DNQueue {
	queue := &DNQueue{nodemap: make(map[int32]*NodeStatWOK), limit: 0, pos: 0}
	return queue
}

func (q *DNQueue) order() bool {
	ls := []*NodeStatWOK{}
	nodes := DNList.GetNodeList()
	for _, n := range nodes {
		nw, ok := q.nodemap[n.Id]
		if ok {
			if nw.NodeInfo != n {
				nw.NodeInfo = n
			}
		} else {
			nw = &NodeStatWOK{NodeInfo: n, OKTimes: new(int32)}
			*nw.OKTimes = 0
			q.nodemap[n.Id] = nw
		}
		if atomic.LoadInt32(nw.OKTimes) < int32(env.ShardNumPerNode) {
			ls = append(ls, nw)
		}
	}
	size := len(ls)
	if size == 0 {
		return false
	}
	q.queue = OrderNodeList(ls)
	q.pos = 0
	q.limit = len(ls)
	return true
}

func (q *DNQueue) GetNodeStatExcluld(blk []int32) *NodeStatWOK {
	for {
		n := q.GetNodeStat()
		//n := q.GetWeightNodeStat()
		if !env.IsExistInArray(n.NodeInfo.Id, blk) && atomic.LoadInt32(n.OKTimes) <= int32(env.ShardNumPerNode) {
			return n
		}
	}
}

func (q *DNQueue) GetNodeStat() *NodeStatWOK {
	q.Lock()
	defer q.Unlock()
	q.pos++
	for {
		if q.pos >= q.limit {
			if q.order() {
				break
			} else {
				logrus.Errorf("[GetNodeStat]Not enough nodes to upload shards,waiting...\n")
				time.Sleep(time.Duration(60) * time.Second)
			}
		} else {
			break
		}
	}
	node := q.queue[q.pos]
	node.AddCount()
	return node
}

func (q *DNQueue) GetWeightNodeStat() *NodeStatWOK {
	q.Lock()
	defer q.Unlock()

	var node *NodeStat
	for {
		node = DNList.GetNodeStat()
		if node != nil {
			break
		}
	}

	nStat, ok := q.nodemap[node.Id]
	if !ok {
		nStat = &NodeStatWOK{NodeInfo: node, OKTimes: new(int32)}
		*nStat.OKTimes = 0
		q.nodemap[node.Id] = nStat
	}

	nStat.AddCount()

	return nStat
}

type NodeList struct {
	sync.RWMutex
	list       map[int32]*NodeStat
	weights 	[] idWeight
	wIds		[] int32
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
			newn, ok := ns[k]
			if !ok {
				ns[k] = v
			} else {
				newn.UpdateState(v)
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

func (n *NodeList) SetwIds (dnc uint, divsor uint) {
	n.Lock()
	defer n.Unlock()

	n.weights = nil
	n.wIds = nil

	for id, v := range n.list {
		weight := idWeight{id, v.Weight}
		n.weights = append(n.weights, weight)
	}

	sort.Sort(idWeightsDes(n.weights))

	for i, v := range n.weights {
		if uint(i) > dnc {
			break
		}
		num := uint(v.weight) / divsor
		for j := uint(0); j < num; j++ {
			n.wIds = append(n.wIds, v.id)
		}
	}
}

func (n *NodeList) GetNodeStat () *NodeStat{
	n.RLock()
	defer n.RUnlock()

	var r = rand.New(rand.NewSource(time.Now().UnixNano()))
	l := len(n.wIds)
	idx := r.Intn(l)
	nId := n.wIds[idx]

	node, ok := n.list[nId]
	if ok {
		return node
	}else {
		return nil
	}
}

func (n *NodeList) Len() int {
	n.RLock()
	defer n.RUnlock()
	return len(n.list)
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

type NodeStat struct {
	net.Node
	okDelayTimes *int64
	okTimes      *int64
	errTimes     *int64
	resetTime    int64
	snid         int32
	timestamp    int64
	sign         string
	ERRTIMES     int64
}

func NewNodeStat(id int32, timestamp int64, sign string) *NodeStat {
	ns := &NodeStat{okDelayTimes: new(int64), okTimes: new(int64), errTimes: new(int64)}
	*ns.okDelayTimes = 0
	*ns.okTimes = 0
	*ns.errTimes = 0
	ns.resetTime = time.Now().Unix()
	ns.snid = id
	ns.timestamp = timestamp
	ns.sign = sign
	ns.ERRTIMES = int64(env.Writetimeout) * int64(time.Millisecond)
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

func (n *NodeStat) SetOK(t int64) {
	atomic.AddInt64(n.okTimes, 1)
	atomic.AddInt64(n.okDelayTimes, t*int64(time.Millisecond))
}

func (n *NodeStat) RandDelayTimes(size int) int {
	return rand.Intn(size * 100)
}

func (n *NodeStat) UpdateState(oldn *NodeStat) {
	*n.okDelayTimes = atomic.LoadInt64(oldn.okDelayTimes)
	*n.okTimes = atomic.LoadInt64(oldn.okTimes)
	*n.errTimes = atomic.LoadInt64(oldn.errTimes)
}

func (n *NodeStat) GetDelayTimes() int64 {
	oktimes := atomic.LoadInt64(n.okDelayTimes)
	count := atomic.LoadInt64(n.okTimes)
	errcount := atomic.LoadInt64(n.errTimes)
	if count == 0 {
		if errcount == 0 {
			return 0
		} else {
			return (n.ERRTIMES * errcount) / errcount
		}
	} else {
		times := oktimes / count
		if times > n.ERRTIMES {
			return times
		} else {
			return (oktimes + n.ERRTIMES*errcount) / (count + errcount)
		}
	}
}

type NodeStatOrder struct {
	Nodes    []*NodeStatWOK
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
		i1 := ns.Nodes[i].NodeInfo.RandDelayTimes(size)
		i2 := ns.Nodes[j].NodeInfo.RandDelayTimes(size)
		return i1 < i2
	} else {
		return ns.Nodes[i].NodeInfo.GetDelayTimes() < ns.Nodes[j].NodeInfo.GetDelayTimes()
	}
}

func OrderNodeList(nodes []*NodeStatWOK) []*NodeStatWOK {
	var ls []*NodeStatWOK
	if env.ALLOC_MODE == 0 {
		ls = SortNodeList(nodes)
	} else if env.ALLOC_MODE == -1 {
		ls = ShuffleNodeList(nodes)
	} else {
		ls = P2pOrderNodeList(nodes)
	}
	return ls
}

func P2pOrderNodeList(nodes []*NodeStatWOK) []*NodeStatWOK {
	nmap := make(map[string]*NodeStatWOK)
	var iids []string
	for _, n := range nodes {
		nmap[n.NodeInfo.Nodeid] = n
		iids = append(iids, n.NodeInfo.Nodeid)
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
	nnodes := []*NodeStatWOK{}
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

func ShuffleNodeList(nodes []*NodeStatWOK) []*NodeStatWOK {
	s := &NodeStatOrder{Nodes: nodes, RandMode: true}
	sort.Sort(s)
	return nodes
}

func SortNodeList(nodes []*NodeStatWOK) []*NodeStatWOK {
	s := &NodeStatOrder{Nodes: nodes, RandMode: false}
	sort.Sort(s)
	return nodes
}
