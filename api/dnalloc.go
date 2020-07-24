package api

import (
	"errors"
	"fmt"
	"math/rand"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/patrickmn/go-cache"
	"github.com/sirupsen/logrus"
	"github.com/yottachain/YTCoreService/env"
	"github.com/yottachain/YTCoreService/net"
	"github.com/yottachain/YTCoreService/pkt"
)

var cond = sync.NewCond(new(sync.Mutex))
var NODE_LIST *NodeList

func NotifyAllocNode(reset bool) {
	if reset {
		atomic.StoreInt32(NODE_LIST.resetSign, 1)
	}
	cond.Signal()
}

func StartPreAllocNode() {
	rand.Seed(time.Now().UnixNano())
	NODE_LIST = &NodeList{list: make(map[int32]*NodeStat), updateTime: 0, resetSign: new(int32)}
	*NODE_LIST.resetSign = 0
	go func() {
		for {
			time.Sleep(time.Duration(env.PTR*60-15) * time.Second)
			cond.Signal()
		}
	}()
	for {
		clients := GetClients()
		size := len(clients)
		if size == 0 {
			cond.L.Lock()
			cond.Wait()
			cond.L.Unlock()
			continue
		}
		ii := int(time.Now().Unix() % int64(size))
		err := PreAllocNode(clients[ii])
		if err != nil {
			time.Sleep(time.Duration(15) * time.Second)
			continue
		} else {
			time.Sleep(time.Duration(15) * time.Second)
		}
		cond.L.Lock()
		cond.Wait()
		cond.L.Unlock()
	}
}

func PreAllocNode(c *Client) error {
	req := &pkt.PreAllocNodeReqV2{UserId: &c.UserId, SignData: &c.Sign, KeyNumber: &c.KeyNumber, Count: new(uint32)}
	*req.Count = uint32(env.PNN)
	req.Excludes = ErrorList()
	res, err := net.RequestSN(req, c.SuperNode, "", 0, false)
	if err != nil {
		return errors.New(fmt.Sprintf("%d-%s", err.GetCode(), err.GetMsg()))
	}
	resp, ok := res.(*pkt.PreAllocNodeResp)
	if ok {
		if resp.Preallocnode != nil && len(resp.Preallocnode) > 0 {
			nodemap := make(map[int32]*NodeStat)
			for _, n := range resp.Preallocnode {
				if n.Id == nil || n.Nodeid == nil || n.Pubkey == nil || n.Timestamp == nil || n.Sign == nil || n.Addrs == nil {
					continue
				}
				ns := &NodeStat{}
				ns.Id = *n.Id
				ns.Nodeid = *n.Nodeid
				ns.Pubkey = *n.Pubkey
				ns.Addrs = n.Addrs
				ns.timestamp = *n.Timestamp
				ns.sign = *n.Sign
				ns.snid = c.SuperNode.ID
				nodemap[ns.Id] = ns
			}
			NODE_LIST.UpdateNodeList(nodemap)
			logrus.Infof("[PreAllocNode]Return %d items,Excludes %d items.\n", len(nodemap), len(req.Excludes))
			return nil
		}
	}
	logrus.Errorf("[PreAllocNode]Return err msg.\n")
	return errors.New("Return err msg")
}

var ERR_LIST_CACHE = cache.New(time.Duration(180)*time.Second, time.Duration(5)*time.Second)

func GetExpiredTime() time.Duration {
	if env.PTR == 0 {
		return time.Duration(180) * time.Second
	} else {
		return time.Duration(env.PTR*60*2) * time.Second
	}
}

func AddError(id int32) {
	ERR_LIST_CACHE.Set(strconv.Itoa(int(id)), "", GetExpiredTime())
}

func ErrorList() []int32 {
	var ids []int32
	ls := ERR_LIST_CACHE.Items()
	for idstr := range ls {
		id, err := strconv.Atoi(idstr)
		if err == nil {
			ids = append(ids, int32(id))
			if len(ids) >= 300 {
				break
			}
		}
	}
	return ids
}
