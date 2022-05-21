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
var DNList *NodeList

func NotifyAllocNode(reset bool) {
	if reset {
		atomic.StoreInt32(DNList.resetSign, 1)
		cond.Signal()
		time.Sleep(time.Duration(60) * time.Second)
	} else {
		cond.Signal()
		if env.StartSync > 0 {
			for {
				if DNList.Len() > 0 {
					break
				} else {
					time.Sleep(time.Duration(1) * time.Second)
				}
			}
		}
	}
}

func StartPreAllocNode() {
	rand.Seed(time.Now().UnixNano())
	DNList = &NodeList{list: make(map[int32]*NodeStat), updateTime: 0, resetSign: new(int32)}
	*DNList.resetSign = 0
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
		ii := int(time.Now().UnixNano() % int64(size))
		err := PreAllocNode(clients[ii])
		if err != nil {
			time.Sleep(time.Duration(15) * time.Second)
			continue
		} else {
			if DNList.Len() < env.PNN {
				time.Sleep(time.Duration(65) * time.Second)
				continue
			}
		}
		cond.L.Lock()
		cond.Wait()
		cond.L.Unlock()
	}
}

func PreAllocNode(c *Client) error {
	defer env.TracePanic("[PreAllocNode]")
	req := &pkt.PreAllocNodeReqV2{UserId: &c.UserId, SignData: &c.SignKey.Sign, KeyNumber: &c.SignKey.KeyNumber, Count: new(uint32)}
	*req.Count = uint32(env.PNN)
	req.Excludes = ErrorList()
	res, err := net.RequestSN(req, c.SuperNode, "", 0, false)
	if err != nil {
		logrus.Debugf("[PreAllocNode]Return ERR:%d-%s\n", err.GetCode(), err.GetMsg())
		return errors.New(fmt.Sprintf("%d-%s", err.GetCode(), err.GetMsg()))
	}
	resp, ok := res.(*pkt.PreAllocNodeResp)
	if ok {
		if resp.Preallocnode != nil && len(resp.Preallocnode) > 0 {
			nodemap := make(map[int32]*NodeStat)
			for index, n := range resp.Preallocnode {
				if n.Id == nil || n.Nodeid == nil || n.Pubkey == nil || n.Timestamp == nil || n.Sign == nil || n.Addrs == nil {
					continue
				}
				ns := NewNodeStat(c.SuperNode.ID, *n.Timestamp, *n.Sign)
				ns.okDelayTimes.Set(int64(index))
				ns.okTimes.Set(1)
				ns.Id = *n.Id
				ns.Nodeid = *n.Nodeid
				ns.Pubkey = *n.Pubkey
				ns.Addrs = n.Addrs
				if n.Weight == nil {
					ns.Weight = 0
				} else {
					ns.Weight = *n.Weight
				}
				//logrus.Debugf("[PreAllocNode]Return %d,Weight:%f\n", ns.Id, ns.Weight)
				if !IsError(ns.Id) {
					nodemap[ns.Id] = ns
				}
			}
			nlen := len(nodemap)
			if nlen > 0 {
				DNList.UpdateNodeList(nodemap)
				logrus.Infof("[PreAllocNode]Return %d nodes,Excludes %d nodes,List len:%d.\n", nlen, len(req.Excludes), DNList.Len())
				return nil
			}
		}
		logrus.Errorf("[PreAllocNode]Return to 0 nodes.\n")
	} else {
		logrus.Errorf("[PreAllocNode]Return err msg.\n")
	}
	return errors.New("Return err msg")
}

var ERR_LIST_CACHE = cache.New(time.Duration(180)*time.Minute, time.Duration(5)*time.Second)

func AddError(id int32) {
	ERR_LIST_CACHE.SetDefault(strconv.Itoa(int(id)), "")
	DNList.Delete(id)
}

func IsError(id int32) bool {
	_, ok := ERR_LIST_CACHE.Get(strconv.Itoa(int(id)))
	return ok
}

func ErrorList() []int32 {
	var ids []int32
	ls := ERR_LIST_CACHE.Items()
	for idstr := range ls {
		id, err := strconv.Atoi(idstr)
		if err == nil {
			ids = append(ids, int32(id))
			if len(ids) >= env.PNN {
				break
			}
		}
	}
	return ids
}
