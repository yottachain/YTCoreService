package api

import (
	"encoding/json"
	"errors"
	"io/ioutil"
	"os"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/yottachain/YTCoreService/api/cache"
	"github.com/yottachain/YTCoreService/codec"
	"github.com/yottachain/YTCoreService/env"
	"github.com/yottachain/YTCoreService/net"
	"github.com/yottachain/YTCoreService/pkt"
	ytcrypto "github.com/yottachain/YTCrypto"
	"github.com/yottachain/YTDNMgmt"
)

const MAX_CLIENT_NUM = 2000

var clients = struct {
	sync.RWMutex
	clientlist map[string]*Client
	clientids  sync.Map
}{clientlist: make(map[string]*Client)}

func AddClient(uid, keyNum uint32, signstr string) (*Client, error) {
	c := addClient(uid, keyNum, signstr)
	cc, er := check(c)
	if er != nil {
		return nil, er
	}
	if cc != nil {
		return cc, nil
	}
	clients.Lock()
	defer clients.Unlock()
	clients.clientlist[c.AccessorKey] = c
	clients.clientids.Store(c.UserId, c)
	NotifyAllocNode(false)
	return c, nil
}

func NewClient(uname string, privkey string) (*Client, error) {
	c, err := newClient(uname, privkey)
	if err != nil {
		return nil, err
	}
	cc, er := check(c)
	if er != nil {
		return nil, er
	}
	if cc != nil {
		return cc, nil
	}
	clients.Lock()
	defer clients.Unlock()
	err = c.Regist()
	if err != nil {
		return nil, err
	}
	clients.clientlist[c.AccessorKey] = c
	clients.clientids.Store(c.UserId, c)
	NotifyAllocNode(false)
	return c, nil
}

func check(c *Client) (*Client, error) {
	size := 0
	clients.RLock()
	client := clients.clientlist[c.AccessorKey]
	size = len(clients.clientlist)
	clients.RUnlock()
	if client != nil {
		return client, nil
	}
	if size > MAX_CLIENT_NUM {
		return nil, errors.New("Maximum number of users reached.")
	}
	return nil, nil
}

func GetClients() []*Client {
	clients.RLock()
	defer clients.RUnlock()
	ls := []*Client{}
	for _, v := range clients.clientlist {
		ls = append(ls, v)
	}
	return ls
}

func GetClientById(uid uint32) *Client {
	if vv, ok := clients.clientids.Load(uid); ok {
		return vv.(*Client)
	}
	return nil
}

func GetClient(key string) *Client {
	clients.RLock()
	defer clients.RUnlock()
	return clients.clientlist[key]
}

func DistoryClient(key string) {
	clients.Lock()
	defer clients.Unlock()
	c := clients.clientlist[key]
	if c != nil {
		delete(clients.clientlist, key)
		clients.clientids.Delete(c.UserId)
	}
}

func StartApi() {
	env.InitClient()
	codec.InitLRC()
	InitBlockRoutinePool()
	InitShardUpPool()
	InitShardDownPool()
	priv, _ := ytcrypto.CreateKey()
	net.Start(0, 0, priv)
	InitSuperList()
	cache.InitDB()
	go StartPreAllocNode()
	go DoCache()
}

func InitSuperList() {
	path := os.Getenv("YTFS.snlist")
	if path == "" {
		path = env.YTFS_HOME + "conf/snlist.properties"
	} else {
		path = env.YTFS_HOME + path
	}
	data, err := ioutil.ReadFile(path)
	if err != nil {
		logrus.Panicf("Failed to read snlist.properties:%s\n", err)
	}
	type JsonSuperNode struct {
		Number int32
		ID     string
		Addrs  []string
	}
	list := []*JsonSuperNode{}
	err = json.Unmarshal(data, &list)
	if err != nil {
		logrus.Panicf("[Init]Failed to unmarshal snlist.properties:%s\n", err)
	}
	ls := make([]*YTDNMgmt.SuperNode, len(list))
	for index, jsonsn := range list {
		sn := &YTDNMgmt.SuperNode{ID: jsonsn.Number, NodeID: jsonsn.ID, Addrs: jsonsn.Addrs}
		ls[index] = sn
	}
	GetSuperList(ls)
}

func GetSuperList(ls []*YTDNMgmt.SuperNode) {
	size := len(ls)
	for {
		ii := int(time.Now().UnixNano() % int64(size))
		sn := ls[ii]
		req := &pkt.ListSuperNodeReq{}
		res, err := net.RequestSN(req, sn, "", 0, false)
		if err == nil {
			resp, ok := res.(*pkt.ListSuperNodeResp)
			if ok {
				if resp.Supernodes != nil && resp.Supernodes.Count != nil && *resp.Supernodes.Count > 0 && resp.Supernodes.Supernode != nil {
					sns := resp.Supernodes.Supernode
					list := []*YTDNMgmt.SuperNode{}
					for _, s := range sns {
						if s.Addrs != nil && s.Id != nil && s.Nodeid != nil && s.Pubkey != nil {
							list = append(list, &YTDNMgmt.SuperNode{ID: *s.Id, NodeID: *s.Nodeid, PubKey: *s.Pubkey, Addrs: s.Addrs})
						}
					}
					if uint32(len(list)) == *resp.Supernodes.Count {
						net.InitNodeList(list)
						return
					}
				}
			}
			logrus.Errorf("[GetSuperList]Return err msg.\n")
		}
		time.Sleep(time.Duration(5) * time.Second)
	}
}
