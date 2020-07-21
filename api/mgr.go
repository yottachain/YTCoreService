package api

import (
	"encoding/json"
	"errors"
	"io/ioutil"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/yottachain/YTCoreService/codec"
	"github.com/yottachain/YTCoreService/env"
	"github.com/yottachain/YTCoreService/net"
	"github.com/yottachain/YTCoreService/pkt"
	ytcrypto "github.com/yottachain/YTCrypto"
	"github.com/yottachain/YTDNMgmt"
)

var VersionID string

const MAX_CLIENT_NUM = 2000

var clients = struct {
	sync.RWMutex
	clientlist map[string]*Client
}{clientlist: make(map[string]*Client)}

func NewClient(uname string, privkey string) (*Client, error) {
	c, err := newClient(uname, privkey)
	if err != nil {
		return nil, err
	}
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
	clients.Lock()
	defer clients.Unlock()
	err = c.Regist()
	if err != nil {
		return nil, err
	}
	clients.clientlist[c.AccessorKey] = c
	return c, nil
}

func StartApi() {
	env.InitClient()
	codec.InitLRC()
	priv, _ := ytcrypto.CreateKey()
	net.Start(0, 0, priv)
	InitSuperList()
}

func InitSuperList() {
	path := env.YTFS_HOME + "conf/snlist.properties"
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
		ii := int(time.Now().Unix() % int64(size))
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
