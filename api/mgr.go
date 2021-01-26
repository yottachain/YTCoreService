package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/eoscanada/eos-go/btcsuite/btcutil/base58"
	"github.com/sirupsen/logrus"
	"github.com/yottachain/YTCoreService/api/cache"
	"github.com/yottachain/YTCoreService/codec"
	"github.com/yottachain/YTCoreService/env"
	"github.com/yottachain/YTCoreService/net"
	"github.com/yottachain/YTCoreService/pkt"
	"github.com/yottachain/YTCrypto"
	ytcrypto "github.com/yottachain/YTCrypto"
	"github.com/yottachain/YTDNMgmt"
)

const MAX_CLIENT_NUM = 2000

var clients = struct {
	sync.RWMutex
	clientlist  map[string]*Client
	clientids   map[uint32]*Client
	clientnames map[string]*Client
}{
	clientlist:  make(map[string]*Client),
	clientids:   make(map[uint32]*Client),
	clientnames: make(map[string]*Client),
}

type Register struct {
	username string
	privkey  string
	c        *Client
}

func NewRegister(uname string, prikey string) *Register {
	return &Register{username: uname, privkey: prikey}
}

func (me *Register) newClient() error {
	bs := base58.Decode(me.privkey)
	if len(bs) != 37 {
		return errors.New("Invalid private key.")
	}
	aeskey := codec.GenerateUserKey(bs)
	pubkey, err := YTCrypto.GetPublicKeyByPrivateKey(me.privkey)
	if err != nil {
		return err
	}
	k := &Key{PrivateKey: me.privkey, KUSp: bs, AESKey: aeskey, PublicKey: pubkey}
	m := make(map[uint32]*Key)
	me.c = &Client{Username: me.username, SignKey: k, StoreKey: k, KeyMap: m}
	me.c.AccessorKey = k.PublicKey
	return nil
}

func (me *Register) regist() error {
	ii := int(time.Now().UnixNano() % int64(net.GetSuperNodeCount()))
	sn := net.GetSuperNode(ii)
	req := &pkt.RegUserReqV2{Username: &me.username, PubKey: &me.c.SignKey.PublicKey, VersionId: &env.VersionID}
	res, err := net.RequestSN(req, sn, "", 1, false)
	if err != nil {
		emsg := fmt.Sprintf("User '%s' registration failed!%s", me.c.Username, pkt.ToError(err))
		logrus.Errorf("[Regist]%s\n", emsg)
		return errors.New(emsg)
	} else {
		resp, ok := res.(*pkt.RegUserResp)
		if ok {
			if resp.SuperNodeNum != nil && resp.UserId != nil && resp.KeyNumber != nil {
				if *resp.SuperNodeNum >= 0 && *resp.SuperNodeNum < uint32(net.GetSuperNodeCount()) {
					me.c.SuperNode = net.GetSuperNode(int(*resp.SuperNodeNum))
					me.c.UserId = *resp.UserId
					me.c.SignKey.KeyNumber = *resp.KeyNumber
					me.c.KeyMap[me.c.SignKey.KeyNumber] = me.c.SignKey
					logrus.Infof("[Regist]User '%s' registration successful,ID-KeyNumber:%d/%d,at sn %d\n",
						me.c.Username, me.c.UserId, me.c.SignKey.KeyNumber, me.c.SuperNode.ID)
					return me.c.SignKey.MakeSign(me.c.UserId)
				}
			}
		}
		logrus.Errorf("[Regist]Return err msg.\n")
		return errors.New("Return err msg")
	}
}

func AddClient(uid, keyNum, storeNum uint32, signstr string) (*Client, error) {
	if env.StartSync == 0 {
		return nil, errors.New("StartSync mode " + strconv.Itoa(env.StartSync))
	}
	sn := net.GetUserSuperNode(int32(uid))
	k := &Key{KeyNumber: keyNum, Sign: signstr}
	m := make(map[uint32]*Key)
	m[k.KeyNumber] = k
	storeK := k
	if keyNum != storeNum {
		storeK = &Key{KeyNumber: storeNum, Sign: signstr}
		m[storeK.KeyNumber] = storeK
	}
	c := &Client{UserId: uid, SuperNode: sn, SignKey: k, StoreKey: storeK, KeyMap: m}
	cc, er := check(c)
	if er != nil {
		return nil, er
	}
	if cc != nil {
		return cc, nil
	}
	clients.Lock()
	clients.clientids[c.UserId] = c
	clients.Unlock()
	NotifyAllocNode(false)
	return c, nil
}

func NewClient(uname string, privkey string) (*Client, error) {
	if env.StartSync > 0 {
		return nil, errors.New("StartSync mode " + strconv.Itoa(env.StartSync))
	}
	reg := NewRegister(uname, privkey)
	err := reg.newClient()
	if err != nil {
		return nil, err
	}
	cc, er := check(reg.c)
	if er != nil {
		return nil, er
	}
	if cc != nil {
		if cc.Username != uname {
			return nil, errors.New("Same privatekey, different username " + cc.Username)
		}
		return cc, nil
	}
	clients.Lock()
	err = reg.regist()
	if err != nil {
		clients.Unlock()
		return nil, err
	}
	for _, v := range reg.c.KeyMap {
		clients.clientlist[v.PublicKey] = reg.c
	}
	clients.clientids[reg.c.UserId] = reg.c
	clients.clientnames[reg.username] = reg.c
	clients.Unlock()
	NotifyAllocNode(false)
	return reg.c, nil
}

func check(c *Client) (*Client, error) {
	clients.RLock()
	defer clients.RUnlock()
	size := len(clients.clientids)
	if size > MAX_CLIENT_NUM {
		return nil, errors.New("Maximum number of users reached.")
	}
	for _, v := range c.KeyMap {
		if v.PublicKey == "" {
			continue
		}
		client, ok := clients.clientlist[v.PublicKey]
		if ok {
			return client, nil
		}
	}
	return nil, nil
}

func GetClients() []*Client {
	clients.RLock()
	defer clients.RUnlock()
	ls := []*Client{}
	for _, v := range clients.clientids {
		ls = append(ls, v)
	}
	return ls
}

func GetClientByName(username string) *Client {
	clients.RLock()
	defer clients.RUnlock()
	return clients.clientnames[username]
}

func GetClientById(uid uint32) *Client {
	clients.RLock()
	defer clients.RUnlock()
	return clients.clientids[uid]
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
		for _, k := range c.KeyMap {
			delete(clients.clientlist, k.PublicKey)
		}
		delete(clients.clientnames, c.Username)
		delete(clients.clientids, c.UserId)
	}
}

func InitApi() {
	cache.IS_S3_Server = false
	StartApi()
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
	go StartSync()
	go AutoReg()
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
		maddr, _ := net.StringListToMaddrs(jsonsn.Addrs)
		sn := &YTDNMgmt.SuperNode{ID: jsonsn.Number, NodeID: jsonsn.ID, Addrs: jsonsn.Addrs, Multiaddrs:maddr}
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
							maddrs, _ := net.StringListToMaddrs(s.Addrs)
							list = append(list, &YTDNMgmt.SuperNode{ID: *s.Id, NodeID: *s.Nodeid, PubKey: *s.Pubkey, Addrs: s.Addrs, Multiaddrs:maddrs})
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
