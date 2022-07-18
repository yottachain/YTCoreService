package api

import (
	"errors"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/aurawing/eos-go/btcsuite/btcutil/base58"
	"github.com/sirupsen/logrus"
	"github.com/yottachain/YTCoreService/api/cache"
	"github.com/yottachain/YTCoreService/codec"
	"github.com/yottachain/YTCoreService/env"
	"github.com/yottachain/YTCoreService/net"
	"github.com/yottachain/YTCoreService/pkt"
	"github.com/yottachain/YTCrypto"
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

type Key struct {
	PrivateKey string
	PublicKey  string
	KUSp       []byte
	AESKey     []byte
	KeyNumber  uint32
	Sign       string
}

func (c *Key) MakeSign(uid uint32) error {
	data := fmt.Sprintf("%d%d", uid, c.KeyNumber)
	s, err := YTCrypto.Sign(c.PrivateKey, []byte(data))
	if err != nil {
		logrus.Errorf("[Regist]Sign ERR:%s\n", err)
		return err
	} else {
		c.Sign = s
		return nil
	}
}

func NewClient(user *UserInfo, retrytime int) (*Client, error) {
	if env.StartSync > 0 {
		return nil, errors.New("StartSync mode " + strconv.Itoa(env.StartSync))
	}
	reg := NewRegister(user)
	err := reg.newClient()
	if err != nil {
		return nil, err
	}
	cc, er := check(reg.c)
	if er != nil {
		return nil, er
	}
	if cc != nil {
		if cc.Username != user.UserName {
			return nil, errors.New("Same privatekey, different username " + cc.Username)
		}
		return cc, nil
	}
	clients.Lock()
	err = reg.regist(retrytime)
	if err != nil {
		clients.Unlock()
		return nil, err
	}
	for _, v := range reg.c.KeyMap {
		clients.clientlist[v.PublicKey] = reg.c
	}
	clients.clientids[reg.c.UserId] = reg.c
	clients.clientnames[reg.c.Username] = reg.c
	clients.Unlock()
	SaveClients()
	NotifyAllocNode(false)
	return reg.c, nil
}

type Register struct {
	users   *UserInfo
	keys    []*Key
	pubkeys []string
	c       *Client
}

func NewRegister(us *UserInfo) *Register {
	return &Register{users: us}
}

func (me *Register) newClient() error {
	c := &Client{Username: me.users.UserName, KeyMap: make(map[uint32]*Key)}
	me.keys = []*Key{}
	me.pubkeys = []string{}
	if me.users.Privkey == nil {
		return errors.New("private key is nil")
	}
	for index, pkey := range me.users.Privkey {
		bs := base58.Decode(pkey)
		if len(bs) != 37 {
			return errors.New("Invalid private key " + pkey)
		}
		aeskey := codec.GenerateUserKey(bs)
		pubkey, err := YTCrypto.GetPublicKeyByPrivateKey(pkey)
		if err != nil {
			return errors.New("Invalid private key " + pkey)
		}
		k := &Key{PrivateKey: pkey, KUSp: bs, AESKey: aeskey, PublicKey: pubkey}
		if index == int(me.users.SignKeyNumber) {
			c.SignKey = k
		}
		if index == int(me.users.EncKeyNumber) {
			c.StoreKey = k
		}
		me.keys = append(me.keys, k)
		me.pubkeys = append(me.pubkeys, pubkey)
	}
	if c.SignKey == nil || c.StoreKey == nil {
		return errors.New("signature / store private key not specified")
	}
	me.c = c
	return nil
}

type RetrieableError struct {
	msg string
}

func (me *RetrieableError) Error() string {
	return me.msg
}

func (me *Register) regist(retrytimes int) error {
	req := &pkt.RegUserReqV3{VersionId: &env.Version, Username: &me.users.UserName, PubKey: me.pubkeys}
	res, err := net.RequestSN(req)
	if err != nil {
		emsg := fmt.Sprintf("User '%s' registration failed!%s", me.c.Username, pkt.ToError(err))
		logrus.Errorf("[Regist]%s\n", emsg)
		if !(err.Code == pkt.COMM_ERROR || err.Code == pkt.SERVER_ERROR || err.Code == pkt.CONN_ERROR || err.Code == pkt.TOO_MANY_CURSOR) {
			return errors.New(emsg)
		} else {
			return &RetrieableError{msg: emsg}
		}
	} else {
		resp, ok := res.(*pkt.RegUserRespV2)
		if ok {
			if resp.UserId != nil && resp.KeyNumber != nil {
				if len(me.keys) == len(resp.KeyNumber) {
					me.c.UserId = *resp.UserId
					for index, k := range me.keys {
						num := resp.KeyNumber[index]
						if num == -1 {
							logrus.Infof("[Regist]User '%s',publickey %s authentication error\n", me.c.Username, k.PublicKey)
							if me.c.SignKey == k || me.c.StoreKey == k {
								return errors.New("publickey authentication error")
							} else {
								continue
							}
						}
						k.KeyNumber = uint32(num)
						me.c.KeyMap[k.KeyNumber] = k
						err := k.MakeSign(me.c.UserId)
						if err != nil {
							return err
						}
					}
					logrus.Infof("[Regist]User '%s' registration successful,ID:%d\n", me.c.Username, me.c.UserId)
					return nil
				}
			}
		}
		logrus.Errorf("[Regist]Return err msg.\n")
		return errors.New("return err msg")
	}
}

func AddClient(uid, keyNum, storeNum uint32, signstr string) (*Client, error) {
	if env.StartSync == 0 {
		return nil, errors.New("StartSync mode " + strconv.Itoa(env.StartSync))
	}
	k := &Key{KeyNumber: keyNum, Sign: signstr}
	m := make(map[uint32]*Key)
	m[k.KeyNumber] = k
	storeK := k
	if keyNum != storeNum {
		storeK = &Key{KeyNumber: storeNum, Sign: signstr}
		m[storeK.KeyNumber] = storeK
	}
	c := &Client{UserId: uid, SignKey: k, StoreKey: storeK, KeyMap: m}
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

func AutoReg() {
	if env.StartSync > 0 {
		return
	}
	infos := ReadUserProperties()
	for {
		users := []*UserInfo{}
		for _, user := range infos {
			_, err := NewClient(user, 1)
			if err != nil {
				_, ok := err.(*RetrieableError)
				if ok {
					users = append(users, user)
				}
			}
			time.Sleep(time.Duration(1) * time.Second)
		}
		infos = users
		if len(infos) == 0 {
			break
		}
		time.Sleep(time.Duration(5) * time.Second)
	}
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

func check(c *Client) (*Client, error) {
	clients.RLock()
	defer clients.RUnlock()
	size := len(clients.clientids)
	if size > MAX_CLIENT_NUM {
		return nil, errors.New("maximum number of users reached")
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

func SaveClients() {
	cs := GetClients()
	var users []*UserInfo
	for _, c := range cs {
		if c.StoreKey == nil {
			continue
		}
		user := &UserInfo{}
		user.UserName = c.Username
		var keys []string
		for _, v := range c.KeyMap {
			keys = append(keys, v.PrivateKey)
		}
		user.Privkey = keys
		user.SignKeyNumber = int32(c.SignKey.KeyNumber)
		user.EncKeyNumber = int32(c.StoreKey.KeyNumber)
		users = append(users, user)
	}
	clients.Lock()
	defer clients.Unlock()
	SaveEncryptUserProperties(users)
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

func StartMobileAPI() {
	env.InitClient()
	codec.InitLRC()
	net.InitClient()
}

func StartApi() {
	env.InitClient()
	codec.InitLRC()
	InitBlockRoutinePool()
	InitShardUpPool()
	InitShardDownPool()
	net.InitClient()
	err := cache.InitDB()
	if err != nil {
		logrus.Panicf("InitDB ERR:%s\n", err)
	}
	go StartPreAllocNode()
	go DoCache()
	go StartSync()
	AutoReg()
}
