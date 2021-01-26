package api

import (
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/aurawing/eos-go/btcsuite/btcutil/base58"
	"github.com/sirupsen/logrus"
	"github.com/yottachain/YTCoreService/codec"
	"github.com/yottachain/YTCoreService/env"
	"github.com/yottachain/YTCoreService/net"
	"github.com/yottachain/YTCoreService/pkt"
	"github.com/yottachain/YTCrypto"
)

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

func NewClientV2(user *env.UserInfo, retrytime int) (*Client, error) {
	if env.StartSync > 0 {
		return nil, errors.New("StartSync mode " + strconv.Itoa(env.StartSync))
	}
	reg := NewRegisterV2(user)
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
	NotifyAllocNode(false)
	return reg.c, nil
}

type RegisterV2 struct {
	users   *env.UserInfo
	keys    []*Key
	pubkeys []string
	c       *Client
}

func NewRegisterV2(us *env.UserInfo) *RegisterV2 {
	return &RegisterV2{users: us}
}

func (me *RegisterV2) newClient() error {
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
		return errors.New("Signature / store private key not specified")
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

func (me *RegisterV2) regist(retrytimes int) error {
	c := me.c
	ii := int(time.Now().UnixNano() % int64(net.GetSuperNodeCount()))
	sn := net.GetSuperNode(ii)
	req := &pkt.RegUserReqV3{VersionId: &env.VersionID, Username: &me.users.UserName, PubKey: me.pubkeys}
	res, err := net.RequestSN(req, sn, "", retrytimes, false)
	if err != nil {
		emsg := fmt.Sprintf("User '%s' registration failed!%s", c.Username, pkt.ToError(err))
		logrus.Errorf("[RegistV2]%s\n", emsg)
		if !(err.Code == pkt.COMM_ERROR || err.Code == pkt.SERVER_ERROR || err.Code == pkt.CONN_ERROR || err.Code == pkt.TOO_MANY_CURSOR) {
			return errors.New(emsg)
		} else {
			return &RetrieableError{msg: emsg}
		}
	} else {
		resp, ok := res.(*pkt.RegUserRespV2)
		if ok {
			if resp.SuperNodeNum != nil && resp.UserId != nil && resp.KeyNumber != nil {
				if *resp.SuperNodeNum >= 0 && *resp.SuperNodeNum < uint32(net.GetSuperNodeCount()) && len(me.keys) == len(resp.KeyNumber) {
					me.c.SuperNode = net.GetSuperNode(int(*resp.SuperNodeNum))
					c.UserId = *resp.UserId
					for index, k := range me.keys {
						num := resp.KeyNumber[index]
						if num == -1 {
							logrus.Infof("[RegistV2]User '%s',publickey %s authentication error\n", c.Username, k.PublicKey)
							if c.SignKey == k || c.StoreKey == k {
								return errors.New("Publickey authentication error.")
							} else {
								continue
							}
						}
						k.KeyNumber = uint32(num)
						c.KeyMap[k.KeyNumber] = k
						err := k.MakeSign(c.UserId)
						if err != nil {
							return err
						}
					}
					logrus.Infof("[RegistV2]User '%s' registration successful,ID:%d,at sn %d\n",
						c.Username, c.UserId, c.SuperNode.ID)
					return nil
				}
			}
		}
		logrus.Errorf("[RegistV2]Return err msg.\n")
		return errors.New("Return err msg")
	}
}

var AUTO_REG_FLAG = true

func AutoReg() {
	if env.StartSync > 0 || AUTO_REG_FLAG == false {
		return
	}
	infos := env.ReadUserProperties()
	for {
		users := []*env.UserInfo{}
		for _, user := range infos {
			_, err := NewClientV2(user, 1)
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
