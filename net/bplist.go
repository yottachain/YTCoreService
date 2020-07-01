package net

import (
	"container/list"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/url"
	"strings"
	"sync/atomic"
	"time"

	"github.com/aurawing/eos-go"
	"github.com/aurawing/eos-go/ecc"
	"github.com/yottachain/YTCoreService/env"
	ytcrypto "github.com/yottachain/YTCrypto"
)

func loadBpList() []string {
	confpath := env.YTSN_HOME + "conf/bplist.properties"
	txt, err := ioutil.ReadFile(confpath)
	if err != nil {
		env.Log.Errorf("Read file '%s' err:%s\n", confpath, err.Error())
		return nil
	}
	var list []string
	err = json.Unmarshal([]byte(txt), &list)
	if err != nil {
		env.Log.Errorf("Unmarshal file '%s' err:%s\n", confpath, err.Error())
		return nil
	}
	return list
}

var firstEosURI *EOSURI
var backupEosURI atomic.Value
var eosURIList *list.List

func SetBakURI(eos *EOSURI) {
	bakeos := backupEosURI.Load()
	if eos != bakeos {
		if NodeMgr != nil {
			NodeMgr.ChangeEosURL(eos.Url)
		}
		backupEosURI.Store(eos)
	}
}

func GetEOSURI() *EOSURI {
	if atomic.LoadInt32(firstEosURI.ErrStatu) == 0 {
		SetBakURI(firstEosURI)
		return firstEosURI
	} else {
		if time.Now().Unix()-atomic.LoadInt64(firstEosURI.ErrTime) > 60*60 {
			atomic.StoreInt32(firstEosURI.ErrStatu, 0)
			atomic.StoreInt64(firstEosURI.ErrTime, time.Now().Unix())
			SetBakURI(firstEosURI)
			return firstEosURI
		}
	}
	for e := eosURIList.Front(); e != nil; e = e.Next() {
		eos, _ := e.Value.(*EOSURI)
		if atomic.LoadInt32(eos.ErrStatu) == 0 {
			SetBakURI(eos)
			return eos
		} else {
			if time.Now().Unix()-atomic.LoadInt64(eos.ErrTime) > 60*60 {
				atomic.StoreInt32(eos.ErrStatu, 0)
				atomic.StoreInt64(eos.ErrTime, time.Now().Unix())
				SetBakURI(eos)
				return eos
			}
		}
	}
	SetBakURI(firstEosURI)
	return firstEosURI
}

func EOSInit() {
	firstEosURI = NewEOSURI(env.EOSURI)
	if NodeMgr != nil {
		NodeMgr.ChangeEosURL(firstEosURI.Url)
	}
	backupEosURI.Store(firstEosURI)
	ls := loadBpList()
	eosURIList = list.New()
	if ls != nil {
		newUrl, err := url.Parse(env.EOSURI)
		if err != nil {
			env.Log.Errorf("EOSURI '%s' err:%s\n", env.EOSURI, err.Error())
			return
		}
		localIp := newUrl.Hostname()
		for _, str := range ls {
			nurl := strings.ReplaceAll(env.EOSURI, localIp, str)
			eosURIList.PushBack(NewEOSURI(nurl))
		}
	}
}

type EOSURI struct {
	Url        string
	Expiration *int64
	ErrTime    *int64
	ErrStatu   *int32
	apivalue   atomic.Value
}

func (self *EOSURI) NewApi() (*eos.API, error) {
	v := self.apivalue.Load()
	if v != nil {
		api, _ := v.(*eos.API)
		return api, nil
	}
	api := eos.New(self.Url)
	keyBag := &eos.KeyBag{}
	err := keyBag.ImportPrivateKey(env.ShadowPriKey)
	if err != nil {
		return nil, fmt.Errorf("import private key: %s", err)
	}
	api.SetSigner(keyBag)
	api.SetCustomGetRequiredKeys(func(tx *eos.Transaction) ([]ecc.PublicKey, error) {
		publickey, _ := ytcrypto.GetPublicKeyByPrivateKey(env.ShadowPriKey)
		pubkey, _ := ecc.NewPublicKey(fmt.Sprintf("%s%s", "YTA", publickey))
		return []ecc.PublicKey{pubkey}, nil
	})
	self.apivalue.Store(api)
	return api, nil
}

func NewEOSURI(url string) *EOSURI {
	c := &EOSURI{}
	c.Url = url
	c.ErrTime = new(int64)
	c.ErrStatu = new(int32)
	c.Expiration = new(int64)
	atomic.StoreInt64(c.Expiration, 5)
	atomic.StoreInt32(c.ErrStatu, 0)
	atomic.StoreInt64(c.ErrTime, 0)
	return c
}

func (self *EOSURI) GetExpiration() time.Duration {
	e := atomic.AddInt64(self.Expiration, 1)
	if e > 3500 {
		atomic.StoreInt64(self.Expiration, 5)
	}
	e = atomic.AddInt64(self.Expiration, 1)
	return time.Duration(e) * time.Second
}

func (self *EOSURI) SetErr(err error) bool {
	msg := err.Error()
	if msg == "" {
		return false
	}
	if strings.Contains(msg, "Duplicate transaction") {
		time.Sleep(time.Duration(10) * time.Millisecond)
	} else {
		atomic.StoreInt32(self.ErrStatu, 1)
		atomic.StoreInt64(self.ErrTime, time.Now().Unix())
	}
	return true
}
