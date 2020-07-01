package net

import (
	"crypto/sha256"
	"errors"
	"io/ioutil"
	"net"
	"net/http"
	"strings"

	"github.com/aurawing/eos-go"
	"github.com/mr-tron/base58"
	"github.com/yottachain/YTCoreService/codec"
	"github.com/yottachain/YTCoreService/env"
	"github.com/yottachain/YTDNMgmt"
)

var NodeMgr *YTDNMgmt.NodeDaoImpl
var superNodeList []*YTDNMgmt.SuperNode
var superNodeMap = make(map[string]*YTDNMgmt.SuperNode)

func InitShadowPriKey() error {
	if strings.HasPrefix(env.ShadowPriKey, "yotta:") {
		keystr := strings.ReplaceAll(env.ShadowPriKey, "yotta:", "")
		data, err := base58.Decode(keystr)
		if err != nil {
			env.Log.Errorf("Base58.Decode 'ShadowPriKey' ERR%s\n", err.Error())
			return err
		}
		key, err := readKey()
		if err != nil {
			return err
		}
		bs := codec.ECBDecrypt(data, key)
		env.ShadowPriKey = string(bs)
	}
	return nil
}

func InitNodeMgr(MongoAddress string) error {
	err := InitShadowPriKey()
	if err != nil {
		return err
	}
	config := YTDNMgmt.InitConfig(env.EOSURI, env.BPAccount, env.ShadowPriKey,
		env.ContractAccount, env.ContractOwnerD, env.ShadowAccount, int32(env.SuperNodeID))
	if config.PProf.Enable {
		go func() {
			err := http.ListenAndServe(config.PProf.BindAddr, nil)
			if err != nil {
				env.Log.Errorf("ERR when starting pprof server on address %s: %s\n", config.PProf.BindAddr, err)
			} else {
				env.Log.Infof("Enable pprof server:%s\n", config.PProf.BindAddr)
			}
		}()
	}
	mgr, err := YTDNMgmt.NewInstance(MongoAddress, env.EOSURI, env.BPAccount, env.ShadowPriKey,
		env.ContractAccount, env.ContractOwnerD, env.ShadowAccount, int32(env.SuperNodeID),
		0, config)
	if err != nil {
		env.Log.Panicf("YTDNMgmt failed to start:%s\n", err.Error())
	}
	NodeMgr = mgr
	ls, err := mgr.GetSuperNodes()
	if err != nil {
		env.Log.Panicf("YTDNMgmt failed to GetSuperNodes:%s\n", err.Error())
	}
	readSuperNodeList(ls)
	IsActive()
	return nil
}

func readKey() ([]byte, error) {
	path := env.YTSN_HOME + "res/key"
	data, err := ioutil.ReadFile(path)
	if err != nil {
		env.Log.Errorf("Resource file 'ShadowPriKey.key' read failure\n")
		return nil, errors.New("Resource file 'ShadowPriKey.key' read failure\n")
	}
	sha256Digest := sha256.New()
	sha256Digest.Write(data)
	return sha256Digest.Sum(nil), nil
}

func readSuperNodeList(ls []*YTDNMgmt.SuperNode) {
	num := int32(len(ls))
	superNodeList = make([]*YTDNMgmt.SuperNode, num)
	for _, sn := range ls {
		if sn.ID < num && sn.ID >= 0 {
			superNodeList[sn.ID] = sn
		}
	}
	for index, sn := range superNodeList {
		if sn == nil || sn.Addrs == nil || len(sn.Addrs) == 0 {
			env.Log.Panicf("YTDNMgmt:No 'SN%d' in yotta.SuperNode.\n", index)
		}
		pkey := sn.PubKey
		if strings.HasPrefix(strings.ToUpper(pkey), "EOS") {
			pkey = pkey[3:]
			sn.PubKey = pkey
		}
		superNodeMap[pkey] = sn
	}
}

func GetSelfIp() string {
	return GetSnIp(env.SuperNodeID)
}

func GetSnIp(snid int) string {
	self := superNodeList[snid]
	ips := strings.Split(self.Addrs[0], "/")
	if len(ips) > 3 {
		addr, err := net.ResolveIPAddr("ip", ips[2])
		if err == nil {
			return addr.String()
		}
	}
	return self.Addrs[0]
}

func IsMaster() bool {
	if env.SelfIp == "" {
		return true
	}
	ip := GetSelfIp()
	return ip == env.SelfIp
}

func IsActive() bool {
	b := IsMaster()
	if b {
		NodeMgr.SetMaster(1)
	} else {
		NodeMgr.SetMaster(0)
	}
	return b
}

func GetSuperNodes() []*YTDNMgmt.SuperNode {
	return superNodeList
}

func GetSuperNodeCount() int {
	return len(superNodeList)
}

func GetLocalSuperNode() *YTDNMgmt.SuperNode {
	return superNodeList[env.SuperNodeID]
}

func GetSuperNode(id int) *YTDNMgmt.SuperNode {
	return superNodeList[id]
}

func GetBlockSuperNode(bs []byte) *YTDNMgmt.SuperNode {
	value := int32(bs[0] & 0xFF)
	value = value<<8 | int32(bs[1]&0xFF)
	value = value<<8 | int32(bs[2]&0xFF)
	value = value<<8 | int32(bs[3]&0xFF)
	value = value & 0x0FFFF
	index := uint32(value) % uint32(GetSuperNodeCount())
	return superNodeList[index]
}

func GetNodeSuperNode(id int32) *YTDNMgmt.SuperNode {
	index := uint32(id) % uint32(GetSuperNodeCount())
	return superNodeList[index]
}

func GetUserSuperNode(id int32) *YTDNMgmt.SuperNode {
	index := uint32(id) % uint32(GetSuperNodeCount())
	return superNodeList[index]
}

func GetRegSuperNode(username string) *YTDNMgmt.SuperNode {
	number, err := eos.StringToName(username)
	if err != nil {
		env.Log.Errorf("Eos.StringToName(%s) return err:%s\n", username, err)
		return superNodeList[0]
	}
	index := number % uint64(GetSuperNodeCount())
	return superNodeList[index]
}

func AuthSuperNode(publickey string) (*YTDNMgmt.SuperNode, error) {
	sn, Ok := superNodeMap[publickey]
	if !Ok {
		return nil, errors.New("Invalid super node pubkey " + publickey)
	} else {
		return sn, nil
	}
}
