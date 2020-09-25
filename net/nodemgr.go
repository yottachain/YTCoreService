package net

import (
	"crypto/sha256"
	"encoding/json"
	"errors"
	"io/ioutil"
	"net"
	"net/http"
	"strings"

	"github.com/aurawing/eos-go"
	"github.com/mr-tron/base58"
	"github.com/sirupsen/logrus"
	"github.com/yottachain/YTCoreService/codec"
	"github.com/yottachain/YTCoreService/env"
	"github.com/yottachain/YTDNMgmt"
)

var NodeMgr *YTDNMgmt.NodeDaoImpl
var superNodeList []*YTDNMgmt.SuperNode
var superNodeMap = make(map[string]*YTDNMgmt.SuperNode)
var LocalIp string = ""
var SelfIP = ""

func InitShadowPriKey() error {
	if strings.HasPrefix(env.ShadowPriKey, "yotta:") {
		keystr := strings.ReplaceAll(env.ShadowPriKey, "yotta:", "")
		data, err := base58.Decode(keystr)
		if err != nil {
			logrus.Errorf("[NodeMgr]Base58.Decode 'ShadowPriKey' ERR%s\n", err.Error())
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

func InitNodeList(ls []*YTDNMgmt.SuperNode) {
	readSuperNodeList(ls)
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
				logrus.Errorf("[NodeMgr]ERR when starting pprof server on address %s: %s\n", config.PProf.BindAddr, err)
			} else {
				logrus.Infof("[NodeMgr]Enable pprof server:%s\n", config.PProf.BindAddr)
			}
		}()
	}
	mgr, err := YTDNMgmt.NewInstance(MongoAddress, env.EOSURI, env.BPAccount, env.ShadowPriKey,
		env.ContractAccount, env.ContractOwnerD, env.ShadowAccount, int32(env.SuperNodeID),
		0, config)
	if err != nil {
		logrus.Panicf("[NodeMgr]Failed to start:%s\n", err.Error())
	}
	NodeMgr = mgr
	ls, err := mgr.GetSuperNodes()
	if err != nil {
		logrus.Panicf("[NodeMgr]Failed to GetSuperNodes:%s\n", err.Error())
	}
	if len(ls) == 0 {
		logrus.Panicf("[NodeMgr]Table 'yotta.SuperNode' no data\n")
	}
	readSuperNodeList(ls)
	readLocalIP()
	IsActive()
	AddLocalSuperNode()
	SelfIP = GetSelfIp()
	return nil
}

func AddLocalSuperNode() {
	type SuperNode struct {
		Number int
		Addr   string
	}
	path := env.YTSN_HOME + "conf/snlist.local.properties"
	data, err := ioutil.ReadFile(path)
	if err != nil {
		logrus.Errorf("Failed to read snlist.local.properties:%s\n", err)
	}
	list := []*SuperNode{}
	err = json.Unmarshal(data, &list)
	if err != nil {
		logrus.Errorf("Failed to unmarshal snlist.local.properties:%s\n", err)
	}
	for _, sn := range list {
		snode := GetSuperNode(sn.Number)
		if snode != nil {
			snode.Addrs = append(snode.Addrs, sn.Addr)
		}
	}
}

func readLocalIP() {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		panic(err)
	}
	for _, addr := range addrs {
		if ipnet, ok := addr.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
			if ipnet.IP.To4() != nil {
				ip := ipnet.IP.String()
				if ip != env.SelfIp {
					LocalIp = LocalIp + ip + ";"
				}
			}
		}
	}
	if env.SelfIp != "" {
		LocalIp = LocalIp + env.SelfIp + ";"
	}
	logrus.Infof("[NodeMgr]Local ip:%s\n", LocalIp)
}

func readKey() ([]byte, error) {
	path := env.YTSN_HOME + "res/key"
	data, err := ioutil.ReadFile(path)
	if err != nil {
		logrus.Errorf("[NodeMgr]Resource file 'ShadowPriKey.key' read failure\n")
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
			logrus.Panicf("[NodeMgr]:No 'SN%d' in yotta.SuperNode.\n", index)
		}
		pkey := sn.PubKey
		if pkey != "" && strings.HasPrefix(strings.ToUpper(pkey), "EOS") {
			pkey = pkey[3:]
			sn.PubKey = pkey
		}
		superNodeMap[pkey] = sn
	}
	logrus.Infof("[NodeMgr]Snlist init ok,Size:%d\n", len(superNodeMap))
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
	if !env.STAT_SERVICE {
		return false
	}
	ip := GetSelfIp()
	if strings.Contains(LocalIp, ip+";") {
		return true
	} else {
		return false
	}
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
	if id < 0 || id >= GetSuperNodeCount() {
		return nil
	}
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
		logrus.Errorf("[NodeMgr]Eos.StringToName(%s) return err:%s\n", username, err)
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
