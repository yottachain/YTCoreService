package net

import (
	"crypto/sha256"
	"encoding/json"
	"io/ioutil"
	"os"
	"strings"

	"github.com/aurawing/eos-go/btcsuite/btcutil/base58"
	ma "github.com/multiformats/go-multiaddr"
	"github.com/sirupsen/logrus"
	"github.com/yottachain/YTCoreService/codec"
	"github.com/yottachain/YTCoreService/env"
	"github.com/yottachain/YTDNMgmt"
)

var NodeMgr *YTDNMgmt.NodeDaoImpl
var SuperNode *YTDNMgmt.SuperNode

func InitClient() {
	initSuperList()
	startTcpClient(DefaultConfig())
	startSnClient(SuperNode.NodeID, SuperNode.Addrs)
}

func InitServer(MongoAddress string, callback OnMessageFunc) {
	initNodeMgr(MongoAddress)
	var tcpcfg, httpcfg *Config
	for _, addr := range SuperNode.Addrs {
		maddr, err := ma.NewMultiaddr(addr)
		if err != nil {
			continue
		}
		if _, err := maddr.ValueForProtocol(ma.P_HTTP); err == nil {
			if httpcfg == nil {
				if port, err := maddr.ValueForProtocol(ma.P_TCP); err == nil && port != "" {
					httpcfg = httpConfig(port, SuperNode.PrivKey)
					StartHttpServer(httpcfg, callback)
				}
			}
			continue
		}
		if port, err := maddr.ValueForProtocol(ma.P_TCP); err == nil {
			if tcpcfg == nil {
				tcpcfg = tcpConfig(port, SuperNode.PrivKey)
				startTcpServer(tcpcfg, callback)
				startTcpClient(tcpcfg)
			}
		}
	}
	if tcpcfg == nil {
		startTcpClient(DefaultConfig())
	}
}

func initShadowPriKey() {
	if strings.HasPrefix(env.ShadowPriKey, "yotta:") {
		keystr := strings.ReplaceAll(env.ShadowPriKey, "yotta:", "")
		data := base58.Decode(keystr)
		if len(data) == 0 {
			logrus.Panicf("[NodeMgr]Base58.Decode 'ShadowPriKey' ERR!%s\n")
		}
		path := env.YTSN_HOME + "res/key"
		data, err := ioutil.ReadFile(path)
		if err != nil {
			logrus.Panicf("[NodeMgr]Resource file 'ShadowPriKey.key' read failure\n")
		}
		sha256Digest := sha256.New()
		sha256Digest.Write(data)
		bs := codec.ECBDecrypt(data, sha256Digest.Sum(nil))
		env.ShadowPriKey = string(bs)
	}
}

func initNodeMgr(MongoAddress string) {
	initShadowPriKey()
	config := YTDNMgmt.InitConfig(env.EOSURI, env.BPAccount, env.ShadowPriKey,
		env.ContractAccount, env.ContractOwnerD, env.ShadowAccount, int32(0))
	mgr, err := YTDNMgmt.NewInstance(MongoAddress, env.EOSURI, env.BPAccount, env.ShadowPriKey,
		env.ContractAccount, env.ContractOwnerD, env.ShadowAccount, int32(0),
		1, config)
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
	SuperNode = ls[0]
}

func initSuperList() {
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
	if len(list) == 0 {
		logrus.Panicln("[Init]Snlist num:0\n")
	}
	jsonsn := list[0]
	maddr, err := StringListToMaddrs(jsonsn.Addrs)
	if err != nil {
		logrus.Panicf("[Init]snlist Addr err:%s\n", err)
	}
	SuperNode = &YTDNMgmt.SuperNode{ID: 0, NodeID: jsonsn.ID, Addrs: jsonsn.Addrs, Multiaddrs: maddr}
}
