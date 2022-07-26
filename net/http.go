package net

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"net/http"
	"sync"
	"time"

	"github.com/aurawing/eos-go/btcsuite/btcutil/base58"
	"github.com/libp2p/go-libp2p-core/crypto"
	"github.com/multiformats/go-multiaddr"
	manet "github.com/multiformats/go-multiaddr-net"
	"github.com/sirupsen/logrus"
	"github.com/yottachain/YTCoreService/env"
	"golang.org/x/crypto/ripemd160"
)

func httpConfig(port, privatekey string) *Config {
	privbytes := base58.Decode(privatekey)
	if len(privbytes) == 0 {
		logrus.Panicf("[HttpHost]Bad format of private key,Base58 format needed")
	}
	pk, err := crypto.UnmarshalSecp256k1PrivateKey(privbytes[1:33])
	if err != nil {
		logrus.Panicf("[HttpHost]Bad format of private key")
	}
	addr, _ := multiaddr.NewMultiaddr(fmt.Sprintf("/ip4/0.0.0.0/tcp/%s/http", port))
	cfg := NewConfig(ListenAddr(addr), Identity(pk))
	return cfg
}

func startHttpClient() {
	tr := &http.Transport{
		MaxIdleConns:    env.P2P_RequestQueueSize + env.P2P_ResponseQueueSize,
		IdleConnTimeout: time.Duration(env.P2P_ConnectTimeout) * time.Millisecond,
	}
	client = &http.Client{Transport: tr}
	client.Timeout = time.Duration(env.P2P_ReadTimeout) * time.Millisecond
}

func StartHttpServer(config *Config, callback OnMessageFunc) {
	serverhost, err := NewHttpHost(config)
	if err != nil {
		logrus.Panicf("[HttpHost]Init HttpHost ERR.\n")
	}
	serverhost.RegHttpHandler(callback)
	logrus.Infof("[HttpHost]HttpServer starting...,NodeID:%s\n", config.ID.String())
	maddrs := config.Addrs()
	for k, m := range maddrs {
		logrus.Infof("[HttpHost]Node Addrs %d:%s\n", k, m.String())
	}
	serverhost.Accept()
}

type HttpHost struct {
	cfg       *Config
	listenner manet.Listener
	sync.Map
}

func NewHttpHost(config *Config) (*HttpHost, error) {
	hst := new(HttpHost)
	hst.cfg = config
	lis, err := manet.Listen(hst.cfg.ListenAddr)
	if err != nil {
		return nil, err
	}
	hst.listenner = lis
	return hst, nil
}

func (h *HttpHost) Accept() {
	hlis := manet.NetListener(h.listenner)
	go http.Serve(hlis, nil)
}

func (h *HttpHost) RegHttpHandler(callback OnMessageFunc) {
	hand := func(requestData []byte, head Head) ([]byte, error) {
		pkarr := head.RemotePubKey
		hasher := ripemd160.New()
		hasher.Write(pkarr)
		sum := hasher.Sum(nil)
		pkarr = append(pkarr, sum[0:4]...)
		publicKey := base58.Encode(pkarr)
		res := callback(uint16(head.MsgId), requestData, publicKey)
		return res, nil
	}
	http.HandleFunc("/msg/", func(writer http.ResponseWriter, request *http.Request) {
		reqData, err := ioutil.ReadAll(request.Body)
		if err != nil {
			writer.WriteHeader(500)
			fmt.Fprintln(writer, "request body read error:", err.Error())
			writer.Write([]byte{})
			return
		}
		pk, err := h.cfg.Privkey.GetPublic().Raw()
		if err != nil {
			writer.WriteHeader(500)
			fmt.Fprintln(writer, "get pubkey error:", err.Error())
			writer.Write([]byte{})
			return
		}
		msgId := 0
		fmt.Sscanf(request.URL.String(), "/msg/%d", &msgId)
		res, err := hand(reqData, Head{MsgId: int32(msgId), RemotePeerID: h.cfg.ID, RemoteAddrs: nil, RemotePubKey: pk})
		if err != nil {
			writer.WriteHeader(500)
			fmt.Fprintln(writer, err.Error())
		} else {
			writer.Write(res)
		}
	})
}

var client *http.Client

func SendHTTPMsg(ma multiaddr.Multiaddr, mid int32, msg []byte) ([]byte, error) {
	addr, err := ma.ValueForProtocol(multiaddr.P_DNS4)
	if err != nil {
		ip, err := ma.ValueForProtocol(multiaddr.P_IP4)
		if err != nil {
			return nil, err
		}
		addr = ip
	}
	port, err := ma.ValueForProtocol(multiaddr.P_TCP)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequest("POST", fmt.Sprintf("http://%s:%s/msg/%d", addr, port, mid), bytes.NewBuffer(msg))
	if err != nil {
		return nil, err
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	respData, err := ioutil.ReadAll(resp.Body)
	return respData, err
}
