package net

import (
	"fmt"
	"net"
	"net/rpc"
	"sync"
	"time"

	"github.com/aurawing/eos-go/btcsuite/btcutil/base58"
	"github.com/libp2p/go-libp2p-core/crypto"
	ma "github.com/multiformats/go-multiaddr"
	mnet "github.com/multiformats/go-multiaddr-net"
	"github.com/sirupsen/logrus"
	"github.com/yottachain/YTCoreService/env"
	"golang.org/x/crypto/ripemd160"
)

func tcpConfig(port, privatekey string) *Config {
	privbytes := base58.Decode(privatekey)
	if len(privbytes) == 0 {
		logrus.Panicf("[TcpHost]Bad format of private key,Base58 format needed")
	}
	pk, err := crypto.UnmarshalSecp256k1PrivateKey(privbytes[1:33])
	if err != nil {
		logrus.Panicf("[TcpHost]Bad format of private key")
	}
	addr, _ := ma.NewMultiaddr(fmt.Sprintf("/ip4/0.0.0.0/tcp/%s", port))
	cfg := NewConfig(ListenAddr(addr), Identity(pk))
	return cfg
}

func startTcpServer(tcpconfig *Config, callback OnMessageFunc) {
	serverhost, err := NewTcpHost(tcpconfig)
	if err != nil {
		logrus.Panicf("[TcpHost]Init TcpHost ERR.\n")
	}
	serverhost.RegHandler(callback)
	logrus.Infof("[TcpHost]TcpServer starting...,NodeID:%s\n", serverhost.Config().ID.String())
	maddrs := tcpconfig.Addrs()
	for k, m := range maddrs {
		logrus.Infof("[TcpHost]Node Addrs %d:%s\n", k, m.String())
	}
	serverhost.Accept()
}

type ConnAutoCloser struct {
	net.Conn
	outtime time.Duration
	timer   *time.Timer
	c       chan struct{}
}

func NewConnAutoCloser(conn net.Conn, otime time.Duration) *ConnAutoCloser {
	t := time.NewTimer(otime)
	cclose := &ConnAutoCloser{conn, otime, t, make(chan struct{}, 1)}
	go func() {
		<-t.C
		if conn != nil {
			conn.Close()
		}
	}()
	return cclose
}

func (conn *ConnAutoCloser) Stop() {
	conn.timer.Reset(0)
}

func (conn *ConnAutoCloser) Read(buf []byte) (int, error) {
	n, err := conn.Conn.Read(buf)
	if err != nil {
		return n, err
	}
	if n > 0 {
		conn.ResetTimer()
	}
	return n, err
}

func (conn *ConnAutoCloser) ResetTimer() {
	select {
	case conn.c <- struct{}{}:
		conn.timer.Reset(conn.outtime)
		<-conn.c
	default:
		return
	}
}

func (conn *ConnAutoCloser) Write(buf []byte) (int, error) {
	n, err := conn.Conn.Write(buf)
	if err != nil {
		return n, err
	}
	if n > 0 {
		conn.ResetTimer()
	}
	return n, err
}

type TcpHost struct {
	cfg      *Config
	listener mnet.Listener
	srv      *rpc.Server
	hand     Handler
	closing  bool
	mutex    sync.Mutex
}

func NewTcpHost(config *Config) (*TcpHost, error) {
	hst := new(TcpHost)
	hst.cfg = config
	ls, err := mnet.Listen(hst.cfg.ListenAddr)
	if err != nil {
		return nil, err
	}
	hst.listener = ls
	srv := rpc.NewServer()
	hst.srv = srv
	return hst, nil
}

func (host *TcpHost) RegHandler(callback OnMessageFunc) {
	host.hand = func(requestData []byte, head Head) ([]byte, error) {
		pkarr := head.RemotePubKey
		hasher := ripemd160.New()
		hasher.Write(pkarr)
		sum := hasher.Sum(nil)
		pkarr = append(pkarr, sum[0:4]...)
		publicKey := base58.Encode(pkarr)
		res := callback(uint16(head.MsgId), requestData, publicKey)
		return res, nil
	}
}

func (host *TcpHost) Accept() {
	addrService := new(AddrService)
	addrService.Info.ID = host.cfg.ID
	addrService.Info.Addrs = host.cfg.Addrs()
	addrService.PubKey = host.cfg.Privkey.GetPublic()
	addrService.Version = host.cfg.Version

	msgService := new(MsgService)
	msgService.Handler = host.hand
	msgService.Pi = Peer{ID: host.cfg.ID, Addrs: addrService.Info.Addrs}
	if err := host.srv.RegisterName("as", addrService); err != nil {
		logrus.Panicf("[TcpHost]%s\n", err)
	}
	if err := host.srv.RegisterName("ms", msgService); err != nil {
		logrus.Panicf("[TcpHost]%s\n", err)
	}
	lis := mnet.NetListener(host.listener)
	go func() {
		for {
			conn, err := lis.Accept()
			if err != nil {
				if host.IsClosed() {
					return
				}
				logrus.Errorf("[TcpHost]Rpc.Serve: accept:%s\n", err.Error())
				continue
			}
			ac := NewConnAutoCloser(conn, time.Duration(env.P2P_IdleTimeout)*time.Millisecond)
			go func() {
				host.srv.ServeConn(ac)
				ac.Stop()
			}()
		}
	}()
}
func (host *TcpHost) Shutdown() {
	host.mutex.Lock()
	defer host.mutex.Unlock()
	if host.closing {
		return
	}
	host.closing = true
	host.listener.Close()
}

func (host *TcpHost) IsClosed() bool {
	host.mutex.Lock()
	defer host.mutex.Unlock()
	return host.closing
}

func (host *TcpHost) Config() *Config {
	return host.cfg
}
