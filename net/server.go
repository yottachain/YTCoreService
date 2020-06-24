package net

import (
	"errors"
	"fmt"
	"sync"
	"time"

	crypto "github.com/libp2p/go-libp2p-core/crypto"
	"github.com/mr-tron/base58"
	ma "github.com/multiformats/go-multiaddr"
	hst "github.com/yottachain/YTHost"
	host "github.com/yottachain/YTHost/hostInterface"
	"github.com/yottachain/YTHost/option"
	"github.com/yottachain/YTHost/service"
	"github.com/yottachain/YTCoreService/env"
	"golang.org/x/crypto/ripemd160"
)

var p2phst host.Host
var mu sync.Mutex

func Stop() {

}

func Start(port int32, privatekey string) error {
	mu.Lock()
	defer mu.Unlock()
	if p2phst != nil {
		return nil
	}
	privbytes, err := base58.Decode(privatekey)
	if err != nil {
		return errors.New("bad format of private key,Base58 format needed")
	}
	pk, err := crypto.UnmarshalSecp256k1PrivateKey(privbytes[1:33])
	if err != nil {
		return errors.New("bad format of private key")
	}
	ma, _ := ma.NewMultiaddr(fmt.Sprintf("/ip4/0.0.0.0/tcp/%d", port))
	p2phst, err = hst.NewHost(option.ListenAddr(ma), option.Identity(pk))
	if err != nil {
		return err
	}
	go p2phst.Accept()
	env.Log.Infof("P2P initialization completed, port %d\n", port)
	env.Log.Infof("NodeID:%s\n", p2phst.Config().ID.String())
	maddrs := p2phst.Addrs()
	for k, m := range maddrs {
		env.Log.Infof("Node Addrs %d:%s\n", k, m.String())
	}
	go Clear()
	return nil
}

func Clear() {
	for {
		if ClearClient() {
			time.Sleep(time.Duration(60) * time.Second)
		}
	}
}

func MessageHandler(requestData []byte, head service.Head) ([]byte, error) {
	pkarr := head.RemotePubKey
	hasher := ripemd160.New()
	hasher.Write(pkarr)
	sum := hasher.Sum(nil)
	pkarr = append(pkarr, sum[0:4]...)
	publicKey := base58.Encode(pkarr)
	res := callback(uint16(head.MsgId), requestData, publicKey)
	return res, nil
}

type OnMessageFunc func(msgType uint16, data []byte, pubkey string) []byte

var callback OnMessageFunc

func RegisterGlobalMsgHandler(call OnMessageFunc) {
	callback = call
	p2phst.RegisterGlobalMsgHandler(MessageHandler)
}
