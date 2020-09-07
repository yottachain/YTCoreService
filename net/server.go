package net

import (
	"errors"
	"fmt"
	"time"

	crypto "github.com/libp2p/go-libp2p-core/crypto"
	"github.com/mr-tron/base58"
	ma "github.com/multiformats/go-multiaddr"
	"github.com/sirupsen/logrus"
	hst "github.com/yottachain/YTHost"
	host "github.com/yottachain/YTHost/interface"
	"github.com/yottachain/YTHost/option"
	"github.com/yottachain/YTHost/service"
	"golang.org/x/crypto/ripemd160"
)

var p2phst host.Host

func Stop() {

}

func Start(port int32, port2 int32, privatekey string) error {
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
	logrus.Infof("[Booter]P2P initialization completed, port %d\n", port)
	logrus.Infof("[Booter]NodeID:%s\n", p2phst.Config().ID.String())
	maddrs := p2phst.Addrs()
	for k, m := range maddrs {
		logrus.Infof("[Booter]Node Addrs %d:%s\n", k, m.String())
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
	//serverhost.RegisterHandler(0x1c, MessageHandler)
}
