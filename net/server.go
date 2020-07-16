package net

import (
	"errors"
	"fmt"
	"time"

	crypto "github.com/libp2p/go-libp2p-core/crypto"
	"github.com/mr-tron/base58"
	"github.com/multiformats/go-multiaddr"
	ma "github.com/multiformats/go-multiaddr"
	"github.com/sirupsen/logrus"
	host "github.com/yottachain/YTHost"
	YTinterface "github.com/yottachain/YTHost/interface"
	"github.com/yottachain/YTHost/newHost"
	"github.com/yottachain/YTHost/option"
	"github.com/yottachain/YTHost/service"
	"golang.org/x/crypto/ripemd160"
)

<<<<<<< HEAD
var p2phst host.Host
=======
var serverhost *newHost.HostPool
var p2phst YTinterface.Host
>>>>>>> 2b2cf5be6901dec4355a317ae048accecdb6237d

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
	addrs := []multiaddr.Multiaddr{}
	if port > 0 {
		add1 := fmt.Sprintf("/ip4/0.0.0.0/tcp/%d", port)
		logrus.Infof("[Booter]P2P initializing..., binding %s\n", add1)
		ma1, _ := ma.NewMultiaddr(add1)
		addrs = append(addrs, ma1)
	}
	if port2 > 0 {
		add2 := fmt.Sprintf("/ip4/0.0.0.0/tcp/%d/http", port2)
		logrus.Infof("[Booter]P2P initializing..., binding %s\n", add2)
		ma2, _ := ma.NewMultiaddr(add2)
		addrs = append(addrs, ma2)
	}
	serverhost = newHost.NewHost(addrs, option.Identity(pk))
	hst, _ := host.NewHost()
	p2phst = hst
	go serverhost.Accept()
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
<<<<<<< HEAD
	p2phst.RegisterGlobalMsgHandler(MessageHandler)
	//serverhost.RegisterHandler(0x1c, MessageHandler)
=======
	serverhost.RegisterHandler(0x1c, MessageHandler)
>>>>>>> 2b2cf5be6901dec4355a317ae048accecdb6237d
}
