package net

import (
	"fmt"

	"github.com/aurawing/eos-go/btcsuite/btcutil/base58"
	"github.com/libp2p/go-libp2p-core/crypto"
	"github.com/multiformats/go-multiaddr"
	ma "github.com/multiformats/go-multiaddr"
	"github.com/sirupsen/logrus"
	YTinterface "github.com/yottachain/YTHost/interface"
	"github.com/yottachain/YTHost/newHost"
	"github.com/yottachain/YTHost/option"
	"github.com/yottachain/YTHost/service"
	"golang.org/x/crypto/ripemd160"
)

var serverhost *newHost.HostPool
var p2phst YTinterface.Host

func Stop() {

}

func Start(port int32, port2 int32, privatekey string) error {
	privbytes := base58.Decode(privatekey)
	if privbytes == nil || len(privbytes) == 0 {
		logrus.Panicf("[Booter]Bad format of private key,Base58 format needed")
	}
	pk, err := crypto.UnmarshalSecp256k1PrivateKey(privbytes[1:33])
	if err != nil {
		logrus.Panicf("[Booter]Bad format of private key")
	}
	addrs := []multiaddr.Multiaddr{}
	add1 := fmt.Sprintf("/ip4/0.0.0.0/tcp/%d", port)
	ma1, _ := ma.NewMultiaddr(add1)
	addrs = append(addrs, ma1)
	if port2 > 0 {
		add2 := fmt.Sprintf("/ip4/0.0.0.0/tcp/%d/http", port2)
		ma2, _ := ma.NewMultiaddr(add2)
		addrs = append(addrs, ma2)
	}
	serverhost = newHost.NewHost(addrs, option.Identity(pk))
	if len(serverhost.Hosts) < 1 {
		logrus.Panicf("[Booter]Init ERR.\n")
	}
	p2phst = serverhost.Hosts[0]
	for _, hst := range serverhost.Hosts {
		logrus.Infof("[Booter]P2P initializing...NodeID:%s\n", hst.Config().ID.String())
		maddrs := hst.Addrs()
		for k, m := range maddrs {
			logrus.Infof("[Booter]Node Addrs %d:%s\n", k, m.String())
		}
	}
	go serverhost.Accept()
	return nil
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
	serverhost.RegisterGlobalMsgHandler(MessageHandler)
}
