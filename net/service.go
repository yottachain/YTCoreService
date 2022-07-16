package net

import (
	"fmt"
	"strings"
	"time"

	"github.com/libp2p/go-libp2p-core/crypto"
	"github.com/libp2p/go-libp2p-core/peer"
	ma "github.com/multiformats/go-multiaddr"
)

type OnMessageFunc func(msgType uint16, data []byte, pubkey string) []byte

type Peer struct {
	ID    peer.ID
	Addrs []ma.Multiaddr
}

func (pi *Peer) List() []ma.Multiaddr {
	var mas = make([]ma.Multiaddr, len(pi.Addrs))
	peerMa, err := ma.NewMultiaddr(fmt.Sprintf("/p2p/%s", pi.ID))
	if err != nil {
		return nil
	}
	for k, v := range pi.Addrs {
		mas[k] = v.Encapsulate(peerMa)
	}

	return mas
}

func (pi *Peer) StringList() []string {
	var list = pi.List()
	var mastr = make([]string, len(list))
	for k, v := range list {
		mastr[k] = strings.Replace(v.String(), "ipfs", "p2p", 1)
	}
	return mastr
}

type AddrService struct {
	Info    peer.AddrInfo
	PubKey  crypto.PubKey
	Version int32
}

type PeerInfo struct {
	ID      peer.ID
	Addrs   []string
	PubKey  []byte
	Version int32
}

func (as *AddrService) RemotePeerInfo(req string, res *PeerInfo) error {
	res.ID = as.Info.ID
	pk, err := crypto.MarshalPublicKey(as.PubKey)
	if err != nil {
		return err
	}
	res.PubKey = pk
	for _, addr := range as.Info.Addrs {
		res.Addrs = append(res.Addrs, addr.String())
	}
	res.Version = as.Version
	return nil
}

type MsgId int32

type Handler func(requestData []byte, head Head) ([]byte, error)

type Head struct {
	MsgId        int32
	RemotePeerID peer.ID
	RemoteAddrs  []ma.Multiaddr
	RemotePubKey []byte
}

type MsgService struct {
	Handler
	Pi Peer
}

type Request struct {
	MsgId          int32
	ReqData        []byte
	RemotePeerInfo PeerInfo
}

type Response struct {
	Data       []byte
	ReturnTime time.Time
}

func (ms *MsgService) Ping(req string, res *string) error {
	*res = "pong"
	return nil
}

func (ms *MsgService) HandleMsg(req Request, data *Response) error {
	if ms.Handler == nil {
		return fmt.Errorf("no handler %x", req.MsgId)
	}
	head := Head{}
	head.MsgId = req.MsgId
	head.RemotePeerID = req.RemotePeerInfo.ID
	head.RemotePubKey = req.RemotePeerInfo.PubKey
	for _, v := range req.RemotePeerInfo.Addrs {
		ma, _ := ma.NewMultiaddr(v)
		head.RemoteAddrs = append(head.RemoteAddrs, ma)
	}
	if resdata, err := ms.Handler(req.ReqData, head); err != nil {
		return err
	} else {
		data.Data = resdata
		data.ReturnTime = time.Now()
		return nil
	}
}
