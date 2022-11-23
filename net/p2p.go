package net

import (
	"bytes"
	"context"
	"fmt"
	"sync"

	"github.com/libp2p/go-libp2p-core/peer"
	ma "github.com/multiformats/go-multiaddr"
	"github.com/yottachain/YTCoreService/env"
	"github.com/yottachain/YTCoreService/pkt"
	"google.golang.org/protobuf/proto"
)

type Node struct {
	Id     int32
	Nodeid string
	Addrs  []string
	Pubkey string
	Weight float64
	Pool   string
	Region string

	sync.RWMutex
	PeerId peer.ID
	Maddr  []ma.Multiaddr
}

func (n *Node) PID() peer.ID {
	n.RLock()
	defer n.RUnlock()
	return n.PeerId
}

func (n *Node) Init() error {
	if n.PID() != "" {
		return nil
	}
	n.Lock()
	defer n.Unlock()
	pid, err := peer.Decode(n.Nodeid)
	if err != nil {
		return err
	}
	n.PeerId = pid
	ma, err := StringListToMaddrs(n.Addrs)
	if err != nil {
		return err
	}
	n.Maddr = ma
	return nil
}

func (n *Node) Addstrings() string {
	return AddrsToString(n.Addrs)
}

func StringListToMaddrs(addrs []string) ([]ma.Multiaddr, error) {
	maddrs := make([]ma.Multiaddr, len(addrs))
	for k, addr := range addrs {
		maddr, err := ma.NewMultiaddr(addr)
		if err != nil {
			return maddrs, err
		}
		maddrs[k] = maddr
	}
	return maddrs, nil
}

func MultiAddrsToString(addrs []ma.Multiaddr) string {
	defer env.TracePanic("[P2P]")
	var buffer bytes.Buffer
	for index, addr := range addrs {
		if index == 0 {
			buffer.WriteString("[")
			buffer.WriteString(addr.String())
		} else {
			buffer.WriteString(",")
			buffer.WriteString(addr.String())
		}
	}
	if buffer.Len() > 0 {
		buffer.WriteString("]")
	}
	return buffer.String()
}

func AddrsToString(addrs []string) string {
	var buffer bytes.Buffer
	for index, addr := range addrs {
		if index == 0 {
			buffer.WriteString("[")
			buffer.WriteString(addr)
		} else {
			buffer.WriteString(",")
			buffer.WriteString(addr)
		}
	}
	if buffer.Len() > 0 {
		buffer.WriteString("]")
	}
	return buffer.String()
}

func DoRequest(msg proto.Message, PeerId peer.ID, Maddr []ma.Multiaddr, ctl bool) (proto.Message, *pkt.ErrorMessage) {
	data, _, msgtype, merr := pkt.MarshalMsg(msg)
	if merr != nil {
		return nil, pkt.NewErrorMsg(pkt.INVALID_ARGS, merr.Error())
	}
	var mgr *ClientStore
	if ctl {
		mgr = ClientMgrForCtl()
	} else {
		mgr = ClientMgrForData()
	}
	var client *TcpClient
	if c, ok := mgr.GetClient(PeerId); !ok {
		newc, err := mgr.Get(context.Background(), PeerId, Maddr)
		if err != nil {
			return nil, pkt.NewErrorMsg(pkt.COMM_ERROR, fmt.Sprintf("%s,occurred on %s", err.Error(), MultiAddrsToString(Maddr)))
		}
		client = newc
	} else {
		client = c
	}
	res, err := client.SendMsg(context.Background(), msgtype, data)
	if err != nil {
		return nil, pkt.NewErrorMsg(pkt.COMM_ERROR, fmt.Sprintf("%s,occurred on %s", err.Error(), MultiAddrsToString(Maddr)))
	}
	resmsg := pkt.UnmarshalMsg(res)
	if errmsg, ok := resmsg.(*pkt.ErrorMessage); ok {
		return nil, errmsg
	} else {
		return resmsg, nil
	}
}

func RequestDN(msg proto.Message, dn *Node, ctl bool) (proto.Message, *pkt.ErrorMessage) {
	if e := dn.Init(); e != nil {
		return nil, pkt.NewErrorMsg(pkt.INVALID_ARGS, e.Error())
	}
	return DoRequest(msg, dn.PeerId, dn.Maddr, ctl)
}
