package test

import (
	"github.com/sirupsen/logrus"
	"github.com/yottachain/YTCoreService/net"
	"github.com/yottachain/YTCoreService/pkt"
	"github.com/yottachain/YTDNMgmt"
)

func TestP2pServer() {
	net.Start(8888, -1, "5JdDoNZwSADC3KG7xCh7mCF62fKp86sLf3GUNDY2B8t2UUB9HdJ")
	//net.RegisterGlobalMsgHandler()
}

func TestP2pClient() {
	net.Start(5555, -1, "5KQKydL7TuRwjzaFSK4ezH9RUXWuYHW1yYDp5CmQfsfTuu9MBLZ")
	node := &YTDNMgmt.SuperNode{
		ID:     0,
		NodeID: "16Uiu2HAm44FX3YuzGXJgHMqnyMM5zCzeT6PUoBNZkz66LutfRREM",
		Addrs:  []string{"/ip4/192.168.3.75/tcp/8888"},
	}
	req := &pkt.DownloadShardReq{
		VHF: []byte("ss"),
	}
	_, err := net.RequestSN(req, node, "", 1, false)
	if err != nil {
		logrus.Infof("res:%d,%s\n", err.Code, err.Msg)
	} else {
		logrus.Infof("msg\n")
	}

}
