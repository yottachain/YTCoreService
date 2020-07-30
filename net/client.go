package net

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/sirupsen/logrus"

	"github.com/libp2p/go-libp2p-core/peer"
	ma "github.com/multiformats/go-multiaddr"
	"github.com/yottachain/YTCoreService/env"
	"github.com/yottachain/YTCoreService/pkt"
)

var Conntimeout = readTimeout("P2PHOST_CONNECTTIMEOUT")
var DirectConntimeout = env.CheckInt(Conntimeout/10, 500, 5000)
var Writetimeout = readTimeout("P2PHOST_WRITETIMEOUT")
var DirectWritetimeout = env.CheckInt(Writetimeout/10, 500, 5000)

func readTimeout(key string) int {
	ct := os.Getenv(key)
	timeout := 60
	if ct != "" {
		ii, err := strconv.Atoi(ct)
		if err == nil {
			timeout = ii
		}
	}
	return timeout
}

var connects = struct {
	sync.RWMutex
	cons map[string]*TcpClient
}{cons: make(map[string]*TcpClient)}

func ClearClient() bool {
	var del_id string = ""
	var del_c *TcpClient
	connects.RLock()
	for k, v := range connects.cons {
		if !v.IsActive() {
			del_id = k
			del_c = v
			break
		}
	}
	if del_id == "" {
		connects.RUnlock()
		return true
	} else {
		connects.RUnlock()
		connects.Lock()
		delete(connects.cons, del_id)
		del_c.DisConnect()
		connects.Unlock()
		return false
	}
}

func NewClient(pid string, nowait bool) (*TcpClient, *pkt.ErrorMessage) {
	connects.RLock()
	con := connects.cons[pid]
	connects.RUnlock()
	var err *pkt.ErrorMessage
	if con == nil {
		connects.Lock()
		con = connects.cons[pid]
		if con == nil {
			con, err = NewP2P(pid, nowait)
			if err == nil {
				connects.cons[pid] = con
			}
		}
		connects.Unlock()
	}
	return con, nil
}

func RemoveClient(pid string) {
	connects.Lock()
	con := connects.cons[pid]
	if con != nil {
		delete(connects.cons, pid)
		con.DisConnect()
	}
	connects.Unlock()
}

type TcpClient struct {
	lastTime      *int64
	connectedTime *int64
	statu         *int32
	PeerId        peer.ID
	nowait        bool
	sync.Mutex
}

func NewP2P(key string, nowait bool) (*TcpClient, *pkt.ErrorMessage) {
	c := &TcpClient{}
	c.lastTime = new(int64)
	c.nowait = nowait
	c.statu = new(int32)
	atomic.StoreInt32(c.statu, 0)
	c.connectedTime = new(int64)
	atomic.StoreInt64(c.connectedTime, 0)
	id, err := peer.Decode(key)
	if err != nil {
		logmsg := fmt.Sprintf("[P2P]PeerID %s INVALID.\n", err.Error())
		logrus.Errorf(logmsg)
		return nil, pkt.NewErrorMsg(pkt.INVALID_ARGS, logmsg)
	}
	c.PeerId = id
	return c, nil
}

func (client *TcpClient) IsActive() bool {
	return time.Now().Unix()-atomic.LoadInt64(client.lastTime) <= env.CONN_EXPIRED
}

func (client *TcpClient) Request(msgid int32, data []byte, addrs []string, log_pre string) (proto.Message, *pkt.ErrorMessage) {
	if atomic.LoadInt32(client.statu) == 1 {
		addrString := AddrsToString(addrs)
		logmsg := fmt.Sprintf("[P2P]%s%s Connection destroyed!\n", log_pre, addrString)
		logrus.Errorf(logmsg)
		return nil, pkt.NewErrorMsg(pkt.COMM_ERROR, logmsg)
	}
	atomic.StoreInt64(client.lastTime, time.Now().Unix())
	err := client.connect(addrs, log_pre)
	if err != nil {
		return nil, err
	}
	timeout := time.Millisecond * time.Duration(Writetimeout)
	if client.nowait {
		timeout = time.Millisecond * time.Duration(DirectWritetimeout)
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	res, serr := p2phst.SendMsg(ctx, client.PeerId, msgid, data)
	if serr != nil {
		addrString := AddrsToString(addrs)
		logmsg := fmt.Sprintf("[P2P]%s%s COMM_ERROR:%s\n", log_pre, addrString, serr.Error())
		logrus.Errorf(logmsg)
		if atomic.LoadInt32(client.statu) != 2 {
			atomic.StoreInt64(client.connectedTime, 0)
		}
		return nil, pkt.NewErrorMsg(pkt.COMM_ERROR, logmsg)
	}
	msg := pkt.UnmarshalMsg(res)
	if errmsg, ok := msg.(*pkt.ErrorMessage); ok {
		return nil, errmsg
	} else {
		return msg, nil
	}
}

func (client *TcpClient) connect(addrs []string, log_pre string) *pkt.ErrorMessage {
	if atomic.LoadInt64(client.connectedTime) >= 0 {
		client.Lock()
		defer client.Unlock()
		atomic.StoreInt32(client.statu, 2)
		defer atomic.StoreInt32(client.statu, 0)
		contime := atomic.LoadInt64(client.connectedTime)
		if contime >= 0 {
			addrString := AddrsToString(addrs)
			if time.Now().Unix()-contime < 5 {
				logmsg := fmt.Sprintf("[P2P]%s%s did not connect successfully 5 seconds ago\n", log_pre, addrString)
				logrus.Errorf(logmsg)
				return pkt.NewErrorMsg(pkt.COMM_ERROR, logmsg)
			} else {
				maddrs, err := StringListToMaddrs(addrs)
				if err != nil {
					atomic.StoreInt64(client.connectedTime, time.Now().Unix())
					logmsg := fmt.Sprintf("[P2P]%sAddrs %s ERR:%s\n", log_pre, addrString, err.Error())
					logrus.Errorf(logmsg)
					return pkt.NewErrorMsg(pkt.INVALID_ARGS, logmsg)
				}
				timeout := time.Millisecond * time.Duration(Conntimeout)
				if client.nowait {
					timeout = time.Millisecond * time.Duration(DirectConntimeout)
				}
				ctx, cancel := context.WithTimeout(context.Background(), timeout)
				defer cancel()
				_, err = p2phst.ClientStore().Get(ctx, client.PeerId, maddrs)
				if err != nil {
					atomic.StoreInt64(client.connectedTime, time.Now().Unix())
					logmsg := fmt.Sprintf("[P2P]%sConnect %s ERR:%s\n", log_pre, addrString, err.Error())
					logrus.Errorf(logmsg)
					return pkt.NewErrorMsg(pkt.COMM_ERROR, logmsg)
				}
				atomic.StoreInt64(client.connectedTime, -1)
			}
		}
	}
	return nil
}

func (client *TcpClient) DisConnect() {
	atomic.StoreInt32(client.statu, 1)
	p2phst.ClientStore().Close(client.PeerId)
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
