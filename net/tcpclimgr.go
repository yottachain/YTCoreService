package net

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/multiformats/go-multiaddr"
	mnet "github.com/multiformats/go-multiaddr-net"
	"github.com/sirupsen/logrus"
	"github.com/yottachain/YTCoreService/env"
)

var ClientMgr *ClientStore

func startTcpClient(tcpconfig *Config) {
	if ClientMgr == nil {
		ClientMgr = &ClientStore{
			connects:  make(map[peer.ID]*TcpClient),
			IdLockMap: make(map[peer.ID]chan time.Time),
			cfg:       tcpconfig,
		}
		go func() {
			for {
				time.Sleep(time.Millisecond * time.Duration(env.P2P_MuteTimeout))
				ClientMgr.CheckDeadConnetion()
			}
		}()
	}
}

type ClientStore struct {
	connects map[peer.ID]*TcpClient
	sync.RWMutex
	IdLockMap map[peer.ID]chan time.Time
	cfg       *Config
}

func (cs *ClientStore) Connect(ctx context.Context, pid peer.ID, mas []multiaddr.Multiaddr) (*TcpClient, error) {
	size := len(mas)
	resChan := make(chan interface{}, size)
	var isOK int32 = 0
	for _, addr := range mas {
		go func(addr multiaddr.Multiaddr) {
			defer func() {
				if r := recover(); r != nil {
					resChan <- r.(error)
				}
			}()
			d := &mnet.Dialer{}
			if conn, err := d.DialContext(ctx, addr); err == nil {
				ytclt := NewClient(conn, &peer.AddrInfo{ID: cs.cfg.ID, Addrs: cs.cfg.Addrs()},
					cs.cfg.Privkey.GetPublic(), cs.cfg.Version,
				)
				if atomic.AddInt32(&isOK, 1) > 1 {
					conn.Close()
					resChan <- errors.New("ctx time out:connecting")
				} else {
					resChan <- ytclt
				}
			} else {
				resChan <- err
			}
		}(addr)
	}
	var errRes error
	for ii := 0; ii < size; ii++ {
		res := <-resChan
		if conn, ok := res.(*TcpClient); ok {
			return conn, nil
		} else {
			errRes = res.(error)
		}
	}
	if errRes == nil {
		return nil, fmt.Errorf("dail all maddr fail")
	}
	return nil, errRes
}

func (cs *ClientStore) Get(ctx context.Context, pid peer.ID, mas []multiaddr.Multiaddr) (*TcpClient, error) {
	if c, ok := cs.GetClient(pid); ok {
		return c, nil
	}
	cs.Lock()
	idLock, ok := cs.IdLockMap[pid]
	if !ok {
		idLock = make(chan time.Time, 1)
		idLock <- time.Unix(0, 0)
		cs.IdLockMap[pid] = idLock
	}
	cs.Unlock()
	if ctx == context.Background() {
		ctxcon, cancel := context.WithTimeout(ctx, time.Duration(env.P2P_ConnectTimeout)*time.Millisecond)
		defer cancel()
		ctx = ctxcon
	}
	select {
	case state := <-idLock:
		defer func() { idLock <- state }()
		c, ok := cs.GetClient(pid)
		if !ok {
			if time.Since(state) < time.Duration(env.P2P_ConnectTimeout)*time.Millisecond {
				return nil, fmt.Errorf("connection failed:retry frequently")
			}
			if clt, err := cs.Connect(ctx, pid, mas); err != nil {
				state = time.Now()
				return nil, err
			} else {
				state = time.Unix(0, 0)
				clt.Start(func() {
					cs.Lock()
					defer cs.Unlock()
					delete(cs.connects, pid)
				})
				cs.AddClient(pid, clt)
				return clt, nil
			}
		} else {
			return c, nil
		}
	case <-ctx.Done():
		return nil, fmt.Errorf("ctx time out:waiting to connect")
	}
}

func (cs *ClientStore) Close(pid peer.ID) error {
	clt, ok := cs.GetClient(pid)
	if !ok {
		return fmt.Errorf("no find client ID is %s", pid.Pretty())
	}
	return clt.Close()
}

func (cs *ClientStore) AddClient(pid peer.ID, c *TcpClient) {
	cs.Lock()
	defer cs.Unlock()
	cs.connects[pid] = c
}

func (cs *ClientStore) GetClient(pid peer.ID) (*TcpClient, bool) {
	cs.RLock()
	defer cs.RUnlock()
	c, ok := cs.connects[pid]
	return c, ok
}

func (cs *ClientStore) GetConnections() int {
	cs.RLock()
	defer cs.RUnlock()
	return len(cs.connects)
}

func (cs *ClientStore) CheckDeadConnetion() {
	cs.RLock()
	var cons []*TcpClient
	for _, c := range cs.connects {
		cons = append(cons, c)
	}
	cs.RUnlock()
	for _, c := range cons {
		if c.IsDazed() {
			c.Close()
		}
	}
	size := cs.GetConnections()
	if size > 0 {
		logrus.Tracef("[ClientStore]Current connections %d\n", size)
	} else {
		cs.Lock()
		defer cs.Unlock()
		cs.IdLockMap = make(map[peer.ID]chan time.Time)
	}
}
