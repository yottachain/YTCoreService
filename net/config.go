package net

import (
	"crypto/rand"
	"fmt"

	"github.com/libp2p/go-libp2p-core/crypto"

	"github.com/libp2p/go-libp2p-core/peer"
	ma "github.com/multiformats/go-multiaddr"
	mnet "github.com/multiformats/go-multiaddr-net"
)

type Config struct {
	ListenAddr ma.Multiaddr
	Privkey    crypto.PrivKey
	ID         peer.ID
	Version    int32
}

func DefaultConfig() *Config {
	cfg := new(Config)
	maddr, _ := ma.NewMultiaddr("/ip4/0.0.0.0/tcp/8001")
	cfg.ListenAddr = maddr
	pi, _, _ := crypto.GenerateSecp256k1Key(rand.Reader)
	id, _ := peer.IDFromPrivateKey(pi)
	cfg.ID = id
	cfg.Privkey = pi
	return cfg
}

func NewConfig(options ...Option) *Config {
	cfg := new(Config)
	for _, bindOp := range options {
		bindOp(cfg)
	}
	return cfg
}

func (cfg *Config) Addrs() []ma.Multiaddr {
	port, err := cfg.ListenAddr.ValueForProtocol(ma.P_TCP)
	if err != nil {
		return nil
	}
	tcpMa, err := ma.NewMultiaddr(fmt.Sprintf("/tcp/%s", port))
	if err != nil {
		return nil
	}
	var res []ma.Multiaddr
	maddrs, err := mnet.InterfaceMultiaddrs()
	if err != nil {
		return nil
	}
	for _, ma := range maddrs {
		newMa := ma.Encapsulate(tcpMa)
		if mnet.IsIPLoopback(newMa) {
			continue
		}
		res = append(res, newMa)
	}
	return res
}

type Option func(cfg *Config)

func ListenAddr(ma ma.Multiaddr) Option {
	return func(cfg *Config) {
		cfg.ListenAddr = ma
	}
}

func Identity(key crypto.PrivKey) Option {
	return func(cfg *Config) {
		cfg.Privkey = key
		id, _ := peer.IDFromPrivateKey(cfg.Privkey)
		cfg.ID = id
	}
}

func Version(v int32) Option {
	return func(cfg *Config) {
		cfg.Version = v
	}
}
