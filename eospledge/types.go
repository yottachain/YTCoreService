package eospledge

import (
	"encoding/json"
	"fmt"
	"sync"

	eos "github.com/eoscanada/eos-go"
)

type FlexString string

func (fi *FlexString) UnmarshalJSON(b []byte) error {
	if b[0] != '"' {
		var intvalue int64
		err := json.Unmarshal(b, &intvalue)
		if err != nil {
			return err
		}
		*fi = FlexString(fmt.Sprintf("%d", intvalue))
		return nil
	} else {
		*fi = FlexString(string(b[1 : len(b)-1]))
		return nil
	}
}

type EosTX struct {
	API            *eos.API
	BpAccount      string
	ContractOwnerM string
	ContractOwnerD string
	ShadowAccount  string
	PrivateKey     string
	sync.RWMutex
}

type RegMiner struct {
	MinerID   uint64          `json:"minerid"`
	Owner     eos.AccountName `json:"adminacc"`
	DepAcc    eos.AccountName `json:"dep_acc"`
	DepAmount eos.Asset       `json:"dep_amoun"`
	Extra     string          `json:"extra"`
}

type ChangeMinerPool struct {
	MinerID     uint64          `json:"minerid"`
	PoolID      eos.AccountName `json:"pool_id"`
	MinerProfit eos.AccountName `json:"minerowner"`
	MaxSpace    uint64          `json:"max_space"`
}

type Miner struct {
	Owner   eos.AccountName `json:"owner"`
	MinerID uint64          `json:"minerid"`
	Caller  eos.AccountName `json:"caller"`
}

type Profit struct {
	Owner   eos.AccountName `json:"owner"`
	MinerID uint64          `json:"minerid"`
	Space   uint64          `json:"space"`
	Caller  eos.AccountName `json:"caller"`
}

type PledgeData struct {
	MinerID     uint32    `json:"minerid"`
	AccountName string    `json:"account_name"`
	Deposit     eos.Asset `json:"deposit"`
	Total       eos.Asset `json:"dep_total"`
}

type PoolInfo struct {
	Owner     eos.AccountName `json:"pool_owner"`
	PoolID    eos.AccountName `json:"pool_id"`
	MaxSpace  FlexString      `json:"max_space"`
	SpaceLeft FlexString      `json:"space_left"`
}

type PayForfeit struct {
	User    eos.AccountName `json:"user"`
	MinerID uint64          `json:"minerid"`
	Quant   eos.Asset       `json:"quant"`
	AccType uint8           `json:"acc_type"`
	Caller  eos.AccountName `json:"caller"`
}

type DrawForfeit struct {
	User    eos.AccountName `json:"user"`
	AccType uint8           `json:"acc_type"`
	Caller  eos.AccountName `json:"caller"`
}

type MActive struct {
	Owner   eos.AccountName `json:"owner"`
	MinerID uint64          `json:"minerid"`
	Caller  eos.AccountName `json:"caller"`
}

// YTASymbol represents the standard YTA symbol on the chain.
var YTASymbol = eos.Symbol{Precision: 4, Symbol: "YTA"}

type DepStoreData struct {
	AccountName  string    `json:"account_name"`
	DepositTotal eos.Asset `json:"deposit_total"`
	Reserved1    eos.Asset `json:"reserved1"`
	Reserved2    uint64    `json:"reserved2"`
	Reserved3    uint64    `json:"reserved3"`
}
