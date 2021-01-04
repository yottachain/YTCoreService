package net

import (
	"encoding/json"
	"errors"
	"strings"
	"time"

	"github.com/aurawing/eos-go"
	"github.com/patrickmn/go-cache"
	"github.com/sirupsen/logrus"
	"github.com/yottachain/YTCoreService/env"
)

type BalanceValue struct {
	Balance int64
}

var USER_Banlance_CACHE = cache.New(30*time.Second, 15*time.Second)

func HasSpace(length uint64, username string) (bool, error) {
	if !env.BP_ENABLE {
		return true, nil
	}
	var balan BalanceValue
	v, found := USER_Banlance_CACHE.Get(username)
	if !found {
		balance, err := GetBalance(username)
		if err != nil {
			return false, err
		}
		logrus.Infof("[EOS]Get [%s] balance:%d\n", username, balance)
		balan = BalanceValue{Balance: balance}
		USER_Banlance_CACHE.Set(username, balan, cache.DefaultExpiration)
	} else {
		balan = v.(BalanceValue)
	}
	needcost := int64(env.UnitFirstCost * length / env.UnitSpace)
	return balan.Balance > needcost, nil
}

type SetHfeeReq struct {
	Owner  eos.AccountName `json:"owner"`
	Cost   uint64          `json:"cost"`
	Caller eos.AccountName `json:"caller"`
}

func SetHfee(username string, cost uint64) error {
	if !env.BP_ENABLE {
		return nil
	}
	obj := SetHfeeReq{Owner: eos.AN(username), Cost: cost,
		Caller: eos.AN(env.BPAccount)}
	_, err := RequestWRetry("sethfee", obj, 8, "", env.ShadowAccount)
	return err
}

type AddUsedSpaceReq struct {
	Owner  eos.AccountName `json:"owner"`
	Length uint64          `json:"length"`
	Caller eos.AccountName `json:"caller"`
}

func AddUsedSpace(username string, length uint64) error {
	return nil
	/*
		if !env.BP_ENABLE {
			return nil
		}
		obj := AddUsedSpaceReq{Owner: eos.AN(username), Length: length,
			Caller: eos.AN(env.BPAccount)}
		_, err := RequestWRetry("addhspace", obj, 8,"",env.ShadowAccount)
		return err
	*/
}

type SubBalanceReq struct {
	Owner  eos.AccountName `json:"owner"`
	Cost   uint64          `json:"cost"`
	UType  uint8           `json:"utype"`
	Caller eos.AccountName `json:"caller"`
}

func SubBalance(username string, cost uint64) error {
	if !env.BP_ENABLE {
		return nil
	}
	obj := SubBalanceReq{Owner: eos.AN(username), Cost: cost,
		UType: 2, Caller: eos.AN(env.BPAccount)}
	_, err := RequestWRetry("subbalance", obj, 8, "", env.ShadowAccount)
	return err
}

type GetBalanceReq struct {
	Owner  eos.AccountName `json:"owner"`
	UType  uint8           `json:"utype"`
	Caller eos.AccountName `json:"caller"`
}

func LoginInfo(username, privkey string) (string, error) {
	obj := GetBalanceReq{Owner: eos.AN(username),
		UType: 1, Caller: eos.AN(username)}
	URI := NewEOSURI("http://localhost:8888")
	api, err := URI.NewUserApi(privkey)
	if err != nil {
		logrus.Errorf("[EOS]LoginInfo,url:%s,ERR:%s\n", URI.Url, err)
		return "", err
	}
	action := &eos.Action{
		Account: eos.AN(env.ContractAccount),
		Name:    eos.ActN(username),
		Authorization: []eos.PermissionLevel{
			{Actor: eos.AN(username), Permission: eos.PN("active")},
		},
		ActionData: eos.NewActionData(obj),
	}
	txOpts := &eos.TxOptions{}
	/*

		if err = txOpts.FillFromChain(api); err != nil {
			logrus.Errorf("[EOS]Filling tx opts: %s\n", err)
			return "", err
		}
	*/
	tx := eos.NewTransaction([]*eos.Action{action}, txOpts)
	tx.SetExpiration(URI.GetExpiration())
	_, packedTx, err := api.SignTransaction(tx, txOpts.ChainID, eos.CompressionNone)
	if err != nil {
		logrus.Errorf("[EOS]Sign transaction: %s\n", err)
		return "", err
	}
	txt, err := json.Marshal(packedTx)
	if err != nil {
		logrus.Errorf("[EOS]Marshal packedTx: %s\n", err)
		return "", err
	}
	return string(txt), nil
}

func PushLogin(tx string) error {
	if !env.BP_ENABLE {
		return nil
	}
	res, err := PushTxWRetry(tx, 3)
	if err != nil {
		return err
	} else {
		console := res.Processed.ActionTraces[0].Console
		index := strings.Index(console, "{\"balance\":")
		console = console[index:]
		index = strings.Index(console, "}")
		console = console[:index+1]
		balance := &BalanceValue{}
		err = json.Unmarshal([]byte(console), balance)
		if err != nil {
			logrus.Errorf("[EOS]Unmarshal '%s' ERR:%s\n", console, err)
			return err
		}
		logrus.Infof("[EOS]PushLogin %s,pass:%d\n", tx, balance.Balance)
		return nil
	}
}

func Login(username, privkey string) error {
	if !env.BP_ENABLE {
		return nil
	}
	obj := GetBalanceReq{Owner: eos.AN(username),
		UType: 1, Caller: eos.AN(username)}
	res, err := RequestWRetry("getbalance", obj, 3, privkey, username)
	if err != nil {
		return err
	} else {
		console := res.Processed.ActionTraces[0].Console
		index := strings.Index(console, "{\"balance\":")
		console = console[index:]
		index = strings.Index(console, "}")
		console = console[:index+1]
		balance := &BalanceValue{}
		err = json.Unmarshal([]byte(console), balance)
		if err != nil {
			logrus.Errorf("[EOS]Unmarshal '%s' ERR:%s\n", console, err)
			return err
		}
		logrus.Infof("[EOS]User %s login pass:%d\n", username, balance.Balance)
		return nil
	}
}

func GetBalance(username string) (v int64, err error) {
	if !env.BP_ENABLE {
		return 10000000, nil
	}
	defer func() {
		if r := recover(); r != nil {
			env.TraceError("[EOS]")
			v = 0
			err = errors.New("Unknown error")
		}
	}()
	obj := GetBalanceReq{Owner: eos.AN(username),
		UType: 2, Caller: eos.AN(env.BPAccount)}
	res, err := RequestWRetry("getbalance", obj, 3, "", env.ShadowAccount)
	if err != nil {
		return 0, err
	}
	console := res.Processed.ActionTraces[0].Console
	index := strings.Index(console, "{\"balance\":")
	console = console[index:]
	index = strings.Index(console, "}")
	console = console[:index+1]
	balance := &BalanceValue{}
	err = json.Unmarshal([]byte(console), balance)
	if err != nil {
		logrus.Errorf("[EOS]Unmarshal '%s' ERR:%s\n", console, err)
		return 0, err
	}
	return balance.Balance, nil
}

func RequestWRetry(actname string, obj interface{}, retrytimes int, privkey, username string) (*eos.PushTransactionFullResp, error) {
	count := 0
	for {
		URI := GetEOSURI()
		res, err := Request(actname, obj, URI, privkey, username)
		if err != nil {
			if strings.ContainsAny(err.Error(), "the fee is the same") {
				return nil, nil
			}
			URI.SetErr(err)
			count++
			if count >= retrytimes {
				return nil, err
			}
		} else {
			return res, nil
		}
	}
}

func Request(actname string, obj interface{}, URI *EOSURI, privkey, username string) (*eos.PushTransactionFullResp, error) {
	var api *eos.API
	var err error
	if privkey == "" {
		api, err = URI.NewApi()
	} else {
		api, err = URI.NewUserApi(privkey)
	}
	if err != nil {
		logrus.Errorf("[EOS]New Api,url:%s,ERR:%s\n", URI.Url, err)
		return nil, err
	}
	action := &eos.Action{
		Account: eos.AN(env.ContractAccount),
		Name:    eos.ActN(actname),
		Authorization: []eos.PermissionLevel{
			{Actor: eos.AN(username), Permission: eos.PN("active")},
		},
		ActionData: eos.NewActionData(obj),
	}
	txOpts := &eos.TxOptions{}
	if err = txOpts.FillFromChain(api); err != nil {
		logrus.Errorf("[EOS]Filling tx opts: %s\n", err)
		return nil, err
	}
	tx := eos.NewTransaction([]*eos.Action{action}, txOpts)
	tx.SetExpiration(URI.GetExpiration())
	_, packedTx, err := api.SignTransaction(tx, txOpts.ChainID, eos.CompressionNone)
	if err != nil {
		logrus.Errorf("[EOS]Sign transaction: %s\n", err)
		return nil, err
	}
	res, err := api.PushTransaction(packedTx)
	if err != nil {
		logrus.Errorf("[EOS]Push %s transaction: %s\n", actname, err)
		return nil, err
	}
	return res, nil
}

func PushTxWRetry(txstr string, retrytimes int) (*eos.PushTransactionFullResp, error) {
	tx := &eos.PackedTransaction{}
	err := json.Unmarshal([]byte(txstr), tx)
	if err != nil {
		logrus.Error("[EOS]Unmarshal %s Err:%s\n", txstr, err)
		return nil, err
	}
	count := 0
	for {
		URI := GetEOSURI()
		res, err := Pushtx(txstr, tx, URI)
		if err != nil {
			URI.SetErr(err)
			count++
			if count >= retrytimes {
				return nil, err
			}
		} else {
			return res, nil
		}
	}
}

func Pushtx(txstr string, tx *eos.PackedTransaction, URI *EOSURI) (*eos.PushTransactionFullResp, error) {
	api, err := URI.NewApi()
	if err != nil {
		logrus.Errorf("[EOS]New Api,url:%s,ERR:%s\n", URI.Url, err)
		return nil, err
	}
	res, err := api.PushTransaction(tx)
	if err != nil {
		logrus.Errorf("[EOS]Push %s transaction: %s\n", txstr, err)
		return nil, err
	}
	return res, nil
}
