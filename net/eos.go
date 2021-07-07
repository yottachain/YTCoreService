package net

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"
	"time"

	"github.com/aurawing/eos-go"
	"github.com/patrickmn/go-cache"
	"github.com/sirupsen/logrus"
	"github.com/yottachain/YTCoreService/env"
)

func AuthUserInfo(publickey, name string, retrytimes int) bool {
	count := 0
	for {
		res, err := checkUserInfo(publickey, name)
		if err != nil {
			time.Sleep(time.Duration(1000) * time.Millisecond)
			count++
			if count >= retrytimes {
				logrus.Errorf("[EOS]AuthUser:%s,publickey:%d,ERR:%s\n", name, publickey, err)
				return false
			}
		} else {
			return res
		}
	}
}

func checkUserInfo(publickey, name string) (bool, error) {
	if env.EOSAPI == "NA" {
		return true, nil
	}
	jsonkey := fmt.Sprintf("{\"public_key\":\"%s%s\"}", "YTA", publickey)
	resp, err := http.Post(env.EOSAPI, "application/x-www-form-urlencoded", strings.NewReader(jsonkey))
	if err != nil {
		return false, err
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return false, err
	}
	if strings.Contains(string(body), "\""+name+"\"") {
		return true, nil
	}
	return false, nil
}

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
	_, err := RequestWRetry("sethfee", obj, 8)
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
		_, err := RequestWRetry("addhspace", obj, 8)
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
	_, err := RequestWRetry("subbalance", obj, 8)
	return err
}

type GetBalanceReq struct {
	Owner  eos.AccountName `json:"owner"`
	UType  uint8           `json:"utype"`
	Caller eos.AccountName `json:"caller"`
}

func GetBalance(username string) (v int64, err error) {
	if !env.BP_ENABLE {
		return 10000000, nil
	}
	defer func() {
		if r := recover(); r != nil {
			ss := env.TraceErrors("[EOS]")
			if strings.ContainsAny(ss, "user not a account") {
				v = -5
				err = errors.New("user not a account")
			} else {
				v = 0
				err = errors.New("Unknown error")
			}
		}
	}()
	obj := GetBalanceReq{Owner: eos.AN(username),
		UType: 2, Caller: eos.AN(env.BPAccount)}
	res, err := RequestWRetry("getbalance", obj, 3)
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

func RequestWRetry(actname string, obj interface{}, retrytimes int) (*eos.PushTransactionFullResp, error) {
	count := 0
	for {
		URI := GetEOSURI()
		res, err := Request(actname, obj, URI)
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

func Request(actname string, obj interface{}, URI *EOSURI) (*eos.PushTransactionFullResp, error) {
	api, err := URI.NewApi()
	if err != nil {
		logrus.Errorf("[EOS]New Api,url:%s,ERR:%s\n", URI.Url, err)
		return nil, err
	}
	action := &eos.Action{
		Account: eos.AN(env.ContractAccount),
		Name:    eos.ActN(actname),
		Authorization: []eos.PermissionLevel{
			{Actor: eos.AN(env.ShadowAccount), Permission: eos.PN("active")},
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
