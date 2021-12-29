package eospledge

import (
	"encoding/json"
	"fmt"

	"github.com/eoscanada/eos-go"
	_ "github.com/eoscanada/eos-go/system"
	_ "github.com/eoscanada/eos-go/token"
)

//GetPledgeData get pledge data of one miner
func GetPledgeData(url string, minerid uint64) (*PledgeData, error) {
	api := eos.New(url)
	req := eos.GetTableRowsRequest{
		Code:       "hdddeposit12",
		Scope:      "hdddeposit12",
		Table:      "miner2dep",
		LowerBound: fmt.Sprintf("%d", minerid),
		UpperBound: fmt.Sprintf("%d", minerid),
		Limit:      1,
		KeyType:    "i64",
		Index:      "1",
		JSON:       true,
	}
	resp, err := api.GetTableRows(req)
	if err != nil {
		return nil, fmt.Errorf("get table row failed, minerid: %d", minerid)
	}
	if resp.More == true {
		return nil, fmt.Errorf("more than one rows returned, minerid: %d", minerid)
	}
	rows := make([]PledgeData, 0)
	err = json.Unmarshal(resp.Rows, &rows)
	if err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return nil, fmt.Errorf("no matched row found, minerid: %s", req.Scope)
	}
	return &rows[0], nil
}

func GetDepStore(url, accountName string) (*DepStoreData, error) {
	api := eos.New(url)
	req := eos.GetTableRowsRequest{
		Code:       "hdddeposit12",
		Scope:      accountName,
		Table:      "depstore",
		LowerBound: accountName,
		UpperBound: accountName,
		Limit:      1,
		KeyType:    "name",
		Index:      "1",
		JSON:       true,
	}
	resp, err := api.GetTableRows(req)
	if err != nil {
		return nil, fmt.Errorf("get table row failed, accountName: %s", accountName)
	}
	if resp.More == true {
		return nil, fmt.Errorf("more than one rows returned, accountName: %s", accountName)
	}
	rows := make([]DepStoreData, 0)
	err = json.Unmarshal(resp.Rows, &rows)
	if err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return nil, fmt.Errorf("no matched row found, accountName: %s", req.Scope)
	}
	return &rows[0], nil
}
