package eos

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/aurawing/eos-go"
	"github.com/shopspring/decimal"
	"github.com/sirupsen/logrus"
	"github.com/yottachain/YTCoreService/dao"
	"github.com/yottachain/YTCoreService/env"
)

type PledgeData struct {
	MinerID     uint32    `json:"minerid"`
	AccountName string    `json:"account_name"`
	Deposit     eos.Asset `json:"deposit"`
	Total       eos.Asset `json:"dep_total"`
}

func GetPledgeData(minerid uint64) (*PledgeData, error) {
	URI := GetEOSURI()
	api, err := URI.NewApi()
	if err != nil {
		logrus.Errorf("[EOS]New Api,url:%s,ERR:%s\n", URI.Url, err)
		return nil, err
	}
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
	if resp.More {
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

type DepStoreData struct {
	AccountName  string    `json:"account_name"`
	DepositTotal eos.Asset `json:"deposit_total"`
	Reserved1    eos.Asset `json:"reserved1"`
	Reserved2    uint64    `json:"reserved2"`
	Reserved3    uint64    `json:"reserved3"`
}

func GetDepStore(accountName string) (*DepStoreData, error) {
	URI := GetEOSURI()
	api, err := URI.NewApi()
	if err != nil {
		logrus.Errorf("[EOS]New Api,url:%s,ERR:%s\n", URI.Url, err)
		return nil, err
	}
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
		return nil, fmt.Errorf("get table row failed, accountName: %s, err: %v", accountName, err)
	}
	if resp.More {
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

func CheckFreeSpace(userID int32) (bool, error) {
	user, err := getUserPledgeInfo(userID)
	if err != nil {
		logrus.Errorf("[PledgeSpace][%d]GetUserPledgeInfo ERR:%s\n", userID, err)
		return false, err
	}
	if user.Usedspace >= user.PledgeFreeSpace {
		return false, nil
	}
	return true, nil
}

func getUserPledgeInfo(userID int32) (*dao.User, error) {
	user := dao.GetUserByUserId(userID)
	if user == nil {
		return nil, fmt.Errorf("user is null")
	}
	if user.PledgeFreeAmount == 0 || time.Since(time.Unix(user.PledgeUpdateTime, 0)).Seconds() > float64(env.PLEDGE_SPACE_UPDATE_INTERVAL) {
		depData, err := GetDepStore(user.Username)
		if err != nil {
			logrus.Errorf("[PledgeSpace][%d]GetDepStore ERR:%s\n", userID, err)
			return nil, err
		} else {
			amount := int64(depData.DepositTotal.Amount)
			user.PledgeFreeAmount = float64(amount / 10000)
			user.PledgeFreeSpace = calcPledgeFreeSpace(user.PledgeFreeAmount)
			user.PledgeUpdateTime = time.Now().Unix()
			err = dao.UpdateUserPledgeInfo(userID, user.PledgeFreeAmount, user.PledgeFreeSpace)
			if err != nil {
				logrus.Errorf("[PledgeSpace][%d]UpdateUserPledgeInfo ERR:%s\n", userID, err)
				return nil, err
			}
		}

	}
	return user, nil
}

func calcPledgeFreeSpace(amount float64) int64 {
	for _, levelInfo := range env.PLEDGE_SPACE_FEE {
		if amount >= float64(levelInfo.Level) {
			return int64(levelInfo.Fee * int(amount))
		}
	}
	return 0
}

type UserDeposit struct {
	Username        string  `json:"username"`
	PledgeFreeSpace float64 `json:"free_space"`
	Usedspace       float64 `json:"used_space"`
}

func CheckUndepStore(username string) error {
	user := dao.GetUserByUsername(username)
	if user == nil {
		logrus.Infof("[PledgeSpace][username=%s]User is null.\n", username)
		err := UndepStore(username)
		if err != nil {
			logrus.Errorf("[PledgeSpace][%d]UndepStore ERR:%s\n", user.UserID, err)
			return err
		}
		return nil
	}
	if user.Usedspace > 0 {
		return fmt.Errorf("usedspace is not null")
	}
	err := UndepStore(username)
	if err != nil {
		logrus.Errorf("[PledgeSpace][%d]UndepStore ERR:%s\n", user.UserID, err)
		return err
	}
	err = dao.UpdateUserPledgeInfo(user.UserID, 0, 0)
	if err != nil {
		logrus.Errorf("[PledgeSpace][%d]UpdateUserPledgeInfo ERR:%s\n", user.UserID, err)
		return err
	}
	return nil
}

func QueryDeposit(username string) (*UserDeposit, error) {
	user := dao.GetUserByUsername(username)
	if user == nil {
		logrus.Infof("[PledgeSpace][username=%s]QueryDeposit user is null.\n", username)
		return nil, nil
	}
	userDeposit := &UserDeposit{Username: username, Usedspace: 0, PledgeFreeSpace: 0}
	if user.PledgeFreeSpace > 0 {
		r, _ := decimal.NewFromFloat(float64(user.PledgeFreeSpace) / 1024 / 1024 / 1024).Round(2).Float64()
		userDeposit.PledgeFreeSpace = r
	}
	if user.Usedspace > 0 {
		r, _ := decimal.NewFromFloat(float64(user.Usedspace) / 1024 / 1024 / 1024).Round(2).Float64()
		userDeposit.Usedspace = r
	}
	return userDeposit, nil
}
