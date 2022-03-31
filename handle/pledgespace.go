package handle

import (
	"fmt"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/yottachain/YTCoreService/dao"
	"github.com/yottachain/YTCoreService/env"
	"github.com/yottachain/YTCoreService/eospledge"
	"github.com/yottachain/YTCoreService/net"
)

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
		return nil, fmt.Errorf("User is null")
	}

	if user.PledgeFreeAmount == 0 || time.Now().Sub(time.Unix(user.PledgeUpdateTime, 0)).Seconds() > float64(env.PLEDGE_SPACE_UPDATE_INTERVAL) {
		bpUrl := net.GetEOSURI().Url

		depData, err := eospledge.GetDepStore(bpUrl, user.Username)
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

func UndepStore(username string) error {
	user := dao.GetUserByUsername(username)
	if user == nil { //用户信息为空，未存储过数据，可直接退抵押
		// return fmt.Errorf("User is null")
		logrus.Infof("[PledgeSpace][username=%s]User is null.\n", username)
		err := net.UndepStore(username)
		if err != nil {
			logrus.Errorf("[PledgeSpace][%d]UndepStore ERR:%s\n", user.UserID, err)
			return err
		}
		return nil
	}
	// logrus.Infof("[PledgeSpace][%d]UndepStore TEST:%v\n", user.UserID, *user)

	if user.Usedspace > 0 {
		return fmt.Errorf("Usedspace is not null")
	}
	err := net.UndepStore(username)
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
