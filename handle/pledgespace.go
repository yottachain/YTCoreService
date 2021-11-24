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
			pledgeFreeAmount := int64(depData.DepositTotal.Amount)
			user.PledgeFreeAmount = float64(pledgeFreeAmount / 10000)
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
