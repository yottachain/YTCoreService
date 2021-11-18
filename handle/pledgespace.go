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

func CheckFreeSpace(userID int32, addUsedspace int64) (bool, error) {
	pledgeInfo, err := getUserPledgeInfo(userID)
	if err != nil {
		logrus.Errorf("[PledgeSpace][%d]GetUserPledgeInfo ERR:%s\n", userID, err)
		return false, err
	}
	if pledgeInfo.PledgeUsedSpace >= pledgeInfo.PledgeFreeSpace {
		return false, nil
	}
	usedspaceNew := pledgeInfo.PledgeUsedSpace
	if addUsedspace > 0 {
		usedspaceNew += addUsedspace
		err = dao.UpdateNodePledgeSpace(userID, usedspaceNew)
		if err != nil {
			return false, err
		}
	}
	if usedspaceNew <= pledgeInfo.PledgeFreeSpace {
		return true, nil
	}
	return false, nil
}

func getUserPledgeInfo(userID int32) (*dao.PledgeInfo, error) {
	info, err := dao.GetNodePledgeInfo(userID)
	if err != nil {
		return nil, err
	}
	var pledgeFreeAmount int64

	if info == nil || info.PledgeFreeAmount == 0 || time.Now().Sub(time.Unix(info.PledgeUpdateTime, 0)).Seconds() > float64(env.PLEDGE_SPACE_UPDATE_INTERVAL) {
		user := dao.GetUserByUserId(userID)
		if user == nil {
			logrus.Errorf("[PledgeSpace][%d]GetUserByUserId The query result is empty\n", userID)
			return nil, fmt.Errorf("User is null")
		}

		bpUrl := net.GetEOSURI().Url

		depData, err := eospledge.GetDepStore(bpUrl, user.Username)
		if err != nil {
			logrus.Errorf("[PledgeSpace][%d]GetDepStore ERR:%s\n", userID, err)
			return nil, err
		} else {
			pledgeFreeAmount = int64(depData.DepositTotal.Amount)
			info.UserID = userID
			info.PledgeFreeAmount = pledgeFreeAmount
			info.PledgeFreeSpace = calcPledgeFreeSpace(pledgeFreeAmount)
			info.PledgeUsedSpace = user.Usedspace
			info.PledgeUpdateTime = time.Now().Unix()

			err = dao.UpdateNodePledgeInfo(userID, pledgeFreeAmount, info.PledgeFreeSpace, info.PledgeUsedSpace)
			if err != nil {
				return nil, err
			}
		}

	}
	return info, nil
}

func calcPledgeFreeSpace(amount int64) int64 {
	for _, levelInfo := range env.PLEDGE_SPACE_FEE {
		if amount >= int64(levelInfo.Level) {
			return int64(levelInfo.Fee * int(amount))
		}
	}
	return 0
}
