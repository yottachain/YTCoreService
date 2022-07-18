package handle

import (
	"time"

	"github.com/yottachain/YTCoreService/dao"
	"github.com/yottachain/YTCoreService/env"
)

func startRelationshipSum() {
	for {
		time.Sleep(time.Duration(15) * time.Minute)
		sumUsedSpace()
	}
}

func sumUsedSpace() {
	defer env.TracePanic("[RelationshipSum]")
	m, err := dao.SumRelationship()
	if err == nil {
		if len(m) > 0 {
			for k, v := range m {
				dao.SetSpaceSum(0, k, uint64(v))
			}
		}
		time.Sleep(time.Duration(15) * time.Minute)
	} else {
		time.Sleep(time.Duration(1) * time.Minute)
	}
}
