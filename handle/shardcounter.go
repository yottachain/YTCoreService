package handle

import (
	"time"

	"github.com/yottachain/YTCoreService/dao"
)

func StartIterate() {
	var firstId int64
	for {
		id, err := dao.GetShardCountProgress()
		if err != nil {
			time.Sleep(time.Duration(30) * time.Second)
		} else {
			firstId = id
			break
		}
	}
	if firstId > 0 {
		return
	}
}

func GetTimestamp(id int64) int64 {
	return id >> 32
}
