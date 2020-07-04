package handle

import (
	"time"

	"github.com/yottachain/YTCoreService/dao"
	"github.com/yottachain/YTCoreService/net"
	"go.mongodb.org/mongo-driver/bson"
)

func IterateUser() {
	lastId := 0
	limit := 1000
	for {
		if !net.IsActive() {
			time.Sleep(time.Duration(30) * time.Second)
			continue
		}
		_, err := dao.ListUsers(lastId, limit, bson.M{"_id": 1, "nextCycle": 1, "username": 1})
		if err != nil {

		}
	}

}
