package handle

import (
	"github.com/yottachain/YTCoreService/dao"
	"go.mongodb.org/mongo-driver/bson"
)

func IterateUser() {
	lastId := 0
	limit := 1000
	for {
		_, err := dao.ListUsers(lastId, limit, bson.M{"_id": 1, "nextCycle": 1, "username": 1})
		if err != nil {

		}
	}

}
