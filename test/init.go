package test

import (
	"os"

	"github.com/sirupsen/logrus"
	"github.com/yottachain/YTCoreService/api"
)

const yfnet = false

var client *api.Client

func initApi() {
	var user string
	var pkey string
	if yfnet {
		os.Setenv("YTFS.snlist", "conf/snlistYF.properties")
		//user = "username1234"
		//pkey = "5KfbRow4L71fZnnu9XEnkmVqByi6CSmRiADJCx6asRS4TUEkU79"
		user = "devtestuser1"
		pkey = "5KTF2yAamvcaoDu6juAvxT5nxTn3UGfNoY2CJn8VAQ4giAfma2a"
		//user = "devvtest1111"
		//pkey = "5JReF8eeGS53B8prdcrSfTf6dGbvu3QJ6KceE8rLsnRaNMMCYw9"
	} else {
		os.Setenv("YTFS.snlist", "conf/snlistZW.properties")
		user = "ianmooneyy11"
		pkey = "5JnLRW1bTRD2bxo93wZ1qnpXfMDHzA97qcQjabnoqgmJTt7kBoH"
		//user = "nloadzooqwer"
		//pkey = "5KRWqgvdYVomJhobea4AbXpi9nR2wj53Hzy2JgUpAgZAry8WyeG"
	}
	api.StartApi()
	c, err := api.NewClient(user, pkey)
	if err != nil {
		logrus.Panicf("[NewClient]ERR:%s\n", err)
	}
	client = c
}
