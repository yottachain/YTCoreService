package test

import (
	"os"

	"github.com/yottachain/YTCoreService/api"
	"github.com/yottachain/YTCoreService/pkt"
)

var istest bool = false

func Upload() {
	var user string
	var pkey string
	if istest {
		os.Setenv("YTFS.snlist", "conf/snlistYF.properties")
		user = "username1234"
		pkey = "5KfbRow4L71fZnnu9XEnkmVqByi6CSmRiADJCx6asRS4TUEkU79"
	} else {
		os.Setenv("YTFS.snlist", "conf/snlistZW.properties")
		user = "ianmooneyy11"
		pkey = "5JnLRW1bTRD2bxo93wZ1qnpXfMDHzA97qcQjabnoqgmJTt7kBoH"
	}

	api.StartApi()
	c, err := api.NewClient(user, pkey)

	if err != nil {
		panic(err)
	}

	up := c.UploadObject()
	_, errmsg := up.UploadFile("d:/test.rar")
	if errmsg != nil {
		panic(pkt.ToError(errmsg))
	}

	select {}
}
