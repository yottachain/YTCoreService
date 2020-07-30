package test

import (
	"os"

	"github.com/yottachain/YTCoreService/api"
	"github.com/yottachain/YTCoreService/pkt"
)

func Upload() {
	os.Setenv("YTFS.snlist", "conf/snlistYF.properties")
	api.StartApi()
	c, err := api.NewClient("username1234", "5KfbRow4L71fZnnu9XEnkmVqByi6CSmRiADJCx6asRS4TUEkU79")

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
