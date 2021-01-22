package test

import (
	"os"

	"github.com/yottachain/YTCoreService/api"
)

const yfnet = true

var client *api.Client

func initApi() {
	if yfnet {
		os.Setenv("YTFS.snlist", "conf/snlistYF.properties")
		os.Setenv("YTFS.userlist", "conf/userlistYF.cfg")
	} else {
		os.Setenv("YTFS.snlist", "conf/snlistZW.properties")
		os.Setenv("YTFS.userlist", "conf/userlistZW.cfg")
	}
	api.InitApi()
}
