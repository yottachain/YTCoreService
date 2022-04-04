package test

import (
	"os"
	"time"

	"github.com/yottachain/YTCoreService/api"
)

const yfnet = false

var client *api.Client

func initApi() {
	if yfnet {
		os.Setenv("YTFS.snlist", "conf/snlistYF.properties")
		os.Setenv("YTFS.userlist", "conf/userlistYF.cfg")
	} else {
		os.Setenv("YTFS.snlist", "conf/snlist.properties")
		os.Setenv("YTFS.userlist", "conf/userlistZW.cfg")
	}
	api.InitApi()

	for {
		cs := api.GetClients()
		if len(cs) > 0 {
			client = cs[0]
			break
		} else {
			time.Sleep(time.Duration(1) * time.Second)
		}
	}

}
