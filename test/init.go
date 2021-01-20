package test

import (
	"os"
	"time"

	"github.com/yottachain/YTCoreService/api"
)

const yfnet = true

var client *api.Client
var authclient *api.Client

func initApi() {
	if yfnet {
		os.Setenv("YTFS.snlist", "conf/snlistYF.properties")
		os.Setenv("YTFS.userlist", "conf/userlistYF.cfg")
	} else {
		os.Setenv("YTFS.snlist", "conf/snlistZW.properties")
		os.Setenv("YTFS.userlist", "conf/userlistYF.cfg")
	}
	api.StartApi()
	clients := api.GetClients()
	for {
		if len(clients) > 0 {
			break
		} else {
			time.Sleep(time.Duration(1) * time.Second)
			clients = api.GetClients()
		}
	}
	client = clients[0]
	if len(clients) > 1 {
		authclient = clients[1]
	}
}
