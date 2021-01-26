package env

import (
	"encoding/json"
	"io/ioutil"
	"os"

	"github.com/sirupsen/logrus"
)

type UserInfo struct {
	UserName      string
	Privkey       []string
	SignKeyNumber int32
	EncKeyNumber  int32
}

func ReadUserProperties() []*UserInfo {
	path := os.Getenv("YTFS.userlist")
	if path == "" {
		path = YTFS_HOME + "conf/userlist.cfg"
	} else {
		path = YTFS_HOME + path
	}
	bs, err := ioutil.ReadFile(path)
	if err != nil {
		logrus.Warnf("[Init]Read userlist.cfg ERR:%s\n", err)
	}
	infos := []*UserInfo{}
	err = json.Unmarshal(bs, &infos)
	if err != nil {
		logrus.Errorf("[Init]Unmarshal userlist.cfg ERR:%s\n", err)
	}
	return infos
}
