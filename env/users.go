package env

import (
	"encoding/json"
	"io/ioutil"

	"github.com/sirupsen/logrus"
)

type UserInfo struct {
	UserName      string
	Privkey       []string
	SignKeyNumber int32
	EncKeyNumber  int32
}

func ReadUserProperties() {
	path := YTFS_HOME + "userlist.cfg"
	bs, err := ioutil.ReadFile(path)
	if err != nil {
		logrus.Warnf("[Init]Read userlist.cfg ERR:%s\n", err)
	}
	infos := []*UserInfo{}
	err = json.Unmarshal(bs, &infos)
	if err != nil {
		logrus.Error("[Init]Unmarshal userlist.cfg ERR:%s\n", err)
	}
}
