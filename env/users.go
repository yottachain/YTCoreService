package env

import (
	"encoding/json"
	"io/ioutil"
	"os"
	"strings"

	"github.com/sirupsen/logrus"
)

type UserInfo struct {
	UserName      string
	Privkey       []string
	SignKeyNumber int32
	EncKeyNumber  int32
}

type EncryteFunc func(data []byte) []byte
type DecryteFunc func(data []byte) []byte

func SaveEncryptUserProperties(userinfo []*UserInfo, encryptedFunc EncryteFunc) {
	path := os.Getenv("YTFS.userlist")
	if path == "" {
		path = YTFS_HOME + "conf/userlist.cfg"
	} else {
		path = YTFS_HOME + path
	}
	bs, err := json.Marshal(userinfo)
	if err != nil {
		logrus.Error("[Init]Unmarshal userlist.cfg ERR:%s\n", err)
		return
	}
	context := "encrypt:" + string(encryptedFunc(bs))
	er := ioutil.WriteFile(path, []byte(context), 0664)
	if er != nil {
		logrus.Error("[Init]Save userlist.cfg ERR:%s\n", err)
	}
}

func ReadUserProperties(decryteFunc DecryteFunc) []*UserInfo {
	path := os.Getenv("YTFS.userlist")
	if path == "" {
		path = YTFS_HOME + "conf/userlist.cfg"
	} else {
		path = YTFS_HOME + path
	}
	_, err := os.Stat(path)
	if err != nil {
		if !os.IsExist(err) {
			return []*UserInfo{}
		}
	}
	bs, err := ioutil.ReadFile(path)
	if err != nil {
		logrus.Warnf("[Init]Read userlist.cfg ERR:%s\n", err)
		return []*UserInfo{}
	}

	text := string(bs)
	if strings.HasPrefix(text, "crypted:") {
		text = text[8:]
		decryteFunc([]byte(text))
	}
	if strings.HasPrefix(text, "nocrypted:") {
		text = text[10:]
	}
	infos := []*UserInfo{}
	bs = decryteFunc([]byte(text))
	err = json.Unmarshal(bs, &infos)
	if err != nil {
		logrus.Errorf("[Init]Unmarshal userlist.cfg ERR:%s\n", err)
	}
	return infos
}
