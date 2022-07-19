package api

import (
	"bytes"
	"encoding/json"
	"io/ioutil"
	"os"
	"strings"

	"github.com/sirupsen/logrus"
	"github.com/yottachain/YTCoreService/codec"
	"github.com/yottachain/YTCoreService/env"
)

type UserInfo struct {
	UserName      string
	Privkey       []string
	SignKeyNumber int32
	EncKeyNumber  int32
}

func encryptedFunc(data []byte) []byte {
	return codec.ECBEncrypt(data, codec.FixKey)
}

func decryteFunc(data []byte) []byte {
	return codec.ECBDecrypt(data, codec.FixKey)
}

func SaveEncryptUserProperties(userinfo []*UserInfo) {
	path := os.Getenv("YTFS.userlist")
	if path == "" {
		path = env.YTFS_HOME + "conf/userlist.cfg"
	} else {
		path = env.YTFS_HOME + path
	}
	bs, err := json.Marshal(userinfo)
	if err != nil {
		logrus.Errorf("[Init]Marshal userlist.cfg ERR:%s\n", err)
		return
	}
	var out bytes.Buffer
	json.Indent(&out, bs, "", "	")
	context := out.String()
	if isEncrypted {
		context = "encrypt:" + string(encryptedFunc(bs))
	}
	er := ioutil.WriteFile(path, []byte(context), 0664)
	if er != nil {
		logrus.Errorf("[Init]Save userlist.cfg ERR:%s\n", err)
	}
}

var isEncrypted = true

func ReadUserProperties() []*UserInfo {
	path := os.Getenv("YTFS.userlist")
	if path == "" {
		path = env.YTFS_HOME + "conf/userlist.cfg"
	} else {
		path = env.YTFS_HOME + path
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
	if strings.HasPrefix(text, "encrypt:") {
		text = text[8:]
		text = string(decryteFunc([]byte(text)))
		isEncrypted = true
	} else {
		text = strings.TrimPrefix(text, "nocrypted:")
		isEncrypted = false
	}
	infos := []*UserInfo{}
	bs = []byte(text)
	err = json.Unmarshal(bs, &infos)
	if err != nil {
		logrus.Errorf("[Init]Unmarshal userlist.cfg ERR:%s\n", err)
	}
	return infos
}
