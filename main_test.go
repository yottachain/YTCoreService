package main

import (
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/yottachain/YTCoreService/env"
)

func init() {
	env.Console = true
}

func Test(t *testing.T) {
	logrus.Infof("%s\n", time.Unix(6882675574427773490>>32, 0).Format("2006-01-02 15:04:05"))
	logrus.Infof("%s\n", time.Unix(6882683700315583710>>32, 0).Format("2006-01-02 15:04:05"))
	logrus.Infof("%s\n", time.Unix(6882683988225249844>>32, 0).Format("2006-01-02 15:04:05"))
	logrus.Infof("%s\n", time.Unix(6882946187213889929>>32, 0).Format("2006-01-02 15:04:05"))

	defer env.TracePanic()

	//***********api test*********
	//test.ListObj()
	//test.UpAndDownFile()
	//test.DownLoadByKey()
	//test.UpAndDownBytes()
	//test.ListBucket()

	//**********code test**********
	//env.InitClient()
	//test.TestCutFile()
	//test.TestLRC()
	select {}
}
