package test

import (
	"os"

	"github.com/sirupsen/logrus"
	"github.com/yottachain/YTCoreService/api"
	"github.com/yottachain/YTCoreService/env"
)

func Encode() {
	//手机端编码
	os.Setenv("YTFS.snlist", "conf/snlistYF.properties")
	api.StartMobileAPI()
	c, err := api.NewClientV2(&env.UserInfo{
		UserName: "testusernew1",
		Privkey:  []string{"5KETn1mgk4wpv78GLiGA2mejyqCzE53S2W7shWzqFBuLRrafJ4f"}}, 3)
	if err != nil {
		logrus.Panicf(":%s\n", err)
	}
	do := c.UploadPreEncode("test", "sss.txt")

	err1 := do.UploadFile("d:/1-21480-12210608112421­_12vv(1).xls")
	if err1 != nil {
		logrus.Panicf(":%s\n", err1)
	}
	ss := do.OutPath()

	//外部s3负责上传
	up, err1 := api.NewUploadEncObject(ss)
	if err1 != nil {
		logrus.Panicf(":%s\n", err1)
	}
	err1 = up.Upload()
	if err1 != nil {
		logrus.Panicf(":%s\n", err1)
	}
}
