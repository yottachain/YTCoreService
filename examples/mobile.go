package examples

import (
	"os"

	"github.com/sirupsen/logrus"
	"github.com/yottachain/YTCoreService/api"
	"github.com/yottachain/YTCoreService/api/sgx"
)

func Encode() {
	//手机端编码
	os.Setenv("YTFS.snlist", "conf/snlistYF.properties")
	api.StartMobileAPI()
	c, err := api.NewClient(&api.UserInfo{
		UserName: "testusernew1",
		Privkey:  []string{"5KETn1mgk4wpv78GLiGA2mejyqCzE53S2W7shWzqFBuLRrafJ4f"}}, 3)
	if err != nil {
		logrus.Panicf(":%s\n", err)
	}
	do := c.UploadPreEncode("test", "sss.txt")

	err1 := do.UploadFile("d:\\1-21480-.xls")
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

func Decode() {

	//1.创建私钥
	key, err := sgx.NewKey("111111111111111111111", 2)
	if err != nil {
		return
	}

	//2.要写入的文件
	f, err := os.OpenFile("d:/test", os.O_WRONLY|os.O_TRUNC|os.O_CREATE, 0644)
	if err != nil {
		return
	}
	defer f.Close()

	//3.解密,data是下载到的未解密数据块
	data := []byte{}
	block := sgx.NewEncryptedBlock(data)
	err = block.Decode(key, f)
	if err != nil {
		return
	}
}
