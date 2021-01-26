package test

import (
	"bytes"
	"os"

	"github.com/sirupsen/logrus"
	"github.com/yottachain/YTCoreService/api"
	"github.com/yottachain/YTCoreService/env"
)

func Auth() {
	api.AUTO_REG_FLAG = false
	os.Setenv("YTFS.snlist", "conf/snlistYF.properties")

	//0.初始化SDK,加载"conf/snlist.properties","conf/ytfs.properties"
	api.InitApi()

	//1.注册导出授权的用户实例
	exportclient, err := api.NewClientV2(&env.UserInfo{
		UserName: "devtestuser1",
		Privkey:  []string{"5KTF2yAamvcaoDu6juAvxT5nxTn3UGfNoY2CJn8VAQ4giAfma2a"}}, 3)
	if err != nil {
		logrus.Panicf("注册导出授权用户失败:%s\n", err)
	}

	//2.注册导入授权的用户实例
	importclient, err := api.NewClientV2(&env.UserInfo{
		UserName: "devvtest1111",
		Privkey:  []string{"5JReF8eeGS53B8prdcrSfTf6dGbvu3QJ6KceE8rLsnRaNMMCYw9"}}, 3)
	if err != nil {
		logrus.Panicf("注册导入授权用户失败:%s\n", err)
	}

	//3.通过上传接口,给导出授权的用户上传一个文件
	md5, yerr := exportclient.UploadFile("D:/本地文件.dat", "test", "testauthexport.dat")
	if yerr != nil {
		logrus.Panicf("上传文件失败:%s\n", yerr.Msg)
	}

	//4.把用户"exportclient"所属文件(第三步上传的文件test/testauth.dat),授权给用户"importclient",
	//根据用户"importclient"的存储公钥导出授权书
	exporter, yerr := exportclient.ExporterAuth("test", "testauthexport.dat")
	if yerr != nil {
		logrus.Panicf("初始化授权导出失败:%s\n", yerr.Msg)
	}
	authdata, yerr := exporter.Export(importclient.StoreKey.PublicKey)
	if yerr != nil {
		logrus.Panicf("导出授权文件失败:%s\n", yerr.Msg)
	}

	//5.用户"importclient"使用上面的授权书(authdata),将文件导入
	importer := importclient.ImporterAuth("test", "testauthimport.dat")
	yerr = importer.Import(authdata)
	if yerr != nil {
		logrus.Panicf("导入授权文件失败:%s\n", yerr.Msg)
	}

	//6.用户"importclient"通过下载接口,下载刚才导入的文件
	down, yerr := importclient.NewDownloadLastVersion("test", "testauthimport.dat")
	if yerr != nil {
		logrus.Panicf("初始化下载失败:%s\n", yerr.Msg)
	}
	newmd5, errmsg := down.SaveToFile("d:/下载到的文件.dat")
	if errmsg != nil {
		logrus.Panicf("下载失败:%s\n", errmsg)
	}

	//7.检测导入文件和源文件一致性
	if bytes.Equal(newmd5, md5) {
		logrus.Info("授权测试通过.\n")
	} else {
		logrus.Info("授权测试不通过.\n")
	}

}
