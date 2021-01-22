package test

import (
	"os"

	"github.com/yottachain/YTCoreService/api"
)

func Sync() {
	UploadFile()
	//SyncFile()
}

//1.注册一个用户(从配置文件conf/userlist.cfg自动注册),上传几个文件
//设置conf/ytfs.properties: driver=nas,startSync=0
func UploadFile() {
	initApi()
	client.UploadFile("D:/Adobe_Reader_XI_zh_CN.exe", "test", "Adobe_Reader_XI_zh_CN.exe")
	client.UploadFile("D:/Secop.rar", "test", "Secop.rar")
	client.UploadFile("D:/YTCoreService_2.0.0.1.gz", "test", "YTCoreService_2.0.0.1.gz")
	client.UploadFile("D:/YTCoreService_2.0.0.2.gz", "test", "YTCoreService_2.0.0.2.gz")
}

//2.不注册用户,设置conf/ytfs.properties: driver=yotta,startSync=1
//启动同步
func SyncFile() {
	api.AUTO_REG_FLAG = false
	if yfnet {
		os.Setenv("YTFS.snlist", "conf/snlistYF.properties")
	} else {
		os.Setenv("YTFS.snlist", "conf/snlistZW.properties")
	}
	api.InitApi()
}
