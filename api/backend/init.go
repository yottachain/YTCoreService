package backend

import (
	"github.com/yottachain/YTCoreService/api"
	"github.com/yottachain/YTCoreService/env"
)

var defauleFs *YTFS

var (
	Object_Timeout int = 60
	SyncFileMin    int
	MaxGetObjNum   int
	MaxListNum     int
)

var ListBucketNum *int32 = new(int32)
var GetObjectNum *int32 = new(int32)
var Object_UP_CH chan int

type YTFS struct{}

func NewYTFS() *YTFS {
	if defauleFs != nil {
		return defauleFs
	}
	fs := &YTFS{}
	api.StartApi()
	InitObjectUpPool()
	return fs
}

func InitObjectUpPool() {
	MaxCreateObjNum := env.GetConfig().GetRangeInt("MaxCreateObjNum", 20, 500, 50)
	Object_Timeout = env.GetConfig().GetRangeInt("ObjectTimeout", 10, 300, 60)
	SyncFileMin = env.GetConfig().GetRangeInt("SyncFileMin", 1, 10, 2) * 1024 * 1024
	Object_UP_CH = make(chan int, MaxCreateObjNum)
	for ii := 0; ii < MaxCreateObjNum; ii++ {
		Object_UP_CH <- 1
	}
	MaxGetObjNum = env.GetConfig().GetRangeInt("MaxGetObjNum", 20, 100, 50)
	MaxListNum = env.GetConfig().GetRangeInt("MaxListNum", 1, 10, 2)
}
