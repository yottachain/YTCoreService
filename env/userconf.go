package env

import (
	"log"
	"os"
	"path"
	"strings"
)

const s3cache_dir = "s3cache"
const dbcache_dir = "dbcache"

var PNN int = 328 * 2
var PTR int = 2

var ALLOC_MODE int = 0

var UploadFileMaxMemory int = 10 * 1024 * 1024
var UploadBlockThreadNum int = 50
var UploadShardThreadNum int = 1500
var UploadShardRetryTimes int = 3
var DownloadRetryTimes int = 3

var DownloadThread int = 200

var CachePath string
var MaxCacheSize int64
var SyncMode int = 0
var Driver string
var StartSync = 0
var OptionMiners = 1000
var WeightDivsor = 500
var Openstat = false

var cfg *Config

func GetConfig() *Config {
	return cfg
}

func GetS3Cache() string {
	return CachePath + s3cache_dir + "/"
}

func GetDBCache() string {
	return CachePath + dbcache_dir + "/"
}

func GetCache() string {
	return CachePath
}

func MkCacheDir(name string) {
	path := CachePath + name + "/"
	err := os.MkdirAll(path, os.ModePerm)
	if err != nil {
		log.Panicf("[Init]Cache path ERR:%s\n", err)
	}
}

func readClientProperties() {
	confpath := YTFS_HOME + "conf/ytfs.properties"
	config, err := NewConfig(confpath)
	if err != nil {
		log.Panicf("[Init]No properties file could be found for ytfs service:%s\n", confpath)
	}
	cfg = config
	CachePath = config.GetString("cache", YTFS_HOME+"cache")
	CachePath = strings.ReplaceAll(CachePath, "\\", "/")
	CachePath = strings.ReplaceAll(CachePath, "\"", "")
	CachePath = path.Clean(CachePath)
	if !strings.HasSuffix(CachePath, "/") {
		CachePath = CachePath + "/"
	}
	MkCacheDir(s3cache_dir)
	MkCacheDir(dbcache_dir)
	SyncMode = config.GetRangeInt("syncmode", 0, 1, 0)
	StartSync = config.GetRangeInt("startSync", 0, 1, 0)
	Driver = strings.ToLower(config.GetString("driver", "yotta"))
	size := config.GetRangeInt("cachemaxsize", 5, 1024*100, 20)
	MaxCacheSize = int64(size) * 1024 * 1024 * 1024
	LogLevel = config.GetString("logLevel", "trace,stdout")
	PNN = config.GetRangeInt("PNN", 328, 328*4, 328*2)
	PTR = config.GetRangeInt("PTR", 1, 60, 2)

	UploadFileMaxMemory = config.GetRangeInt("uploadFileMaxMemory", 5, 2048, 30)
	UploadFileMaxMemory = UploadFileMaxMemory * 1024 * 1024
	UploadBlockThreadNum = config.GetRangeInt("uploadBlockThreadNum", 10, 1024, 30)
	UploadShardThreadNum = config.GetRangeInt("uploadShardThreadNum", 1500, 20000, 1500)
	UploadShardRetryTimes = config.GetRangeInt("uploadShardRetryTimes", 1, 10, 3)
	DownloadRetryTimes = config.GetRangeInt("downloadRetryTimes", 3, 10, 3)
	DownloadThread = config.GetRangeInt("downloadThread", 328, 328*4, 328*2)

	ALLOC_MODE = config.GetRangeInt("ALLOC_MODE", -1, 2000, 0)

	ShardNumPerNode = config.GetRangeInt("shardNumPerNode", 1, 200, 1)

	Conntimeout = config.GetRangeInt("P2PHOST_CONNECTTIMEOUT", 1000, 60000, 15000)
	DirectConntimeout = CheckInt(Conntimeout/10, 500, 5000)
	Writetimeout = config.GetRangeInt("P2PHOST_WRITETIMEOUT", 1000, 60000, 15000)
	DirectWritetimeout = CheckInt(Writetimeout/10, 500, 5000)

	OptionMiners = config.GetRangeInt("optionMiners", 200, 3000, 1000)
	WeightDivsor = config.GetRangeInt("WeightDivsor", 1, 500, 500)
	Openstat = config.GetBool("OpenStat", false)
}
