package env

import (
	"log"
	"os"
	"path"
	"strings"
)

const s3cache_dir = "s3cache"
const dbcache_dir = "dbcache"

var MaxCacheSize int64
var CachePath string

var SyncMode int = 0
var Driver string
var StartSync = 0

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
	logConfig(config)
	p2pConfig(config)

	MaxCacheSize = int64(config.GetRangeInt("cachemaxsize", 5, 1024*100, 20)) * 1024 * 1024 * 1024
	CachePath = config.GetString("cache", YTFS_HOME+"cache")
	CachePath = strings.ReplaceAll(CachePath, "\\", "/")
	CachePath = strings.ReplaceAll(CachePath, "\"", "")
	CachePath = path.Clean(CachePath)
	if !strings.HasSuffix(CachePath, "/") {
		CachePath = CachePath + "/"
	}
	MkCacheDir(s3cache_dir)
	MkCacheDir(dbcache_dir)

	dnConfig(config)
	upConfig(config)
	downConfig(config)

	SyncMode = config.GetRangeInt("syncmode", 0, 1, 0)
	StartSync = config.GetRangeInt("startSync", 0, 1, 0)
	Driver = strings.ToLower(config.GetString("driver", "yotta"))
}

var (
	PNN        int = 328 * 2
	PTR        int = 2
	ALLOC_MODE int = 0
)

func dnConfig(config *Config) {
	PNN = config.GetRangeInt("PNN", 328, 328*10, 328*2)
	PTR = config.GetRangeInt("PTR", 1, 60, 2)
	ALLOC_MODE = config.GetRangeInt("ALLOC_MODE", -1, 2000, 0)
}

var (
	LRC2                      = false
	CopyNum                   = 10
	ExtraPercent              = 100
	BlkTimeout            int = 0
	MakeBlockThreadNum    int = 5
	UploadBlockThreadNum  int = 100
	UploadShardThreadNum  int = 1500
	UploadShardRetryTimes int = 3
	ThrowErr                  = false
)

func upConfig(config *Config) {
	LRC2 = config.GetBool("LRC2", true)
	CopyNum = config.GetRangeInt("CopyNum", 5, 18, 10)
	ExtraPercent = config.GetRangeInt("ExtraPercent", 0, 100, 100)
	BlkTimeout = config.GetRangeInt("BlkTimeout", 0, 100, 0)
	MakeBlockThreadNum = config.GetRangeInt("makeBlockThreadNum", 1, 20, 5)
	UploadBlockThreadNum = config.GetRangeInt("uploadBlockThreadNum", 10, 1024, 100)
	UploadShardThreadNum = config.GetRangeInt("uploadShardThreadNum", 1500, 100000, 1500)
	UploadShardRetryTimes = config.GetRangeInt("uploadShardRetryTimes", 1, 10, 3)
	ThrowErr = config.GetBool("throwErr", false)
}

var (
	DownloadRetryTimes int = 3
	DownloadThread     int = 200
)

func downConfig(config *Config) {
	DownloadRetryTimes = config.GetRangeInt("downloadRetryTimes", 3, 10, 3)
	DownloadThread = config.GetRangeInt("downloadThread", 328, 328*4, 328*2)
}
