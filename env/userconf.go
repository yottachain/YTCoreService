package env

import (
	"log"
	"os"
	"strconv"
)

var PNN int = 328 * 2
var PTR int = 2
var RETRYTIMES int = 500

var uploadFileMaxMemory int = 10
var uploadBlockThreadNum int = 50
var uploadShardThreadNum int = 1500

var downloadThread int = 200

func readClientProperties() {
	confpath := YTFS_HOME + "conf/ytfs.properties"
	config, err := NewConfig(confpath)
	if err != nil {
		log.Panicf("[Init]No properties file could be found for ytfs service:%s\n", confpath)
	}
	LogLevel = config.GetString("logLevel", "trace,stdout")
	PNN = config.GetRangeInt("PNN", 328, 328*4, 328*2)
	PTR = config.GetRangeInt("PTR", 1, 60, 2)
	RETRYTIMES = config.GetRangeInt("RETRYTIMES", 50, 500, 50)

	uploadFileMaxMemory = config.GetRangeInt("uploadFileMaxMemory", 5, 300, 30)
	uploadBlockThreadNum = config.GetRangeInt("uploadBlockThreadNum", 10, 300, 30)
	uploadShardThreadNum = config.GetRangeInt("uploadShardThreadNum", 1500, 3000, 1500)

	downloadThread = config.GetRangeInt("downloadThread", 328, 328*4, 328*2)

	P2PHOST_CONNECTTIMEOUT := config.GetRangeInt("P2PHOST_CONNECTTIMEOUT", 1000, 60000, 15000)
	os.Setenv("P2PHOST_CONNECTTIMEOUT", strconv.Itoa(P2PHOST_CONNECTTIMEOUT))
	P2PHOST_WRITETIMEOUT := config.GetRangeInt("P2PHOST_WRITETIMEOUT", 1000, 60000, 15000)
	os.Setenv("P2PHOST_WRITETIMEOUT", strconv.Itoa(P2PHOST_WRITETIMEOUT))
}
