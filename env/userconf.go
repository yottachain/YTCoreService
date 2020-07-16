package env

import (
	"os"
	"strconv"
	"strings"
)

var ClientLogLevel string

var PNN int = 328 * 2
var PTR int = 2
var RETRYTIMES int = 500

var uploadFileMaxMemory int = 10
var uploadBlockThreadNum int = 50
var uploadShardThreadNum int = 1500

var downloadThread int = 200

func readClientProperties() {
	confpath := YTFS_HOME + "conf/ytfs.properties"
	config := ReadConfig(confpath)

	ClientLogLevel = strings.Trim(config["logLevel"], " ")
	PNN = StringToInt(config["PNN"], 328, 328*4, 328*2)
	PTR = StringToInt(config["PTR"], 1, 60, 2)
	RETRYTIMES = StringToInt(config["RETRYTIMES"], 50, 500, 50)

	uploadFileMaxMemory = StringToInt(config["uploadFileMaxMemory"], 5, 300, 30)
	uploadBlockThreadNum = StringToInt(config["uploadBlockThreadNum"], 10, 300, 30)
	uploadShardThreadNum = StringToInt(config["uploadShardThreadNum"], 1500, 3000, 1500)

	downloadThread = StringToInt(config["downloadThread"], 328, 328*4, 328*2)

	P2PHOST_CONNECTTIMEOUT := StringToInt(config["P2PHOST_CONNECTTIMEOUT"], 1000, 60000, 15000)
	os.Setenv("P2PHOST_CONNECTTIMEOUT", strconv.Itoa(P2PHOST_CONNECTTIMEOUT))
	P2PHOST_WRITETIMEOUT := StringToInt(config["P2PHOST_WRITETIMEOUT"], 1000, 60000, 15000)
	os.Setenv("P2PHOST_WRITETIMEOUT", strconv.Itoa(P2PHOST_WRITETIMEOUT))
}
