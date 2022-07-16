package env

import (
	"log"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"runtime"
	"strings"

	"github.com/sirupsen/logrus"
)

var YTSN_HOME string
var YTFS_HOME string

var P2P_RequestQueueSize int = 2
var P2P_ResponseQueueSize int = 30
var P2P_ConnectTimeout int = 5000
var P2P_WriteTimeout int = 15000
var P2P_ReadTimeout int = 15000
var P2P_MuteTimeout int = 60000
var P2P_IdleTimeout int = 60000 * 3

func GetCurrentPath() string {
	file, _ := exec.LookPath(os.Args[0])
	if file == "" {
		ApplicationPath, _ := filepath.Abs(file)
		return ApplicationPath
	}
	if runtime.GOOS == "windows" {
		ApplicationPath, _ := filepath.Abs(file)
		ApplicationPath, _ = filepath.Split(ApplicationPath)
		return ApplicationPath
	} else {
		fi, err := os.Lstat(file)
		if err != nil {
			log.Panicf("GetCurrentPath.Lstat ERR:%s\n ", err)
		}
		if fi.Mode()&os.ModeSymlink == os.ModeSymlink {
			execPath, err := os.Readlink(file)
			if err != nil {
				log.Panicf("GetCurrentPath.Readlink ERR:%s\n ", err)
			}
			ApplicationPath, _ := filepath.Split(execPath)
			return ApplicationPath
		} else {
			ApplicationPath, _ := filepath.Split(file)
			return ApplicationPath
		}
	}
}

func InitServer() {
	pathstr := os.Getenv("YTSN_HOME")
	if pathstr == "" {
		pathstr = GetCurrentPath()
	}
	pathstr = strings.ReplaceAll(pathstr, "\\", "/")
	pathstr = path.Clean(pathstr)
	if !strings.HasSuffix(pathstr, "/") {
		pathstr = pathstr + "/"
	}
	YTSN_HOME = pathstr
	os.Setenv("YTSN_HOME", YTSN_HOME)
	os.Setenv("NODEMGMT_CONFIGDIR", YTSN_HOME+"conf")
	readSnProperties()
	InitLog(YTSN_HOME, "log", logrus.StandardLogger())
	InitLog(YTSN_HOME, "std", STDLog)
	ULimit()
}

func p2pConfig(config *Config) {
	P2P_ConnectTimeout = config.GetRangeInt("P2PHOST_CONNECTTIMEOUT", 1000, 60000, 5000)
	P2P_RequestQueueSize = config.GetRangeInt("P2PHOST_REQ_QUEUESIZE", 1, 10, 2)
	P2P_ResponseQueueSize = config.GetRangeInt("P2PHOST_RESP_QUEUESIZE", 10, 100, 10)
	P2P_WriteTimeout = config.GetRangeInt("P2PHOST_WRITETIMEOUT", 1000, 60000, 7000)
	P2P_ReadTimeout = config.GetRangeInt("P2PHOST_READTIMEOUT", 1000, 180000, 20000)
	P2P_IdleTimeout = config.GetRangeInt("P2PHOST_IDLETIMEOUT", 60000, 3600000, 180000)
	P2P_MuteTimeout = config.GetRangeInt("P2PHOST_MUTETIMEOUT", P2P_WriteTimeout, P2P_IdleTimeout, P2P_WriteTimeout*3)
}
