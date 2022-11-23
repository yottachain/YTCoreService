package env

import (
	"bufio"
	"fmt"
	"log"
	"net/http"
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
var P2P_DualConnection = false

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

func InitClient() {
	pathstr := os.Getenv("YTFS_HOME")
	if pathstr == "" {
		pathstr = GetCurrentPath()
	}
	pathstr = strings.ReplaceAll(pathstr, "\\", "/")
	pathstr = path.Clean(pathstr)
	if !strings.HasSuffix(pathstr, "/") {
		pathstr = pathstr + "/"
	}
	YTFS_HOME = pathstr
	readClientProperties()
	InitLog(YTFS_HOME, "cli", logrus.StandardLogger())
	ULimit()
	port, err := GetFreePort()
	if err != nil {
		return
	}
	addr := fmt.Sprintf("0.0.0.0:%d", port)
	logrus.Infof("[Init]Starting pprof server on address %s\n", addr)
	go http.ListenAndServe(addr, nil)
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
	ReadExport(YTSN_HOME + "bin/ytsn.ev")
}

func p2pConfig(config *Config) {
	P2P_ConnectTimeout = config.GetRangeInt("P2PHOST_CONNECTTIMEOUT", 1000, 60000, 5000)
	P2P_RequestQueueSize = config.GetRangeInt("P2PHOST_REQ_QUEUESIZE", 1, 10, 2)
	P2P_ResponseQueueSize = config.GetRangeInt("P2PHOST_RESP_QUEUESIZE", 10, 100, 10)
	P2P_WriteTimeout = config.GetRangeInt("P2PHOST_WRITETIMEOUT", 1000, 60000, 7000)
	P2P_ReadTimeout = config.GetRangeInt("P2PHOST_READTIMEOUT", 1000, 180000, 20000)
	P2P_IdleTimeout = config.GetRangeInt("P2PHOST_IDLETIMEOUT", 60000, 3600000, 180000)
	P2P_MuteTimeout = config.GetRangeInt("P2PHOST_MUTETIMEOUT", P2P_WriteTimeout, P2P_IdleTimeout, P2P_WriteTimeout*3)
	P2P_DualConnection = config.GetBool("P2PHOST_DUALCONNECTION", false)
}

func ReadExport(path string) {
	f, err := os.Open(path)
	if err != nil {
		logrus.Errorf("[Init]Read export %s ERR: %s\n", path, err)
		return
	}
	defer f.Close()
	r := bufio.NewReader(f)
	for {
		b, _, err := r.ReadLine()
		if err != nil {
			break
		}
		s := strings.TrimSpace(string(b))
		if strings.HasPrefix(strings.ToUpper(s), "EXPORT") {
			s = strings.TrimSpace(s[7:])
			index := strings.Index(s, "=")
			if index < 0 {
				continue
			}
			key := strings.TrimSpace(s[:index])
			if len(key) == 0 {
				continue
			}
			value := strings.TrimSpace(s[index+1:])
			if !strings.Contains(value, "$") {
				os.Setenv(key, value)
				logrus.Infof("[Init]Set ENV %s=%s\n", key, value)
			}
		}
	}
}
