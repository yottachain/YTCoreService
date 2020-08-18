package env

import (
	"fmt"
	"log"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	rotatelogs "github.com/lestrrat-go/file-rotatelogs"
	"github.com/rifflock/lfshook"
	"github.com/sirupsen/logrus"
)

var YTSN_HOME string
var YTFS_HOME string
var LogLevel string
var Console bool = false

func GetCurrentPath() string {
	file, _ := exec.LookPath(os.Args[0])
	if file == "" {
		ApplicationPath, _ := filepath.Abs(file)
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
	YTFS_HOME = os.Getenv("YTFS_HOME")
	if YTFS_HOME == "" {
		YTFS_HOME = GetCurrentPath()
	}
	if !strings.HasSuffix(YTFS_HOME, "/") {
		YTFS_HOME = YTFS_HOME + "/"
	}
	readClientProperties()
	initClientLog()
	port, err := GetFreePort()
	if err != nil {
		return
	}
	addr := fmt.Sprintf("0.0.0.0:%d", port)
	logrus.Infof("[Init]Starting pprof server on address %s\n", addr)
	go http.ListenAndServe(addr, nil)
}

func InitServer() {
	YTSN_HOME = os.Getenv("YTSN_HOME")
	if YTSN_HOME == "" {
		YTSN_HOME = GetCurrentPath()
	}
	if !strings.HasSuffix(YTSN_HOME, "/") {
		YTSN_HOME = YTSN_HOME + "/"
	}
	os.Setenv("YTSN_HOME", YTSN_HOME)
	os.Setenv("NODEMGMT_CONFIGDIR", YTSN_HOME+"conf")
	readSnProperties()
	initServerLog()
	ReadExport(YTSN_HOME + "bin/ytsn.ev")
	ReadExport(YTSN_HOME + "bin/ytsnd.sh")
}

func initClientLog() {
	logFileName := YTFS_HOME + "log/client.log"
	os.MkdirAll(YTFS_HOME+"log", os.ModePerm)
	initLog(logFileName, nil)
}

func initServerLog() {
	logFileName := YTSN_HOME + "log/server.log"
	nodelogFileName := YTSN_HOME + "log/nodemgr.log"
	os.MkdirAll(YTSN_HOME+"log", os.ModePerm)
	initLog(logFileName, nil)
	NodeMgrLog = logrus.New()
	initLog(nodelogFileName, NodeMgrLog)
}

func initLog(logFileName string, log *logrus.Logger) {
	logFile, logErr := os.OpenFile(logFileName, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0666)
	if logErr != nil {
		logrus.Panic("Fail to find", *logFile, "Server start Failed")
	}
	format := &Formatter{}
	lv, std := ParseLevel(LogLevel)
	if std {
		Console = true
	}
	hook, _ := NewHook(logFileName, format)
	if log != nil {
		log.SetLevel(lv)
		log.SetFormatter(format)
		if Console {
			log.SetOutput(os.Stdout)
		} else {
			log.SetOutput(logFile)
			if hook != nil {
				log.AddHook(hook)
			}
		}
	} else {
		logrus.SetFormatter(format)
		logrus.SetLevel(lv)
		if Console {
			logrus.SetOutput(os.Stdout)
		} else {
			logrus.SetOutput(logFile)
			if hook != nil {
				logrus.AddHook(hook)
			}
		}
	}
}

func NewHook(logName string, format *Formatter) (logrus.Hook, error) {
	writer, err := rotatelogs.New(
		logName+".%Y%m%d",
		rotatelogs.WithRotationTime(time.Hour*24),
	)
	if err != nil {
		return nil, err
	}
	lfsHook := lfshook.NewHook(lfshook.WriterMap{
		logrus.DebugLevel: writer,
		logrus.InfoLevel:  writer,
		logrus.WarnLevel:  writer,
		logrus.ErrorLevel: writer,
		logrus.FatalLevel: writer,
		logrus.PanicLevel: writer,
	}, format)
	return lfsHook, nil
}

var NodeMgrLog *logrus.Logger

type LogWrite struct {
}

func (l LogWrite) Write(p []byte) (n int, err error) {
	num := len(p)
	if NodeMgrLog == nil {
		return num, nil
	}
	if nodemgrLog == "OFF" {
		return num, nil
	}
	if nodemgrLog == "ON" && Console == false {
		if num > 20 {
			NodeMgrLog.Printf(string(p[20:]))
		} else {
			NodeMgrLog.Printf(string(p))
		}
		return num, nil
	}
	if num > 20 {
		logrus.Printf(string(p[20:]))
	} else {
		logrus.Printf(string(p))
	}
	return num, nil
}
