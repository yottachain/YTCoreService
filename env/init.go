package env

import (
	"fmt"
	"log"
	"net/http"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"runtime"
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

func SetLimit() {
	sysType := runtime.GOOS
	if sysType == "linux" {
		ULimit()
	}
}

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
	initClientLog()
	port, err := GetFreePort()
	if err != nil {
		return
	}
	addr := fmt.Sprintf("0.0.0.0:%d", port)
	logrus.Infof("[Init]Starting pprof server on address %s\n", addr)
	SetLimit()
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
	initServerLog()
	ReadExport(YTSN_HOME + "bin/ytsn.ev")
	ReadExport(YTSN_HOME + "bin/ytsnd.sh")
	SetLimit()
}

func initClientLog() {
	logFileName := YTFS_HOME + "log/log"
	os.MkdirAll(YTFS_HOME+"log", os.ModePerm)
	initLog(logFileName, nil)
}

func initServerLog() {
	logFileName := YTSN_HOME + "log/log"
	nodelogFileName := YTSN_HOME + "log/std"
	os.MkdirAll(YTSN_HOME+"log", os.ModePerm)
	initLog(logFileName, nil)
	STDLog = logrus.New()
	initLog(nodelogFileName, STDLog)
}

func initLog(logFileName string, log *logrus.Logger) {
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
			if hook != nil {
				log.AddHook(hook)
			} else {
				log.SetOutput(os.Stdout)
			}
		}
	} else {
		logrus.SetFormatter(format)
		logrus.SetLevel(lv)
		if Console {
			logrus.SetOutput(os.Stdout)
		} else {
			if hook != nil {
				logrus.AddHook(hook)
			} else {
				logrus.SetOutput(os.Stdout)
			}
		}
	}
}

func AddLog(logFileName string) *logrus.Logger {
	format := &Formatter{}
	log := logrus.New()
	log.Level = logrus.TraceLevel
	log.Formatter = format
	hook, _ := NewHook(logFileName, format)
	if hook != nil {
		log.AddHook(hook)
	}
	return log
}

func NewHook(logName string, format *Formatter) (logrus.Hook, error) {
	writer, err := rotatelogs.New(
		logName+".%Y%m%d",
		rotatelogs.WithRotationTime(time.Hour*24),
		rotatelogs.WithLinkName(logName),
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

var STDLog *logrus.Logger

type LogWrite struct {
}

func (l LogWrite) Write(p []byte) (n int, err error) {
	num := len(p)
	if STDLog == nil {
		return num, nil
	}
	if StdLog == "OFF" {
		return num, nil
	}
	if StdLog == "ON" && Console == false {
		if num > 20 {
			STDLog.Printf(string(p[20:]))
		} else {
			STDLog.Printf(string(p))
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
