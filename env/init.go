package env

import (
	"bufio"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	rotatelogs "github.com/lestrrat-go/file-rotatelogs"
	"github.com/rifflock/lfshook"
	easy "github.com/t-tomalak/logrus-easy-formatter"

	"github.com/sirupsen/logrus"
)

var YTSN_HOME string
var YTClient_HOME string

func InitClient() {
	YTClient_HOME = os.Getenv("YTClient_HOME")
	if YTClient_HOME == "" {
		dir, err := filepath.Abs(filepath.Dir(os.Args[0]))
		if err == nil {
			YTClient_HOME = dir
		}
	}
	if !strings.HasSuffix(YTClient_HOME, "/") {
		YTClient_HOME = YTClient_HOME + "/"
	}
	readClientProperties()
	initClientLog()
}

func InitServer() {
	YTSN_HOME = os.Getenv("YTSN_HOME")
	if YTSN_HOME == "" {
		dir, err := filepath.Abs(filepath.Dir(os.Args[0]))
		if err == nil {
			YTSN_HOME = dir
		} else {
			YTSN_HOME = "/app/ytsn"
		}
	}
	if !strings.HasSuffix(YTSN_HOME, "/") {
		YTSN_HOME = YTSN_HOME + "/"
	}
	os.Setenv("YTSN_HOME", YTSN_HOME)
	os.Setenv("NODEMGMT_CONFIGDIR", YTSN_HOME+"conf")
	readSnProperties()
	initServerLog()
	SetLimit()
	ReadExport(YTClient_HOME + "bin/ytsn.ev")
	ReadExport(YTClient_HOME + "bin/ytsnd.sh")
}

func initClientLog() {
	logFileName := YTClient_HOME + "log/client.log"
	os.MkdirAll(YTClient_HOME+"log", os.ModePerm)
	initLog(logFileName)
}

func initServerLog() {
	logFileName := YTSN_HOME + "log/server.log"
	os.MkdirAll(YTSN_HOME+"log", os.ModePerm)
	initLog(logFileName)
}

var Log *logrus.Logger

func initLog(logFileName string) {
	Log = logrus.New()
	logFile, logErr := os.OpenFile(logFileName, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0666)
	if logErr != nil {
		logrus.Panic("Fail to find", *logFile, "Server start Failed")
	}
	format := &easy.Formatter{
		TimestampFormat: time.StampMilli,
		LogFormat:       "[%lvl%][%time%]%msg%",
	}
	Log.SetFormatter(format)
	lv, err := logrus.ParseLevel(ServerLogLevel)
	if err != nil {
		Log.SetLevel(logrus.TraceLevel)
		Log.SetOutput(os.Stdout)
	} else {
		Log.SetOutput(logFile)
		Log.SetLevel(lv)
		hook, err := newHook(logFileName, format)
		if err == nil {
			Log.AddHook(hook)
		}
	}
}

func newHook(logName string, format *easy.Formatter) (logrus.Hook, error) {
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

func SetLimit() {
	var rLimit syscall.Rlimit
	err := syscall.Getrlimit(syscall.RLIMIT_NOFILE, &rLimit)
	if err != nil {
		Log.Errorf("Error Getting Rlimit%s\n ", err)
	}
	Log.Infof("Rlimit %d\n", rLimit)
	rLimit.Max = 655350
	rLimit.Cur = 655350
	err = syscall.Setrlimit(syscall.RLIMIT_NOFILE, &rLimit)
	if err != nil {
		Log.Errorf("Error Setting Rlimit %s\n", err)
	}
	err = syscall.Getrlimit(syscall.RLIMIT_NOFILE, &rLimit)
	if err != nil {
		Log.Errorf("Error Getting Rlimit %s\n", err)
	}
	Log.Infof("Rlimit Final%d\n", rLimit)
}

func ReadExport(path string) {
	f, err := os.Open(path)
	defer f.Close()
	if err != nil {
		return
	}
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
			if strings.Index(value, "$") < 0 {
				os.Setenv(key, value)
				Log.Infof("Set ENV %s=%s\n", key, value)
			}
		}
	}
}

func SaveConfig(path string, ss string) error {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_TRUNC|os.O_CREATE, 0644)
	defer f.Close()
	if err != nil {
		Log.Infof("Write properties file Err:%s\n", err)
		return err
	}
	f.Write([]byte(ss))
	return nil
}

func ReadConfig(path string) map[string]string {
	config := make(map[string]string)
	f, err := os.Open(path)
	defer f.Close()
	if err != nil {
		Log.Infoln("No properties file could be found for ytfs service")
	}
	r := bufio.NewReader(f)
	for {
		b, _, err := r.ReadLine()
		if err != nil {
			break
		}
		s := strings.TrimSpace(string(b))
		index := strings.Index(s, "=")
		if index < 0 {
			continue
		}
		key := strings.TrimSpace(s[:index])
		if len(key) == 0 {
			continue
		}
		value := strings.TrimSpace(s[index+1:])
		config[key] = value
	}
	return config
}

type LogWrite struct {
}

func (l LogWrite) Write(p []byte) (n int, err error) {
	if nodemgrLog == "off" {
		return 0, nil
	}
	num := len(p)
	if Log != nil {
		if num > 20 {
			Log.Printf(string(p[20:]))
		} else {
			Log.Printf(string(p))
		}
	}
	return num, nil
}
