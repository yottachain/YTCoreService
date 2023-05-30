package env

import (
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"time"

	rotatelogs "github.com/lestrrat-go/file-rotatelogs"
	"github.com/rifflock/lfshook"
	"github.com/sirupsen/logrus"
)

var LogLevel string = "trace,std"
var LogClean int = 3
var Console bool = false
var StdLog string = "OFF"

func init() {
	log.SetOutput(&LogWrite{})
}

func logConfig(config *Config) {
	LogLevel = config.GetString("logLevel", "trace,stdout")
	LogClean = config.GetRangeInt("logClean", 0, 50000, 3)
	StdLog = config.GetUpperString("stdLog", "OFF")
}

func InitLog(LOG_HOME string, prefix string, log *logrus.Logger) {
	format := &Formatter{NoPrefix: false}
	lv, std := ParseLevel(LogLevel)
	logFileName := LOG_HOME + "log/" + prefix
	hook, _ := NewHook(logFileName, format)
	log.SetFormatter(format)
	log.SetLevel(lv)
	if std || Console || hook == nil {
		log.SetOutput(os.Stdout)
	} else {
		os.MkdirAll(LOG_HOME+"log", os.ModePerm)
		log.AddHook(hook)
		go func() {
			for {
				time.Sleep(time.Second * 5)
				clearLog(logFileName, prefix)
				time.Sleep(time.Minute * 30)
			}
		}()
	}
}

func clearLog(logName string, prefix string) {
	if LogClean < 1 {
		return
	}
	logs := []string{filepath.Base(logName)}
	cur := time.Now()
	for ii := 0; ii < LogClean; ii++ {
		name := logName + "." + cur.Format("20060102")
		logs = append(logs, filepath.Base(name))
		cur = cur.Add(-time.Hour * 24)
	}
	dir := filepath.Dir(logName)
	l, err := ioutil.ReadDir(dir)
	if err != nil {
		return
	}
	for _, f := range l {
		if strings.HasPrefix(f.Name(), prefix) {
			del := true
			for _, n := range logs {
				if n == f.Name() {
					del = false
					break
				}
			}
			if del {
				os.Remove(dir + "/" + f.Name())
			}
		}
	}
}

func TraceError(prefix string) {
	stack := make([]byte, 2048)
	length := runtime.Stack(stack, true)
	ss := string(stack[0:length])
	ls := strings.Split(ss, "\n")
	for _, s := range ls {
		logrus.Error(prefix + s + "\n")
	}
}

func TracePanic(prefix string) {
	if r := recover(); r != nil {
		TraceError(prefix)
	}
}

func TraceErrors(prefix string) string {
	stack := make([]byte, 2048)
	length := runtime.Stack(stack, true)
	ss := string(stack[0:length])
	ls := strings.Split(ss, "\n")
	for _, s := range ls {
		logrus.Error(prefix + s + "\n")
	}
	return ss
}

func NewLogger(name string) *logrus.Logger {
	if name == "" {
		return logrus.StandardLogger()
	}
	log := &logrus.Logger{
		Out:          logrus.StandardLogger().Out,
		Hooks:        logrus.StandardLogger().Hooks,
		Level:        logrus.StandardLogger().Level,
		ExitFunc:     logrus.StandardLogger().ExitFunc,
		ReportCaller: logrus.StandardLogger().ReportCaller,
	}
	format := &Formatter{NoPrefix: false, Prefix: "[" + name + "]"}
	log.SetFormatter(format)
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
		logrus.TraceLevel: writer,
		logrus.DebugLevel: writer,
		logrus.InfoLevel:  writer,
		logrus.WarnLevel:  writer,
		logrus.ErrorLevel: writer,
		logrus.FatalLevel: writer,
		logrus.PanicLevel: writer,
	}, format)
	return lfsHook, nil
}

type Formatter struct {
	NoPrefix bool
	Prefix   string
}

func ParseLevel(lvl string) (logrus.Level, bool) {
	s := strings.ToLower(lvl)
	var lv logrus.Level
	if strings.Contains(s, "panic") {
		lv = logrus.PanicLevel
	} else if strings.Contains(s, "fatal") {
		lv = logrus.FatalLevel
	} else if strings.Contains(s, "error") {
		lv = logrus.ErrorLevel
	} else if strings.Contains(s, "warn") || strings.Contains(s, "warning") {
		lv = logrus.WarnLevel
	} else if strings.Contains(s, "info") {
		lv = logrus.InfoLevel
	} else if strings.Contains(s, "debug") {
		lv = logrus.DebugLevel
	} else {
		lv = logrus.TraceLevel
	}
	if strings.Contains(s, "std") {
		return lv, true
	} else {
		return lv, false
	}
}

func GetLevelString(level logrus.Level) string {
	switch level {
	case logrus.TraceLevel:
		return "Trace"
	case logrus.DebugLevel:
		return "Debug"
	case logrus.InfoLevel:
		return "Infos"
	case logrus.WarnLevel:
		return "Warns"
	case logrus.ErrorLevel:
		return "Error"
	case logrus.FatalLevel:
		return "Fatal"
	case logrus.PanicLevel:
		return "Panic"
	}
	return "Debug"
}

const TimestampFormat = "15:04:05.000"
const FormatString = "[%s][%s]%s%s"

func (f *Formatter) Format(entry *logrus.Entry) ([]byte, error) {
	if f.NoPrefix {
		if strings.HasSuffix(entry.Message, "\n") {
			return []byte(entry.Message), nil
		} else {
			return []byte(entry.Message + "\n"), nil
		}
	}
	output := FormatString
	if !strings.HasSuffix(entry.Message, "\n") {
		output = FormatString + "\n"
	}
	output = fmt.Sprintf(output, entry.Time.Format(TimestampFormat), GetLevelString(entry.Level), f.Prefix, entry.Message)
	for k, val := range entry.Data {
		switch v := val.(type) {
		case string:
			output = strings.Replace(output, "%"+k+"%", v, 1)
		case int:
			s := strconv.Itoa(v)
			output = strings.Replace(output, "%"+k+"%", s, 1)
		case bool:
			s := strconv.FormatBool(v)
			output = strings.Replace(output, "%"+k+"%", s, 1)
		}
	}
	return []byte(output), nil
}

var STDLog *logrus.Logger = logrus.New()

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
	if StdLog == "ON" && !Console {
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

type NoFmtLog struct {
	Writer *logrus.Logger
	Closer io.Closer
}

func (me *NoFmtLog) Close() {
	if me.Closer != nil {
		me.Closer.Close()
	}
}

func NewNoFmtLog(logFileName string) (*NoFmtLog, error) {
	log := logrus.New()
	writer, err := os.OpenFile(logFileName, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return nil, err
	}
	log.Level = logrus.TraceLevel
	log.Formatter = &Formatter{NoPrefix: true}
	log.Out = writer
	mylog := &NoFmtLog{Writer: log, Closer: writer}
	return mylog, nil
}
