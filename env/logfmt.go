package env

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/sirupsen/logrus"
)

type Formatter struct {
	NoPrefix bool
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
const LogFormat = "[%time%][%lvl%]%msg%"

func (f *Formatter) Format(entry *logrus.Entry) ([]byte, error) {
	if f.NoPrefix {
		return []byte(entry.Message), nil
	}
	output := LogFormat
	timestampFormat := TimestampFormat
	output = strings.Replace(output, "%time%", entry.Time.Format(timestampFormat), 1)
	output = strings.Replace(output, "%msg%", entry.Message, 1)
	output = strings.Replace(output, "%lvl%", GetLevelString(entry.Level), 1)
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
