package env

import (
	"strconv"
	"strings"

	"github.com/sirupsen/logrus"
)

type Formatter struct {
}

func GetLevelString(level logrus.Level) string {
	switch level {
	case logrus.TraceLevel:
		return "Trace"
	case logrus.DebugLevel:
		return "Debug"
	case logrus.InfoLevel:
		return "Info "
	case logrus.WarnLevel:
		return "Warn "
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
