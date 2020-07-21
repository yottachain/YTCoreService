package env

import (
	"bufio"
	"bytes"
	"os"
	"strconv"
	"strings"

	"github.com/sirupsen/logrus"
)

type Config struct {
	path string
	prop map[string]string
	sec  string
}

func NewConfig(p string) (*Config, error) {
	config := make(map[string]string)
	f, err := os.Open(p)
	defer f.Close()
	if err != nil {
		return nil, err
	}
	r := bufio.NewReader(f)
	for {
		b, _, err := r.ReadLine()
		if err != nil {
			break
		}
		s := strings.TrimSpace(string(b))
		if strings.HasPrefix(s, "#") {
			continue
		}
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
	cfg := &Config{path: p, prop: config, sec: ""}
	return cfg, nil
}

func (c *Config) SetSection(s string) {
	c.sec = s
}

func (c *Config) GetString(key string, def string) string {
	var v string = ""
	if c.sec != "" {
		v = strings.TrimSpace(os.Getenv(c.sec + "." + key))
	}
	if v == "" {
		v = strings.TrimSpace(c.prop[key])
	}
	if v == "" {
		v = def
	}
	return v
}

func (c *Config) HasValue(key string) bool {
	s := c.GetString(key, "")
	return s != ""
}

func (c *Config) HasIntValue(key string) (int, bool) {
	s := c.GetString(key, "")
	num, err := strconv.Atoi(s)
	if err != nil {
		return 0, false
	} else {
		return num, true
	}
}

func (c *Config) GetUpperString(key string, def string) string {
	return strings.ToUpper(c.GetString(key, def))
}

func (c *Config) GetLowerString(key string, def string) string {
	return strings.ToLower(c.GetString(key, def))
}

func (c *Config) GetBool(key string, def bool) bool {
	s := c.GetUpperString(key, "")
	if def == true {
		if s == "FALSE" || s == "OFF" {
			return false
		} else {
			return true
		}
	} else {
		if s == "TRUE" || s == "ON" {
			return true
		} else {
			return false
		}
	}
}

func (c *Config) GetInt(key string, def int) int {
	s := c.GetString(key, strconv.Itoa(def))
	num, err := strconv.Atoi(s)
	if err != nil {
		return def
	}
	return num
}

func (c *Config) GetRangeInt(key string, min int, max int, def int) int {
	s := c.GetString(key, strconv.Itoa(def))
	num, err := strconv.Atoi(s)
	if err != nil {
		return def
	}
	return CheckInt(num, min, max)
}

func (c *Config) SaveValue(key string, value string) error {
	file, err := os.OpenFile(c.path, os.O_RDWR, 0666)
	if err != nil {
		logrus.Errorf("[Init]Failed to read %s:%s\n", c.path, err)
		return err
	}
	defer file.Close()
	var content bytes.Buffer
	var findkey bool = false
	reader := bufio.NewReader(file)
	for {
		bytes, _, _ := reader.ReadLine()
		if len(bytes) == 0 {
			break
		}
		lineString := string(bytes)
		if strings.HasPrefix(lineString, key) {
			findkey = true
			content.WriteString(key + "=" + value + "\n")
		} else {
			content.WriteString(lineString + "\n")
		}
	}
	if !findkey {
		content.WriteString(key + "=" + value + "\n")
	}
	f, err := os.OpenFile(c.path, os.O_WRONLY|os.O_TRUNC|os.O_CREATE, 0644)
	defer f.Close()
	if err != nil {
		logrus.Errorf("[Init]Write properties file Err:%s\n", err)
		return err
	}
	f.Write(content.Bytes())
	return nil
}

func ReadExport(path string) {
	f, err := os.Open(path)
	defer f.Close()
	if err != nil {
		logrus.Errorf("[Init]Read export %s ERR: %s\n", path, err)
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
				logrus.Infof("[Init]Set ENV %s=%s\n", key, value)
			}
		}
	}
}

func CheckInt(num int, min int, max int) int {
	if num < min {
		return min
	}
	if num > max {
		return max
	}
	return num
}

func ToInt(src string, def int) int {
	num, err := strconv.Atoi(strings.Trim(src, " "))
	if err != nil {
		return def
	}
	return num
}

func StringToInt(src string, min int, max int, def int) int {
	num, err := strconv.Atoi(strings.Trim(src, " "))
	if err != nil {
		return def
	}
	return CheckInt(num, min, max)
}
