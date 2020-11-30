package env

import "github.com/sirupsen/logrus"

func ULimit() {
	logrus.Infof("[SetLimit]Ulimit -a\n")
}
