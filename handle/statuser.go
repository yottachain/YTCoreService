package handle

import (
	"os"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/yottachain/YTCoreService/dao"
	"github.com/yottachain/YTCoreService/env"
	"github.com/yottachain/YTCoreService/net"
	"go.mongodb.org/mongo-driver/bson"
)

var log *env.NoFmtLog

func StatUser() {
	env.InitServer()
	dao.InitMongo()
	logrus.SetOutput(os.Stdout)
	net.InitNodeMgr(dao.MongoAddress)
	net.EOSInit()

	logname := env.YTSN_HOME + "/users.txt"
	os.Remove(logname)
	f, err := env.AddLog(logname)
	if err != nil {
		logrus.Panicf("[StatUser]Create LOG err:%s\n", err)
	}
	log = f
	log.Writer.Info("ID:用户ID\n")
	log.Writer.Info("UserName:用户名\n")
	log.Writer.Info("FileTotal:文件数\n")
	log.Writer.Info("SpaceTotal:存储总量(单位:bytes)\n")
	log.Writer.Info("Usedspace:实际占用空间(单位:bytes)\n")
	log.Writer.Info("Balance:用户HDD余额(单位:1/100000000 HDD)\n")
	log.Writer.Info("-------------------\n")
	defer log.Close()
	Iterate()
	logrus.Infof("[StatUser]STAT complete.\n")
	dao.Close()
}

func Iterate() {
	var lastId int32 = 0
	limit := 100
	for {
		us, err := dao.ListUsers(lastId, limit, bson.M{"_id": 1, "usedspace": 1, "username": 1, "spaceTotal": 1, "fileTotal": 1})
		if err != nil {
			time.Sleep(time.Duration(30) * time.Second)
			continue
		}
		if len(us) == 0 {
			break
		} else {
			for _, user := range us {
				lastId = user.UserID
				if net.GetUserSuperNode(user.UserID).ID != int32(env.SuperNodeID) {
					continue
				}
				b, err := GetBlance(user.Username)
				if err != nil {
					log.Writer.Infof("ID:%d\n", user.UserID)
					log.Writer.Infof("UserName:%s\n", user.Username)
					log.Writer.Infof("FileTotal:%d\n", user.FileTotal)
					log.Writer.Infof("SpaceTotal:%d\n", user.SpaceTotal)
					log.Writer.Infof("Usedspace:%d\n", user.Usedspace)
					log.Writer.Infof("Balance:%s\n", "Account ERR")
					log.Writer.Info("-------------------\n")
					logrus.Errorf("[StatUser]Failed to get balance:%s\n", err)
				} else {
					log.Writer.Infof("ID:%d\n", user.UserID)
					log.Writer.Infof("UserName:%s\n", user.Username)
					log.Writer.Infof("FileTotal:%d\n", user.FileTotal)
					log.Writer.Infof("SpaceTotal:%d\n", user.SpaceTotal)
					log.Writer.Infof("Usedspace:%d\n", user.Usedspace)
					log.Writer.Infof("Balance:%d\n", b)
					log.Writer.Info("-------------------\n")
					logrus.Infof("[StatUser]Get balance successfully!,UserName:%s", user.Username)
				}
				time.Sleep(time.Duration(5) * time.Second)
			}
		}
	}
}

func GetBlance(username string) (int64, error) {
	if username == "pollydevnew2" {
		return -5, nil
	}
	balance, err := net.GetBalance(username)
	if err != nil {
		return 0, err
	}
	return balance, nil
}
