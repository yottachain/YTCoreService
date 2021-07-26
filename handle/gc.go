package handle

import (
	"time"

	"github.com/sirupsen/logrus"
	"github.com/yottachain/YTCoreService/dao"
	"github.com/yottachain/YTCoreService/env"
	"github.com/yottachain/YTCoreService/net"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

func StartGC() {
	if !env.GC {
		return
	}
	if !net.IsActive() {
		return
	}
	for {
		time.Sleep(time.Duration(10 * time.Minute))
		ListUser(true)
		ListUser(false)
		time.Sleep(time.Duration(3 * time.Hour))
	}
}

func ListUser(InArrears bool) {
	defer env.TracePanic("[GC]")
	var lastId int32 = 0
	limit := 100
	logrus.Infof("[GC]Start iterate user...\n")
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
					logrus.Errorf("[GC][%s][%d]Failed to get balance:%s\n", user.Username, user.UserID, err)
					time.Sleep(time.Duration(5) * time.Second)
					continue
				} else {
					logrus.Infof("[GC][%s][%d]Get balance successfully:%d\n", user.Username, user.UserID, b)
					if b >= 0 {
						if InArrears {
							continue
						}
						logrus.Infof("[GC][%s][%d]Start clearing unreferenced data......\n", user.Username, user.UserID)
						IterateObjects(user, false)
					} else {
						if !InArrears {
							continue
						}
						var expired bool = false
						lasttime, err := dao.GetLastAccessTime(uint32(user.UserID))
						if err != nil {
							logrus.Errorf("[GC][%s][%d]Get user lastAccessTime err:%s\n", user.Username, user.UserID, err)
							continue
						} else {
							if time.Now().Unix()-lasttime.Unix() > 60*60*24*30 {
								logrus.Infof("[GC][%s][%d]Be in arrears but not renewed,LastAccessTime:%s\n", user.Username, user.UserID, lasttime.Format("2006-01-02 15:04:05"))
								expired = true
							} else {
								logrus.Infof("[GC][%s][%d]Be in arrears but not expired,LastAccessTime:%s\n", user.Username, user.UserID, lasttime.Format("2006-01-02 15:04:05"))
							}
							if user.Username == "pollydevnew2" {
								logrus.Infof("[GC][%s][%d]Force delete...\n", user.Username, user.UserID)
								expired = true
							}
						}
						if expired {
							logrus.Infof("[GC][%s][%d]Start deleting data......\n", user.Username, user.UserID)
							IterateObjects(user, true)
						} else {
							logrus.Infof("[GC][%s][%d]Start clearing unreferenced data......\n", user.Username, user.UserID)
							IterateObjects(user, false)
						}
						logrus.Infof("[GC][%s][%d]Delete completed.\n", user.Username, user.UserID)
					}
				}
			}
		}
	}
	logrus.Infof("[GC]Iterate user OK!\n")
}

func IterateObjects(user *dao.User, del bool) {
	firstId := primitive.NilObjectID
	for {
		vnus, err := dao.ListObjectsForDel(uint32(user.UserID), firstId, 1, del)
		if err != nil {
			time.Sleep(time.Duration(30) * time.Second)
			continue
		}
		if len(vnus) == 0 {
			break
		}
		for _, vnu := range vnus {
			if time.Now().Unix()-vnu.Timestamp().Unix() >= 60*5 {
				_, found := Upload_CACHE.Get(vnu.Hex())
				if !found {
					DelBlocks(user.UserID, vnu, false, del)
				}
			}
			firstId = vnu
		}
		if firstId == primitive.NilObjectID {
			break
		}
	}
}
