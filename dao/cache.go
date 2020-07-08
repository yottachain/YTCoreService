package dao

import (
	"context"
	"time"

	"github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
)

type Action struct {
	Id        primitive.ObjectID `bson:"_id"`
	Step      int                `bson:"step"`
	UsedSpace uint64             `bson:"usedSpace"`
	UserID    int32              `bson:"userid"`
	Username  string             `bson:"username"`
}

func AddAction(action *Action) error {
	source := NewCacheBaseSource()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := source.GetOBJColl().InsertOne(ctx, action)
	if err != nil {
		logrus.Errorf("[AddAction]UserID:%d,ERR:%s\n", action.UserID, err)
		return err
	}
	return nil
}

func AddNewObject(id primitive.ObjectID, usedSpace uint64, userID int32, username string, step int) error {
	action := &Action{Id: id, Step: step, UsedSpace: usedSpace, UserID: userID, Username: username}
	source := NewCacheBaseSource()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := source.GetOBJColl().InsertOne(ctx, action)
	if err != nil {
		logrus.Errorf("[AddNewObject]UserID:%d,ERR:%s\n", userID, err)
		return err
	}
	return nil
}

func FindOneNewObject() *Action {
	source := NewCacheBaseSource()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	action := &Action{}
	err := source.GetOBJColl().FindOneAndDelete(ctx, bson.M{}).Decode(action)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return nil
		}
		logrus.Errorf("[FindOneNewObject]ERR:%s\n", err)
		return nil
	}
	return action
}

func SetUserSumTime(userid int32) error {
	source := NewCacheBaseSource()
	filter := bson.M{"_id": userid}
	data := bson.M{"$set": bson.M{"statTime": time.Now().Unix() * 1000}}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := source.GetSumColl().UpdateOne(ctx, filter, data)
	if err != nil {
		logrus.Errorf("[SetUserSumTime]UserID:%d,ERR:%s\n", userid, err)
		return err
	}
	return nil
}

func GetUserSumTime(userid int32) (int64, error) {
	source := NewCacheBaseSource()
	filter := bson.M{"_id": userid}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var result = struct {
		statTime int64 `bson:"statTime"`
	}{}
	err := source.GetSumColl().FindOne(ctx, filter).Decode(&result)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return 0, nil
		} else {
			logrus.Errorf("[GetUserSumTime]ERR:%s\n", err)
			return 0, err
		}
	}
	return result.statTime, err
}
