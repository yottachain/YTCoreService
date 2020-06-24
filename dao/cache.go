package dao

import (
	"context"
	"time"

	"github.com/yottachain/YTCoreService/env"
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
		env.Log.Errorf("UserID '%d' AddAction ERR:%s\n", action.UserID, err)
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
		env.Log.Errorf("UserID '%d' AddNewObject ERR:%s\n", userID, err)
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
		env.Log.Errorf("FindOneNewObject ERR:%s\n", err)
		return nil
	}
	return action
}
