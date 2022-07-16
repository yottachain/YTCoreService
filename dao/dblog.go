package dao

import (
	"context"
	"time"

	"github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

const UPDATE_ONE = 0
const UPDATE_MANY = 1

const DELETE_ONE = 10
const DELETE_MANY = 11

type DBLog struct {
	Id     primitive.ObjectID `bson:"_id"`
	Coll   string             `bson:"coll"`
	Type   int8               `bson:"type"`
	Filter []byte             `bson:"filter"`
	Update []byte             `bson:"update"`
}

func (dblog *DBLog) Execute() error {
	filter := bson.M{}
	err := bson.Unmarshal(dblog.Filter, filter)
	if err != nil {
		logrus.Errorf("[OPLogs]Unmarshal filter ERR:%s\n", err)
		return err
	}
	update := bson.M{}
	if dblog.Type == UPDATE_ONE || dblog.Type == UPDATE_MANY {
		err := bson.Unmarshal(dblog.Update, update)
		if err != nil {
			logrus.Errorf("[OPLogs]Unmarshal update ERR:%s\n", err)
			return err
		}
	}
	source := NewBaseSource()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if dblog.Type == UPDATE_ONE {
		_, err = source.GetColl(dblog.Coll).UpdateOne(ctx, filter, update)
	} else if dblog.Type == UPDATE_MANY {
		_, err = source.GetColl(dblog.Coll).UpdateMany(ctx, filter, update)
	} else if dblog.Type == DELETE_ONE {
		_, err = source.GetColl(dblog.Coll).DeleteOne(ctx, filter)
	} else if dblog.Type == DELETE_MANY {
		_, err = source.GetColl(dblog.Coll).DeleteMany(ctx, filter)
	}
	if err != nil {
		logrus.Errorf("[OPLogs]Execute ERR:%s\n", err)
		return err
	}
	return nil
}

func UpdateOP(filter bson.M, update bson.M, collname string, many bool) (*DBLog, error) {
	f, err := bson.Marshal(filter)
	if err != nil {
		logrus.Errorf("[OPLogs]Update,Marshal filter ERR:%s\n", err)
		return nil, err
	}
	u, err := bson.Marshal(update)
	if err != nil {
		logrus.Errorf("[OPLogs]Update,Marshal update ERR:%s\n", err)
		return nil, err
	}
	log := &DBLog{Id: primitive.NewObjectID(), Coll: collname, Type: UPDATE_ONE, Filter: f, Update: u}
	if many {
		log.Type = UPDATE_MANY
	}
	return log, nil
}

func Save(logs []*DBLog) error {
	source := NewBaseSource()
	count := len(logs)
	obs := make([]interface{}, count)
	for ii, o := range logs {
		obs[ii] = o
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := source.GetOPLogsColl().InsertMany(ctx, obs)
	if err != nil {
		logrus.Errorf("[OPLogs]Save ERR:%s\n", err)
		return err
	}
	return nil
}

func UpdateLog(filter bson.M, update bson.M, collname string, many bool) error {
	f, err := bson.Marshal(filter)
	if err != nil {
		logrus.Errorf("[OPLogs]Update,Marshal filter ERR:%s\n", err)
		return err
	}
	u, err := bson.Marshal(update)
	if err != nil {
		logrus.Errorf("[OPLogs]Update,Marshal update ERR:%s\n", err)
		return err
	}
	log := &DBLog{Id: primitive.NewObjectID(), Coll: collname, Type: UPDATE_ONE, Filter: f, Update: u}
	if many {
		log.Type = UPDATE_MANY
	}
	source := NewBaseSource()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err = source.GetOPLogsColl().InsertOne(ctx, log)
	if err != nil {
		logrus.Errorf("[OPLogs]Update ERR:%s\n", err)
		return err
	}
	return nil
}

func DeleteLog(filter bson.M, collname string, many bool) error {
	f, err := bson.Marshal(filter)
	if err != nil {
		logrus.Errorf("[OPLogs]Delete,Marshal filter ERR:%s\n", err)
		return err
	}
	log := &DBLog{Id: primitive.NewObjectID(), Coll: collname, Type: DELETE_ONE, Filter: f, Update: []byte{}}
	if many {
		log.Type = DELETE_MANY
	}
	source := NewBaseSource()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err = source.GetOPLogsColl().InsertOne(ctx, log)
	if err != nil {
		logrus.Errorf("[OPLogs]Delete ERR:%s\n", err)
		return err
	}
	return nil
}
