package dao

import (
	"context"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/yottachain/YTCoreService/env"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type DelLOG struct {
	Id  primitive.ObjectID `bson:"_id"`
	UID int32              `bson:"UID"`
	VNU primitive.ObjectID `bson:"VNU"`
}

func AddDelLOG(uid int32, vnu primitive.ObjectID) error {
	action := &DelLOG{Id: primitive.NewObjectID(), UID: uid, VNU: vnu}
	source := NewCacheBaseSource()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := source.GetDELColl().InsertOne(ctx, action)
	if err != nil {
		logrus.Errorf("[CacheMeta]AddDelLOG UserID:%d,ERR:%s\n", uid, err)
		return err
	}
	return nil
}

func FindOneDelLOG() *DelLOG {
	source := NewCacheBaseSource()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	action := &DelLOG{}
	err := source.GetDELColl().FindOneAndDelete(ctx, bson.M{}).Decode(action)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return nil
		}
		logrus.Errorf("[CacheMeta]FindOneDelLOG ERR:%s\n", err)
		return nil
	}
	return action
}

func ListDelLOG(startID primitive.ObjectID, limit int) ([]*DelLOG, error) {
	source := NewCacheBaseSource()
	filter := bson.M{"_id": bson.M{"$gt": startID}}
	opt := options.Find().SetSort(bson.M{"_id": 1})
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()
	cur, err := source.GetDELColl().Find(ctx, filter, opt)
	defer func() {
		if cur != nil {
			cur.Close(ctx)
		}
	}()
	if err != nil {
		logrus.Errorf("[CacheMeta]ListDelLOG ERR:%s\n", err)
		return nil, err
	}
	VNUS := []*DelLOG{}
	for cur.Next(ctx) {
		var res = &DelLOG{}
		err = cur.Decode(res)
		if err != nil {
			logrus.Errorf("[CacheMeta]ListDelLOG Decode ERR:%s\n", err)
			return nil, err
		}
		VNUS = append(VNUS, res)
		if len(VNUS) > limit {
			break
		}
	}
	if curerr := cur.Err(); curerr != nil {
		logrus.Errorf("[CacheMeta]ListDelLOG Cursor ERR:%s, block count:%d\n", curerr, len(VNUS))
		return nil, curerr
	}
	return VNUS, nil
}

type Action struct {
	Id        primitive.ObjectID `bson:"_id"`
	Step      int                `bson:"step"`
	UsedSpace uint64             `bson:"usedSpace"`
	UserID    int32              `bson:"userid"`
	Username  string             `bson:"username"`
}

func (me *Action) RoundUsedSpace() {
	unitspace := uint64(env.PFL)
	addusedspace := me.UsedSpace / unitspace
	if me.UsedSpace%unitspace > 1 {
		addusedspace = addusedspace + 1
	}
	me.UsedSpace = addusedspace * unitspace
}

func AddAction(action *Action) error {
	return AddNewObject(action.Id, action.UsedSpace, action.UserID, action.Username, action.Step)
}

func AddNewObject(id primitive.ObjectID, usedSpace uint64, userID int32, username string, step int) error {
	action := &Action{Id: id, Step: step, UsedSpace: usedSpace, UserID: userID, Username: username}
	source := NewCacheBaseSource()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := source.GetOBJColl().InsertOne(ctx, action)
	if err != nil {
		logrus.Errorf("[CacheMeta]AddNewObject UserID:%d,ERR:%s\n", userID, err)
		return err
	}
	return nil
}

const FIND_ID_LIMIT = 500

func FindAndDeleteNewObject() *Action {
	act, ids := ListNewObject()
	if act != nil {
		err := DeleteNewObjects(ids)
		if err != nil {
			return nil
		}
	}
	return act
}

func DeleteNewObjects(ids []primitive.ObjectID) error {
	filter := bson.M{"_id": bson.M{"$in": ids}}
	source := NewCacheBaseSource()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	_, err := source.GetOBJColl().DeleteMany(ctx, filter)
	if err != nil {
		logrus.Errorf("[CacheMeta]DeleteNewObjects ERR:%s\n", err)
		return err
	}
	return nil
}

func ListNewObject() (*Action, []primitive.ObjectID) {
	source := NewCacheBaseSource()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	cur, err := source.GetOBJColl().Find(ctx, bson.M{})
	defer func() {
		if cur != nil {
			cur.Close(ctx)
		}
	}()
	if err != nil {
		logrus.Errorf("[CacheMeta]ListNewObject ERR:%s\n", err)
		return nil, nil
	}
	var first *Action
	ids := []primitive.ObjectID{}
	loop := 0
	for cur.Next(ctx) {
		loop++
		res := &Action{}
		err = cur.Decode(res)
		if err != nil {
			logrus.Errorf("[CacheMeta]ListNewObject Decode ERR:%s\n", err)
			return nil, nil
		}
		if first == nil {
			first = res
			first.RoundUsedSpace()
			ids = append(ids, res.Id)
		} else {
			if first.UserID == res.UserID && first.Step == res.Step && len(ids) < FIND_ID_LIMIT {
				res.RoundUsedSpace()
				ids = append(ids, res.Id)
				first.UsedSpace = first.UsedSpace + res.UsedSpace
				loop = 0
			} else {
				if loop > FIND_ID_LIMIT {
					break
				}
			}
		}
	}
	if err := cur.Err(); err != nil {
		logrus.Errorf("[CacheMeta]ListNewObject Cursor ERR:%s\n", err)
		return nil, nil
	}
	return first, ids
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
		logrus.Errorf("[CacheMeta]FindOneNewObject ERR:%s\n", err)
		return nil
	}
	return action
}

func SetUserSumTime(userid int32) error {
	source := NewCacheBaseSource()
	filter := bson.M{"_id": userid}
	data := bson.M{"$set": bson.M{"statTime": time.Now().Unix() * 1000}}
	opt := options.Update().SetUpsert(true)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := source.GetSumColl().UpdateOne(ctx, filter, data, opt)
	if err != nil {
		logrus.Errorf("[CacheMeta]SetUserSumTime UserID:%d,ERR:%s\n", userid, err)
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
		StatTime int64 `bson:"statTime"`
	}{}
	err := source.GetSumColl().FindOne(ctx, filter).Decode(&result)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return 0, nil
		} else {
			logrus.Errorf("[CacheMeta]GetUserSumTime ERR:%s\n", err)
			return 0, err
		}
	}
	return result.StatTime, err
}
