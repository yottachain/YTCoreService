package dao

import (
	"context"
	"time"

	"github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func DelOrUpObject(uid int32, vnu primitive.ObjectID) (*ObjectMeta, error) {
	source := NewUserMetaSource(uint32(uid))
	filter := bson.M{"VNU": vnu, "NLINK": bson.M{"$lte": 1}}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	result := &ObjectMeta{}
	err := source.GetObjectColl().FindOneAndDelete(ctx, filter).Decode(result)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			result = nil
		} else {
			logrus.Errorf("[DelObject]DelOrUpObject UID %d,VNU %s,ERR:%s\n", uid, vnu.Hex(), err)
			return nil, err
		}
	}
	if result == nil {
		fmeta := &ObjectMeta{UserId: uid, VNU: vnu}
		fmeta.DECObjectNLINK()
		return nil, nil
	}
	logrus.Infof("[DelObject]DelOrUpObject UID %d,VNU %s OK\n", uid, vnu.Hex())
	usedspace := int64(result.Usedspace)
	length := int64(result.Length)
	if usedspace > 0 {
		UpdateUserSpace(uid, -usedspace, -1, -length)
	}
	return result, nil
}

func DelOrUpBLK(vbi int64) error {
	source := NewBaseSource()
	filter := bson.M{"_id": vbi, "NLINK": bson.M{"$lte": 1}}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	result := &BlockMeta{}
	err := source.GetBlockColl().FindOneAndDelete(ctx, filter).Decode(&result)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			result = nil
		} else {
			logrus.Errorf("[DelBlock]DelOrUpBLK %d,ERR:%s\n", vbi, err)
			return err
		}
	}
	if result == nil {
		return decBlockNLINK(vbi)
	}
	logrus.Infof("[DelBlock]DelOrUpBLK %d OK\n", vbi)
	er := DelShards(vbi, int(result.AR))
	if er != nil {
		return er
	}
	return decBlockCount()
}

func decBlockNLINK(vbi int64) error {
	source := NewBaseSource()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	filter := bson.M{"_id": vbi, "NLINK": bson.M{"$lte": 0xFFFFFF}}
	update := bson.M{"$inc": bson.M{"NLINK": -1}}
	res, err := source.GetBlockColl().UpdateOne(ctx, filter, update)
	if err != nil {
		logrus.Errorf("[DelBlock]DecBlockNLINK %d,ERR:%s\n", vbi, err)
		return err
	}
	if res.MatchedCount > 0 {
		decBlockNlinkCount()
	}
	return nil
}

func DelShards(vbi int64, count int) error {
	source := NewBaseSource()
	ids := make([]int64, count)
	for ii := 0; ii < count; ii++ {
		ids[ii] = vbi + int64(ii)
	}
	filter := bson.M{"_id": bson.M{"$in": ids}}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := source.GetShardColl().DeleteMany(ctx, filter)
	if err != nil {
		logrus.Errorf("[DelBlock]DelShards %d items ERR:%s\n", count, err)
		return err
	}
	logrus.Infof("[DelBlock]DelShards %d items OK\n", count)
	return nil
}

func decBlockNlinkCount() error {
	if true {
		return nil
	}
	source := NewBaseSource()
	filter := bson.M{"_id": 0}
	update := bson.M{"$inc": bson.M{"NLINK": -1}}
	opt := options.Update().SetUpsert(true)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := source.GetBlockCountColl().UpdateOne(ctx, filter, update, opt)
	if err != nil {
		logrus.Errorf("[DelBlock]DecBlockNlinkCount ERR:%s\n", err)
		return err
	}
	return nil
}

func decBlockCount() error {
	if true {
		return nil
	}
	source := NewBaseSource()
	filter := bson.M{"_id": 1}
	update := bson.M{"$inc": bson.M{"NLINK": -1}}
	opt := options.Update().SetUpsert(true)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := source.GetBlockCountColl().UpdateOne(ctx, filter, update, opt)
	if err != nil {
		logrus.Errorf("[DelBlock]DecBlockCount ERR:%s\n", err)
		return err
	}
	return nil
}
