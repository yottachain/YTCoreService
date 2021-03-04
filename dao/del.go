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

func DelObject(uid int32, VNU primitive.ObjectID) (*ObjectMeta, error) {
	source := NewUserMetaSource(uint32(uid))
	filter := bson.M{"VNU": VNU, "NLINK": bson.M{"$lt": 1}}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	result := &ObjectMeta{}
	err := source.GetObjectColl().FindOneAndDelete(ctx, filter).Decode(result)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			result = nil
		} else {
			logrus.Errorf("[ObjectMeta]DelObject ERR:%s\n", err)
			return nil, err
		}
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
			logrus.Errorf("[BlockMeta]DelOrUpBLK ERR:%s\n", err)
			return err
		}
	}
	if result == nil {
		return decBlockNLINK(vbi)
	}
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
		logrus.Errorf("[BlockMeta]DecBlockNLINK ERR:%s\n", err)
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
		logrus.Errorf("[BlockMeta]FindShdAndDel ERR:%s\n", err)
		return err
	}
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
		logrus.Errorf("[BlockMeta]decBlockNlinkCount ERR:%s\n", err)
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
		logrus.Errorf("[BlockMeta]DecBlockCount ERR:%s\n", err)
		return err
	}
	return nil
}
