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

func DelOrUpObject(uid int32, vnu primitive.ObjectID, up bool, del bool) (*ObjectMeta, error) {
	source := NewUserMetaSource(uint32(uid))
	filter := bson.M{"VNU": vnu, "NLINK": bson.M{"$lt": 1}}
	if up {
		filter = bson.M{"VNU": vnu, "NLINK": bson.M{"$lte": 1}}
	}
	if del {
		filter = bson.M{"VNU": vnu}
	}
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
	if !up {
		logrus.Infof("[DelObject]DelOrUpObject UID %d,VNU %s OK\n", uid, vnu.Hex())		 
		return result, nil
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

func DelOrUpBLK(vbi int64) ([]*ShardMeta, error) {
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
			return nil, err
		}
	}
	if result == nil {
		logrus.Infof("[DelBlock]DelOrUpBLK %d ignored,refer count >1\n", vbi)
		return nil, decBlockNLINK(vbi)
	}
	logrus.Infof("[DelBlock]DelOrUpBLK %d OK\n", vbi)
	DeleteLog(filter, source.GetBlockColl().Name(), false)
	bkid := GenerateShardID(1)
	if result.VNF == 0 {
		DelBLKData(vbi)
		SaveBlockBakup(bkid, vbi)
		return nil, decBlockCount()
	} else {
		shds, er := DelShards(vbi, int(result.VNF))
		if er != nil {
			return nil, er
		} else {
			SaveBlockBakup(bkid, vbi)
			return shds, decBlockCount()
		}
	}
}

func DelBLKData(vbi int64) {
	source := NewBaseSource()
	filter := bson.M{"_id": vbi}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := source.GetBlockDataColl().DeleteOne(ctx, filter)
	if err != nil {
		logrus.Errorf("[DelBlock]DelBLKData ERR:%s\n", err)
	} else {
		DeleteLog(filter, source.GetBlockDataColl().Name(), false)
		logrus.Infof("[DelBlock]DelBLKData %d OK\n", vbi)
	}
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
	UpdateLog(filter, update, source.GetBlockDataColl().Name(), false)
	if res.MatchedCount > 0 {
		decBlockNlinkCount()
	}
	return nil
}

func DelShards(vbi int64, count int) ([]*ShardMeta, error) {
	var shds []*ShardMeta = nil
	if env.DelLogPath != "" {
		metas, er := GetShardMetas(vbi, count)
		if er != nil {
			return nil, er
		} else {
			shds = metas
		}
	}
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
		logrus.Errorf("[DelBlock][%d]DelShards %d items ERR:%s\n", vbi, count, err)
		return nil, err
	}
	DeleteLog(filter, source.GetShardColl().Name(), true)
	logrus.Infof("[DelBlock][%d]DelShards %d items OK\n", vbi, count)
	return shds, nil
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
