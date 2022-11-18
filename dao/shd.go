package dao

import (
	"context"
	"strings"
	"time"

	"github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type ShardMeta struct {
	VFI     int64  `bson:"_id"`
	NodeId  int32  `bson:"nodeId"`
	VHF     []byte `bson:"VHF"`
	NodeId2 int32  `bson:"nodeId2"`
}

type ShardRebuidMeta struct {
	ID        int64 `bson:"_id"`
	VFI       int64 `bson:"VFI"`
	NewNodeId int32 `bson:"nid"`
	OldNodeId int32 `bson:"sid"`
}

func GetShardCountProgress() (int64, error) {
	source := NewBaseSource()
	filter := bson.M{"_id": 0}
	var result = struct {
		Lastid int64 `bson:"lastid"`
	}{}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err := source.GetShardCountColl().FindOne(ctx, filter).Decode(&result)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return 0, nil
		} else {
			logrus.Errorf("[ShardMeta]GetShardCountProgress ERR:%s\n", err)
			return 0, err
		}
	}
	return result.Lastid, nil
}

func SetShardCountProgress(id int64) error {
	source := NewBaseSource()
	filter := bson.M{"_id": 0}
	update := bson.M{"$set": bson.M{"lastid": id}}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	opt := options.Update().SetUpsert(true)
	_, err := source.GetShardCountColl().UpdateOne(ctx, filter, update, opt)
	if err != nil {
		logrus.Errorf("[ShardMeta]SetShardCountProgress ERR:%s\n", err)
		return err
	}
	return nil
}

func UpdateShardCount(hash map[int32]int64, firstid int64, lastid int64) error {
	f1 := "uspaces.sn0"
	operations := []mongo.WriteModel{}
	for k, v := range hash {
		b1 := bson.M{"_id": k}
		or1 := bson.M{"lstid": nil}
		or2 := bson.M{"lstid": bson.M{"$lte": firstid}}
		b2 := bson.M{"$or": []bson.M{or1, or2}}
		filter := bson.M{"$and": []bson.M{b1, b2}}
		mode := &mongo.UpdateOneModel{Filter: filter, Update: bson.M{"$inc": bson.M{f1: v}, "$set": bson.M{"lstid": lastid}}}
		operations = append(operations, mode)
	}
	source := NewDNIBaseSource()
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()
	_, err := source.GetNodeColl().BulkWrite(ctx, operations)
	if err != nil {
		return err
	}
	return nil
}

func UpdateShardMeta(metas []*ShardMeta, newid int32) error {
	source := NewBaseSource()
	operations := []mongo.WriteModel{}
	for _, v := range metas {
		if v.NodeId != -1 && v.NodeId2 != -1 {
			filter := bson.M{"_id": v.VFI}
			update := bson.M{"$set": bson.M{"nodeId": newid, "nodeId2": newid}}
			mode := &mongo.UpdateOneModel{Filter: filter, Update: update}
			operations = append(operations, mode)
		} else {
			if v.NodeId != -1 {
				filter := bson.M{"_id": v.VFI}
				update := bson.M{"$set": bson.M{"nodeId": newid}}
				mode := &mongo.UpdateOneModel{Filter: filter, Update: update}
				operations = append(operations, mode)
			}
			if v.NodeId2 != -1 {
				filter := bson.M{"_id": v.VFI}
				update := bson.M{"$set": bson.M{"nodeId2": newid}}
				mode := &mongo.UpdateOneModel{Filter: filter, Update: update}
				operations = append(operations, mode)
			}
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()
	_, err := source.GetShardColl().BulkWrite(ctx, operations)
	if err != nil {
		return err
	}
	return nil
}

func SaveShardRebuildMetas(ls []*ShardRebuidMeta) error {
	source := NewBaseSource()
	count := len(ls)
	obs := make([]interface{}, count)
	for ii, o := range ls {
		obs[ii] = o
	}
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()
	_, err := source.GetShardRebuildColl().InsertMany(ctx, obs)
	if err != nil {
		errstr := err.Error()
		if !strings.ContainsAny(errstr, "duplicate key error") {
			logrus.Errorf("[ShardMeta]SaveShardRebuildMetas ERR:%s\n", err)
			return err
		}
	}
	return nil
}

func SaveShardMetas(ls []*ShardMeta) error {
	source := NewBaseSource()
	count := len(ls)
	obs := make([]interface{}, count)
	for ii, o := range ls {
		obs[ii] = o
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := source.GetShardColl().InsertMany(ctx, obs)
	if err != nil {
		errstr := err.Error()
		if !strings.ContainsAny(errstr, "duplicate key error") {
			logrus.Errorf("[ShardMeta]SaveShardMetas ERR:%s\n", err)
			return err
		}
	}
	return nil
}

func GetShardNodes(ids []int64, srcnodeid int32) ([]*ShardMeta, []int64, error) {
	source := NewBaseSource()
	filter := bson.M{"_id": bson.M{"$in": ids}}
	fields := bson.M{"_id": 1, "nodeId": 1, "nodeId2": 1}
	opt := options.Find().SetProjection(fields)
	metas := []*ShardMeta{}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	cur, err := source.GetShardColl().Find(ctx, filter, opt)
	defer func() {
		if cur != nil {
			cur.Close(ctx)
		}
	}()
	if err != nil {
		logrus.Errorf("[ShardMeta]GetShardNodes ERR:%s\n", err)
		return nil, nil, err
	}
	listok := []int64{}
	for cur.Next(ctx) {
		var res = &ShardMeta{}
		err = cur.Decode(res)
		if err != nil {
			logrus.Errorf("[ShardMeta]GetShardNodes Decode ERR:%s\n", err)
			return nil, nil, err
		}
		if srcnodeid == res.NodeId || srcnodeid == res.NodeId2 {
			if srcnodeid != res.NodeId {
				res.NodeId = -1
			}
			if srcnodeid != res.NodeId2 {
				res.NodeId2 = -1
			}
			metas = append(metas, res)
		} else {
			logrus.Warnf("[ShardMeta]GetShardNodes ERR:Invalid SrcNodeID id %d\n", srcnodeid)
		}
		listok = append(listok, res.VFI)
	}
	if curerr := cur.Err(); curerr != nil {
		logrus.Errorf("[ShardMeta]GetShardNodes Cursor ERR:%s\n", curerr)
		return nil, nil, curerr
	}
	deletedIds := []int64{}
	if len(listok) != len(ids) {
		for _, id := range ids {
			has := false
			for _, sid := range listok {
				if id == sid {
					has = true
					break
				}
			}
			if !has {
				deletedIds = append(deletedIds, id)
			}
		}
		logrus.Warnf("[ShardMeta]GetShardNodes Return:%d,reqcount:%d\n", len(metas), len(ids))
	}
	return metas, deletedIds, nil
}

func GetShardMetas(vbi int64, count int) ([]*ShardMeta, error) {
	source := NewBaseSource()
	metas := []*ShardMeta{}
	ids := make([]int64, count)
	for ii := 0; ii < count; ii++ {
		ids[ii] = vbi + int64(ii)
	}
	filter := bson.M{"_id": bson.M{"$in": ids}}
	//filter := bson.M{"_id": bson.M{"$gte": vbi, "$lt": vbi + int64(count)}}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	cur, err := source.GetShardColl().Find(ctx, filter)
	defer func() {
		if cur != nil {
			cur.Close(ctx)
		}
	}()
	if err != nil {
		logrus.Errorf("[ShardMeta]GetShardMetas ERR:%s\n", err)
		return nil, err
	}
	for cur.Next(ctx) {
		var res = &ShardMeta{}
		err = cur.Decode(res)
		if err != nil {
			logrus.Errorf("[ShardMeta]GetShardMetas Decode ERR:%s\n", err)
			return nil, err
		}
		metas = append(metas, res)
	}
	if curerr := cur.Err(); curerr != nil {
		logrus.Errorf("[ShardMeta]GetShardMetas Cursor ERR:%s\n", curerr)
		return nil, curerr
	}
	return metas, nil
}
