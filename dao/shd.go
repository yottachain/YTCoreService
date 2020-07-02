package dao

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/yottachain/YTCoreService/env"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type ShardMeta struct {
	VFI    int64  `bson:"_id"`
	NodeId int32  `bson:"nodeId"`
	VHF    []byte `bson:"VHF"`
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
		lastid int64 `bson:"lastid"`
	}{}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err := source.GetShardCountColl().FindOne(ctx, filter).Decode(&result)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return GenerateZeroID(time.Now().Unix()), nil
		} else {
			env.Log.Errorf("GetShardCountProgress ERR:%s\n", err)
			return 0, err
		}
	}
	return result.lastid, nil
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
		env.Log.Errorf("SetShardCountProgress ERR:%s\n", err)
		return err
	}
	return nil
}

func ListShardCount(firstid int64, lastid int64) (map[int32]int64, error) {
	source := NewBaseSource()
	filter := bson.M{"_id": bson.M{"$gt": firstid, "$lte": lastid}}
	fields := bson.M{"_id": 1, "nodeId": 1}
	opt := options.Find().SetProjection(fields)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	cur, err := source.GetShardColl().Find(ctx, filter, opt)
	defer cur.Close(ctx)
	if err != nil {
		env.Log.Errorf("ListShardCount ERR:%s\n", err)
		return nil, err
	}
	count := make(map[int32]int64)
	for cur.Next(ctx) {
		var res = &ShardMeta{}
		err = cur.Decode(res)
		if err != nil {
			env.Log.Errorf("ListShardCount.Decode ERR:%s\n", err)
			return nil, err
		}
		num, ok := count[res.NodeId]
		if ok {
			count[res.NodeId] = num + 1
		} else {
			count[res.NodeId] = 1
		}
	}
	if curerr := cur.Err(); curerr != nil {
		env.Log.Errorf("ListShardCount ERR:%s\n", curerr)
		return nil, curerr
	}
	return count, nil
}

func ListRebuildShardCount(firstid int64, lastid int64) (map[int32]int64, map[int64]int32, error) {
	source := NewBaseSource()
	filter := bson.M{"_id": bson.M{"$gt": firstid, "$lte": lastid}}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	cur, err := source.GetShardColl().Find(ctx, filter)
	defer cur.Close(ctx)
	if err != nil {
		env.Log.Errorf("ListShardCount ERR:%s\n", err)
		return nil, nil, err
	}
	count := make(map[int32]int64)
	upmetas := make(map[int64]int32)
	for cur.Next(ctx) {
		var res = &ShardRebuidMeta{}
		err = cur.Decode(res)
		if err != nil {
			env.Log.Errorf("ListRebuildShardCount.Decode ERR:%s\n", err)
			return nil, nil, err
		}
		num, ok := count[res.NewNodeId]
		if ok {
			count[res.NewNodeId] = num + 1
		} else {
			count[res.NewNodeId] = 1
		}
		num, ok = count[res.OldNodeId]
		if ok {
			count[res.NewNodeId] = num - 1
		} else {
			count[res.NewNodeId] = -1
		}
		upmetas[res.VFI] = res.NewNodeId
	}
	if curerr := cur.Err(); curerr != nil {
		env.Log.Errorf("ListRebuildShardCount ERR:%s\n", curerr)
		return nil, nil, curerr
	}
	return count, upmetas, nil
}

func UpdateShardCount(hash map[int32]int64, firstid int64, lastid int64) error {
	f1 := fmt.Sprintf("uspaces.sn%d", env.SuperNodeID)
	f2 := fmt.Sprintf("uspaces.lstid%d", env.SuperNodeID)
	operations := []mongo.WriteModel{}
	for k, v := range hash {
		b1 := bson.M{"_id": k}
		b2 := bson.M{f2: bson.M{"$gt": firstid}}
		filter := bson.M{"$and": []bson.M{b1, b2}}
		mode := &mongo.UpdateOneModel{Filter: filter, Update: bson.M{"$inc": bson.M{f1: v}, "$set": bson.M{f2: lastid}}}
		operations = append(operations, mode)
	}
	source := NewDNIBaseSource()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	_, err := source.GetNodeColl().BulkWrite(ctx, operations)
	if err != nil {
		env.Log.Errorf("UpdateShardCount ERR:%s\n", err)
		return err
	}
	return nil
}

func UpdateShardMeta(metas map[int64]int32) error {
	source := NewBaseSource()
	operations := []mongo.WriteModel{}
	for k, v := range metas {
		filter := bson.M{"_id": k}
		update := bson.M{"$set": bson.M{"nodeId": v}}
		mode := &mongo.UpdateOneModel{Filter: filter, Update: update}
		operations = append(operations, mode)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	_, err := source.GetShardColl().BulkWrite(ctx, operations)
	if err != nil {
		env.Log.Errorf("UpdateShardMeta ERR:%s\n", err)
		return err
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
			env.Log.Errorf("SaveShardMetas ERR:%s\n", err)
			return err
		}
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
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	_, err := source.GetShardRebuildColl().InsertMany(ctx, obs)
	if err != nil {
		return err
	}
	return nil
}

func GetShardNodes(ids []int64) ([]*ShardRebuidMeta, error) {
	source := NewBaseSource()
	filter := bson.M{"_id": bson.M{"$in": ids}}
	fields := bson.M{"_id": 1, "nodeId": 1}
	opt := options.Find().SetProjection(fields)
	metas := []*ShardRebuidMeta{}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	cur, err := source.GetShardColl().Find(ctx, filter, opt)
	defer cur.Close(ctx)
	if err != nil {
		env.Log.Errorf("GetShardNodes ERR:%s\n", err)
		return nil, err
	}
	for cur.Next(ctx) {
		var res = &ShardMeta{}
		err = cur.Decode(res)
		if err != nil {
			env.Log.Errorf("GetShardNodes.Decode ERR:%s\n", err)
			return nil, err
		}
		meta := &ShardRebuidMeta{VFI: res.VFI, OldNodeId: res.NodeId}
		metas = append(metas, meta)
	}
	if curerr := cur.Err(); curerr != nil {
		env.Log.Errorf("GetShardNodes ERR:%s\n", curerr)
		return nil, curerr
	}
	if len(metas) != len(ids) {
		env.Log.Warnf("GetShardNodes return:%d reqcount:%d\n", len(metas), len(ids))
	}
	return metas, nil
}

func GetShardMetas(vbi int64, count int) ([]*ShardMeta, error) {
	source := NewBaseSource()
	metas := []*ShardMeta{}
	filter := bson.M{"_id": bson.M{"$gte": vbi, "$lt": vbi + int64(count)}}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	cur, err := source.GetShardColl().Find(ctx, filter)
	defer cur.Close(ctx)
	if err != nil {
		env.Log.Errorf("GetShardMetas ERR:%s\n", err)
		return nil, err
	}
	for cur.Next(ctx) {
		var res = &ShardMeta{}
		err = cur.Decode(res)
		if err != nil {
			env.Log.Errorf("GetShardMetas.Decode ERR:%s\n", err)
			return nil, err
		}
		metas = append(metas, res)
	}
	if curerr := cur.Err(); curerr != nil {
		env.Log.Errorf("GetShardMetas ERR:%s\n", curerr)
		return nil, curerr
	}
	if len(metas) != count {
		env.Log.Errorf("GetShardMetas return:%d reqcount:%d\n", len(metas), count)
		return nil, errors.New("")
	}
	return metas, nil
}
