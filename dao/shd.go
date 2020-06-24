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
)

type ShardMeta struct {
	VFI    int64  `bson:"_id"`
	NodeId int32  `bson:"nodeId"`
	VHF    []byte `bson:"VHF"`
}

func SaveShardMetas(ls []*ShardMeta) (bool, error) {
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
			return false, err
		} else {
			return false, nil
		}
	}
	return true, nil
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

func UpdateShardNum(ls []*ShardMeta) error {
	hash := make(map[int32]int64)
	for _, m := range ls {
		if num, ok := hash[m.NodeId]; ok {
			hash[m.NodeId] = num + 1
		} else {
			hash[m.NodeId] = 1
		}
	}
	operations := []mongo.WriteModel{}
	for k, v := range hash {
		key := fmt.Sprintf("uspaces.sn%d", env.SuperNodeID)
		mode := &mongo.UpdateOneModel{Filter: bson.M{"_id": k},
			Update: bson.M{"$inc": bson.M{key: v}}}
		operations = append(operations, mode)
	}
	source := NewDNIBaseSource()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := source.GetNodeColl().BulkWrite(ctx, operations)
	if err != nil {
		env.Log.Errorf("UpdateShardNum ERR:%s\n", err)
		return err
	}
	return nil
}
