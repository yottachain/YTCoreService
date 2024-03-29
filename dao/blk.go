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

type BlockMeta struct {
	VBI   int64  `bson:"_id"`
	VHP   []byte `bson:"VHP"`
	VHB   []byte `bson:"VHB"`
	KED   []byte `bson:"KED"`
	VNF   int16  `bson:"VNF"`
	NLINK int32  `bson:"NLINK"`
	AR    int16  `bson:"AR"`
}

func GetBlockByVHP(vhp []byte) ([]*BlockMeta, error) {
	source := NewBaseSource()
	var result = []*BlockMeta{}
	filter := bson.M{"VHP": vhp}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	opt := options.Find().SetProjection(bson.M{"VHB": 1, "KED": 1, "AR": 1})
	cur, err := source.GetBlockColl().Find(ctx, filter, opt)
	defer func() {
		if cur != nil {
			cur.Close(ctx)
		}
	}()
	if err != nil {
		logrus.Errorf("[BlockMeta]GetBlockByVHP ERR:%s\n", err)
		return nil, err
	}
	for cur.Next(ctx) {
		var res = &BlockMeta{}
		err = cur.Decode(res)
		if err != nil {
			logrus.Errorf("[BlockMeta]GetBlockByVHP Decode ERR:%s\n", err)
			return nil, err
		}
		result = append(result, res)
	}
	if err := cur.Err(); err != nil {
		logrus.Errorf("[BlockMeta]GetBlockByVHP Cursor ERR:%s\n", err)
		return nil, err
	}
	return result, nil
}

func GetBlockVNF(vbi int64) (*BlockMeta, error) {
	source := NewBaseSource()
	filter := bson.M{"_id": vbi}
	opt := options.FindOne().SetProjection(bson.M{"VNF": 1, "AR": 1, "VHB": 1})
	result := &BlockMeta{}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err := source.GetBlockColl().FindOne(ctx, filter, opt).Decode(&result)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return nil, nil
		} else {
			logrus.Errorf("[BlockMeta]GetBlockVNF ERR:%s\n", err)
			return nil, err
		}
	}
	return result, nil
}

func GetBlockById(vbi int64) (*BlockMeta, error) {
	source := NewBaseSource()
	filter := bson.M{"_id": vbi}
	opt := options.FindOne().SetProjection(bson.M{"_id": 1, "NLINK": 1, "VNF": 1, "AR": 1, "KED": 1})
	result := &BlockMeta{}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err := source.GetBlockColl().FindOne(ctx, filter, opt).Decode(&result)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return nil, nil
		} else {
			logrus.Errorf("[BlockMeta]GetBlockById ERR:%s\n", err)
			return nil, err
		}
	}
	return result, nil
}

func GetBlockByVHP_VHB(vhp []byte, vhb []byte) (*BlockMeta, error) {
	source := NewBaseSource()
	filter := bson.M{"VHP": vhp, "VHB": vhb}
	opt := options.FindOne().SetProjection(bson.M{"_id": 1, "NLINK": 1, "VNF": 1, "AR": 1, "KED": 1})
	result := &BlockMeta{}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err := source.GetBlockColl().FindOne(ctx, filter, opt).Decode(&result)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return nil, nil
		} else {
			logrus.Errorf("[BlockMeta]GetBlockByVHP_VHB ERR:%s\n", err)
			return nil, err
		}
	}
	return result, nil
}

func INCBlockNLINK(meta *BlockMeta) error {
	if meta.NLINK >= 0xFFFFFF {
		return nil
	}
	source := NewBaseSource()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	filter := bson.M{"_id": meta.VBI}
	update := bson.M{"$inc": bson.M{"NLINK": 1}}
	_, err := source.GetBlockColl().UpdateOne(ctx, filter, update)
	if err != nil {
		logrus.Errorf("[BlockMeta]INCBlockNLINK ERR:%s\n", err)
		return err
	}
	IncBlockNlinkCount(1)
	return nil
}

func SaveBlockMeta(meta *BlockMeta) error {
	source := NewBaseSource()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := source.GetBlockColl().InsertOne(ctx, meta)
	if err != nil {
		errstr := err.Error()
		if !strings.ContainsAny(errstr, "duplicate key error") {
			logrus.Errorf("[BlockMeta]SaveBlockMeta ERR:%s\n", err)
			return err
		}
	}
	IncBlockCount()
	return nil
}

func SaveBlockData(id int64, data []byte) error {
	source := NewBaseSource()
	var result = struct {
		ID   int64  `bson:"_id"`
		Data []byte `bson:"Data"`
	}{ID: id, Data: data}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := source.GetBlockDataColl().InsertOne(ctx, result)
	if err != nil {
		errstr := err.Error()
		if !strings.ContainsAny(errstr, "duplicate key error") {
			logrus.Errorf("[BlockMeta]SaveBlockData ERR:%s\n", err)
			return err
		}
	}
	return nil
}

func GetBlockData(id int64) []byte {
	source := NewBaseSource()
	filter := bson.M{"_id": id}
	var result = struct {
		Data []byte `bson:"Data"`
	}{}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err := source.GetBlockDataColl().FindOne(ctx, filter).Decode(&result)
	if err != nil {
		if err != mongo.ErrNoDocuments {
			logrus.Errorf("[BlockMeta]GetBlockData ERR:%s\n", err)
		}
		return nil
	}
	return result.Data
}

func GetBlockCount() (uint64, error) {
	source := NewBaseSource()
	var result = struct{ NLINK uint64 }{}
	filter := bson.M{"_id": 1}
	opt := options.FindOne().SetProjection(bson.M{"NLINK": 1})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err := source.GetBlockCountColl().FindOne(ctx, filter, opt).Decode(&result)
	if err != nil {
		if err != mongo.ErrNoDocuments {
			logrus.Errorf("[BlockMeta]GetBlockCount ERR:%s\n", err)
			return 0, err
		} else {
			return 0, nil
		}
	}
	return result.NLINK, nil
}

func IncBlockCount() error {
	source := NewBaseSource()
	filter := bson.M{"_id": 1}
	update := bson.M{"$inc": bson.M{"NLINK": 1}}
	opt := options.Update().SetUpsert(true)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := source.GetBlockCountColl().UpdateOne(ctx, filter, update, opt)
	if err != nil {
		logrus.Errorf("[BlockMeta]IncBlockCount ERR:%s\n", err)
		return err
	}
	return nil
}

func GetBlockNlinkCount() (uint64, error) {
	source := NewBaseSource()
	var result = struct{ NLINK uint64 }{}
	filter := bson.M{"_id": 0}
	opt := options.FindOne().SetProjection(bson.M{"NLINK": 1})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err := source.GetBlockCountColl().FindOne(ctx, filter, opt).Decode(&result)
	if err != nil {
		if err != mongo.ErrNoDocuments {
			logrus.Errorf("[BlockMeta]GetBlockNlinkCount ERR:%s\n", err)
			return 0, err
		} else {
			return 0, nil
		}
	}
	return result.NLINK, nil
}

func IncBlockNlinkCount(inc int) error {
	source := NewBaseSource()
	filter := bson.M{"_id": 0}
	update := bson.M{"$inc": bson.M{"NLINK": inc}}
	opt := options.Update().SetUpsert(true)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := source.GetBlockCountColl().UpdateOne(ctx, filter, update, opt)
	if err != nil {
		logrus.Errorf("[BlockMeta]IncBlockNlinkCount ERR:%s\n", err)
		return err
	}
	return nil
}

func AddLinks(ids []int64) error {
	source := NewBaseSource()
	filter := bson.M{"_id": bson.M{"$in": ids}}
	update := bson.M{"$inc": bson.M{"NLINK": 1}}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	_, err := source.GetBlockColl().UpdateMany(ctx, filter, update)
	if err != nil {
		logrus.Errorf("[BlockMeta]AddLinks ERR:%s\n", err)
	}
	IncBlockNlinkCount(len(ids))
	return nil
}

func GetUsedSpace(ids []int64) (map[int64]*BlockMeta, error) {
	source := NewBaseSource()
	filter := bson.M{"_id": bson.M{"$in": ids}}
	fields := bson.M{"_id": 1, "VNF": 1, "AR": 1, "NLINK": 1}
	opt := options.Find().SetProjection(fields)
	metas := make(map[int64]*BlockMeta)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	cur, err := source.GetBlockColl().Find(ctx, filter, opt)
	defer func() {
		if cur != nil {
			cur.Close(ctx)
		}
	}()
	if err != nil {
		logrus.Errorf("[BlockMeta]GetUsedSpace ERR:%s\n", err)
		return nil, err
	}
	for cur.Next(ctx) {
		var res = &BlockMeta{}
		err = cur.Decode(res)
		if err != nil {
			logrus.Errorf("[BlockMeta]GetUsedSpace Decode ERR:%s\n", err)
			return nil, err
		}
		metas[res.VBI] = res
	}
	if curerr := cur.Err(); curerr != nil {
		logrus.Errorf("[BlockMeta]GetUsedSpace Cursor ERR:%s\n", curerr)
		return nil, curerr
	}
	return metas, nil
}

func SaveShardBakup(id, shardid int64, dnid int32) error {
	source := NewBaseSource()
	var result = struct {
		ID  int64 `bson:"_id"`
		VBI int64 `bson:"VBI"`
		NID int32 `bson:"NID"`
	}{ID: id, VBI: shardid, NID: dnid}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := source.GetBlockBakColl().InsertOne(ctx, result)
	if err != nil {
		errstr := err.Error()
		if !strings.ContainsAny(errstr, "duplicate key error") {
			logrus.Errorf("[BlockMeta]SaveBlockBakup ERR:%s\n", err)
			return err
		}
	}
	return nil
}

func SaveBlockBakup(id, bid int64) error {
	source := NewBaseSource()
	var result = struct {
		ID  int64 `bson:"_id"`
		VBI int64 `bson:"VBI"`
	}{ID: id, VBI: bid}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := source.GetBlockBakColl().InsertOne(ctx, result)
	if err != nil {
		errstr := err.Error()
		if !strings.ContainsAny(errstr, "duplicate key error") {
			logrus.Errorf("[BlockMeta]SaveBlockBakup ERR:%s\n", err)
			return err
		}
	}
	return nil
}
