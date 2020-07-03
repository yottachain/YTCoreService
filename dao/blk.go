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

const AR_DB_MODE = 0
const AR_COPY_MODE = -2
const AR_RS_MODE = -1

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
	defer cur.Close(ctx)
	if err != nil {
		logrus.Errorf("[GetBlockByVHP]ERR:%s\n", err)
		return nil, err
	}
	for cur.Next(ctx) {
		var res = &BlockMeta{}
		err = cur.Decode(res)
		if err != nil {
			logrus.Errorf("[GetBlockByVHP]Decode ERR:%s\n", err)
			return nil, err
		}
		result = append(result, res)
	}
	if err := cur.Err(); err != nil {
		logrus.Errorf("[GetBlockByVHP]Cursor ERR:%s\n", err)
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
			logrus.Errorf("[GetBlockVNF]ERR:%s\n", err)
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
			logrus.Errorf("[GetBlockById]ERR:%s\n", err)
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
			logrus.Errorf("[GetBlockByVHP_VHB]ERR:%s\n", err)
			return nil, err
		}
	}
	return result, nil
}

func SaveBlockMeta(meta *BlockMeta) error {
	source := NewBaseSource()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := source.GetBlockColl().InsertOne(ctx, meta)
	if err != nil {
		errstr := err.Error()
		if !strings.ContainsAny(errstr, "duplicate key error") {
			logrus.Errorf("[SaveBlockMeta]ERR:%s\n", err)
			return err
		}
	}
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
			logrus.Errorf("[SaveBlockData]ERR:%s\n", err)
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
			logrus.Errorf("[GetBlockData]ERR:%s\n", err)
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
			logrus.Errorf("[GetBlockCount]ERR:%s\n", err)
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
		logrus.Errorf("[IncBlockCount]ERR:%s\n", err)
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
			logrus.Errorf("[GetBlockNlinkCount]ERR:%s\n", err)
			return 0, err
		} else {
			return 0, nil
		}
	}
	return result.NLINK, nil
}

func IncBlockNlinkCount() error {
	source := NewBaseSource()
	filter := bson.M{"_id": 0}
	update := bson.M{"$inc": bson.M{"NLINK": 1}}
	opt := options.Update().SetUpsert(true)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := source.GetBlockCountColl().UpdateOne(ctx, filter, update, opt)
	if err != nil {
		logrus.Errorf("[IncBlockNlinkCount]ERR:%s\n", err)
		return err
	}
	return nil
}
