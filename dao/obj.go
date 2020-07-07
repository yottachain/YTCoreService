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

type ObjectMeta struct {
	VHW       []byte             `bson:"_id"`
	VNU       primitive.ObjectID `bson:"VNU"`
	NLINK     int32              `bson:"NLINK"`
	Length    uint64             `bson:"length"`
	Usedspace uint64             `bson:"usedspace"`
	BlockList [][]byte           `bson:"blocks"`
	UserId    int32              `bson:"-"`
}

func NewObjectMeta(userID int32, vhw []byte) *ObjectMeta {
	return &ObjectMeta{UserId: userID, VHW: vhw}
}

func (self *ObjectMeta) GetAndUpdateNlink() error {
	source := NewUserMetaSource(uint32(self.UserId))
	filter := bson.M{"_id": self.VHW}
	update := bson.M{"$set": bson.M{"NLINK": 1}}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err := source.GetObjectColl().FindOneAndUpdate(ctx, filter, update).Decode(self)
	if err != nil {
		logrus.Errorf("[GetAndUpdateNlink]ERR:%s\n", err)
		return err
	}
	return nil
}

func (self *ObjectMeta) IsExists() (bool, error) {
	source := NewUserMetaSource(uint32(self.UserId))
	filter := bson.M{"_id": self.VHW}
	opt := options.FindOne().SetProjection(bson.M{"NLINK": 1, "VNU": 1, "blocks": 1})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err := source.GetObjectColl().FindOne(ctx, filter, opt).Decode(self)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return false, nil
		} else {
			logrus.Errorf("[IsExists]ERR:%s\n", err)
			return false, err
		}
	}
	return true, nil
}

func (self *ObjectMeta) InsertOrUpdate() error {
	source := NewUserMetaSource(uint32(self.UserId))
	filter := bson.M{"VNU": self.VNU}
	opt := options.Update().SetUpsert(true)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := source.GetObjectColl().UpdateOne(ctx, filter, bson.M{"$set": self}, opt)
	if err != nil {
		logrus.Errorf("[InsertOrUpdate]ERR:%s\n", err)
		return err
	}
	return nil
}

func (self *ObjectMeta) INCObjectNLINK() error {
	if self.NLINK >= 255 {
		return nil
	}
	source := NewUserMetaSource(uint32(self.UserId))
	filter := bson.M{"_id": self.VHW}
	update := bson.M{"$inc": bson.M{"NLINK": 1}}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := source.GetObjectColl().UpdateOne(ctx, filter, update)
	if err != nil {
		logrus.Errorf("[INCObjectNLINK]ERR:%s\n", err)
		return err
	}
	return nil
}

func (self *ObjectMeta) GetByVHW() error {
	source := NewUserMetaSource(uint32(self.UserId))
	filter := bson.M{"_id": self.VHW}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err := source.GetObjectColl().FindOne(ctx, filter).Decode(self)
	if err != nil {
		logrus.Errorf("[GetByVHW]ERR:%s\n", err)
		return err
	}
	return nil
}

func (self *ObjectMeta) GetByVNU() error {
	source := NewUserMetaSource(uint32(self.UserId))
	filter := bson.M{"VNU": self.VNU}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err := source.GetObjectColl().FindOne(ctx, filter).Decode(self)
	if err != nil {
		logrus.Errorf("[GetByVNU]ERR:%s\n", err)
		return err
	}
	return nil
}

func AddRefer(userid uint32, VNU primitive.ObjectID, block []byte, usedSpace uint64) error {
	source := NewUserMetaSource(userid)
	filter := bson.M{"VNU": VNU}
	update := bson.M{"$inc": bson.M{"usedspace": usedSpace}, "$push": bson.M{"blocks": block}}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := source.GetObjectColl().UpdateOne(ctx, filter, update)
	if err != nil {
		logrus.Errorf("[AddRefer]ERR:%s\n", err)
		return err
	}
	return nil
}

func ListObjects(userid uint32, startVnu primitive.ObjectID, limit int) ([][]byte, primitive.ObjectID, error) {
	source := NewUserMetaSource(userid)
	filter := bson.M{"VNU": bson.M{"$gt": startVnu}}
	fields := bson.M{"VNU": 1, "blocks": 1}
	opt := options.Find().SetProjection(fields).SetSort(bson.M{"VNU": 1})
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()
	cur, err := source.GetObjectColl().Find(ctx, filter, opt)
	defer cur.Close(ctx)
	if err != nil {
		logrus.Errorf("[ListObjects]ERR:%s\n", err)
		return nil, startVnu, err
	}
	vbis := [][]byte{}
	stoptime := time.Now().Unix() - int64(env.PMS*24*60*60)
	for cur.Next(ctx) {
		var res = &ObjectMeta{}
		err = cur.Decode(res)
		if err != nil {
			logrus.Errorf("[ListObjects]Decode ERR:%s\n", err)
			return nil, startVnu, err
		}
		if res.VNU.Timestamp().Unix() > stoptime {
			startVnu = primitive.NilObjectID
			break
		}
		if len(vbis) > limit {
			break
		}
		for _, bs := range res.BlockList {
			vbis = append(vbis, bs[0:9])
		}
		startVnu = res.VNU
	}
	if curerr := cur.Err(); curerr != nil {
		logrus.Errorf("[ListObjects]Cursor ERR:%s, block count:%d\n", curerr, len(vbis))
		return nil, startVnu, curerr
	}
	return vbis, startVnu, nil
}
