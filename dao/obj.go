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
