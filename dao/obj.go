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
		logrus.Errorf("[ObjectMeta]GetAndUpdateNlink ERR:%s\n", err)
		return err
	}
	return nil
}

func (self *ObjectMeta) ChecekVNUExists() (bool, error) {
	source := NewUserMetaSource(uint32(self.UserId))
	filter := bson.M{"VNU": self.VNU}
	opt := options.FindOne().SetProjection(bson.M{"NLINK": 1})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err := source.GetObjectColl().FindOne(ctx, filter, opt).Decode(self)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return false, nil
		} else {
			logrus.Errorf("[ObjectMeta]ChecekVNUExists ERR:%s\n", err)
			return false, err
		}
	}
	if self.NLINK < 1 {
		return false, nil
	} else {
		return true, nil
	}
}

func (self *ObjectMeta) GetAndUpdateLink() error {
	source := NewUserMetaSource(uint32(self.UserId))
	filter := bson.M{"_id": self.VHW}
	update := bson.M{"$inc": bson.M{"NLINK": 1}}
	opt := options.FindOneAndUpdate().SetProjection(bson.M{"VNU": 1})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err := source.GetObjectColl().FindOneAndUpdate(ctx, filter, update, opt).Decode(self)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return nil
		} else {
			logrus.Errorf("[ObjectMeta]GetAndUpdateLink ERR:%s\n", err)
			return err
		}
	}
	return nil
}

func (self *ObjectMeta) IsExists() (bool, error) {
	source := NewUserMetaSource(uint32(self.UserId))
	filter := bson.M{"_id": self.VHW}
	opt := options.FindOne().SetProjection(bson.M{"NLINK": 1, "VNU": 1, "length": 1, "blocks": 1})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err := source.GetObjectColl().FindOne(ctx, filter, opt).Decode(self)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return false, nil
		} else {
			logrus.Errorf("[ObjectMeta]IsExists ERR:%s\n", err)
			return false, err
		}
	}
	return true, nil
}

func (self *ObjectMeta) UpdateLength() error {
	source := NewUserMetaSource(uint32(self.UserId))
	filter := bson.M{"_id": self.VHW}
	update := bson.M{"$set": bson.M{"length": self.Length}}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := source.GetObjectColl().UpdateOne(ctx, filter, update)
	if err != nil {
		logrus.Errorf("[ObjectMeta]UpdateLength ERR:%s\n", err)
		return err
	}
	return nil
}

func (self *ObjectMeta) Insert() error {
	source := NewUserMetaSource(uint32(self.UserId))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := source.GetObjectColl().InsertOne(ctx, self)
	if err != nil {
		logrus.Errorf("[ObjectMeta]Insert ERR:%s\n", err)
		return err
	}
	return nil
}

func (self *ObjectMeta) DECObjectNLINK() error {
	source := NewUserMetaSource(uint32(self.UserId))
	filter := bson.M{"VNU": self.VNU, "NLINK": bson.M{"$gt": 0}}
	update := bson.M{"$inc": bson.M{"NLINK": -1}}
	opt := options.FindOneAndUpdate().SetProjection(bson.M{"_id": 1, "usedspace": 1, "length": 1})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err := source.GetObjectColl().FindOneAndUpdate(ctx, filter, update, opt).Decode(self)
	if err != nil {
		self.Usedspace = 0
		if err == mongo.ErrNoDocuments {
			return nil
		} else {
			logrus.Errorf("[ObjectMeta]DECObjectNLINK ERR:%s\n", err)
			return err
		}
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
		logrus.Errorf("[ObjectMeta]INCObjectNLINK ERR:%s\n", err)
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
		logrus.Errorf("[ObjectMeta]GetByVHW ERR:%s\n", err)
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
		logrus.Errorf("[ObjectMeta]GetByVNU ERR:%s\n", err)
		return err
	}
	return nil
}

func (self *ObjectMeta) GetAndUpdate() error {
	source := NewUserMetaSource(uint32(self.UserId))
	filter := bson.M{"VNU": self.VNU}
	update := bson.M{"$inc": bson.M{"NLINK": -1}}
	opt := options.FindOneAndUpdate().SetProjection(bson.M{"_id": 1, "NLINK": 1, "length": 1, "usedspace": 1})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err := source.GetObjectColl().FindOneAndUpdate(ctx, filter, update, opt).Decode(self)
	if err != nil {
		logrus.Errorf("[ObjectMeta]GetAndUpdate ERR:%s\n", err)
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
		logrus.Errorf("[ObjectMeta]AddRefer ERR:%s\n", err)
		return err
	}
	return nil
}

func GetLastAccessTime(userid uint32) (time.Time, error) {
	source := NewUserMetaSource(userid)
	opt := options.FindOne().SetProjection(bson.M{"VNU": 1}).SetSort(bson.M{"VNU": -1})
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()
	result := &ObjectMeta{}
	err := source.GetObjectColl().FindOne(ctx, bson.M{}, opt).Decode(&result)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return time.Now(), nil
		} else {
			logrus.Errorf("[ObjectMeta]GetLastAccessTime ERR:%s\n", err)
			return time.Now(), err
		}
	}
	return result.VNU.Timestamp(), nil
}

func ListObjectsForDel(userid uint32, startVnu primitive.ObjectID, limit int, InArrears bool) ([]primitive.ObjectID, error) {
	source := NewUserMetaSource(userid)
	filter := bson.M{"VNU": bson.M{"$gt": startVnu}, "NLINK": bson.M{"$lt": 1}}
	if InArrears {
		filter = bson.M{"VNU": bson.M{"$gt": startVnu}}
	}
	fields := bson.M{"VNU": 1}
	opt := options.Find().SetProjection(fields).SetSort(bson.M{"VNU": 1})
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()
	cur, err := source.GetObjectColl().Find(ctx, filter, opt)
	defer cur.Close(ctx)
	if err != nil {
		logrus.Errorf("[ObjectMeta]ListObjectsForDel ERR:%s\n", err)
		return nil, err
	}
	VNUS := []primitive.ObjectID{}
	for cur.Next(ctx) {
		var res = &ObjectMeta{}
		err = cur.Decode(res)
		if err != nil {
			logrus.Errorf("[ObjectMeta]ListObjectsForDel Decode ERR:%s\n", err)
			return nil, err
		}
		VNUS = append(VNUS, res.VNU)
		if len(VNUS) > limit {
			break
		}
	}
	if curerr := cur.Err(); curerr != nil {
		logrus.Errorf("[ObjectMeta]ListObjectsForDel Cursor ERR:%s, block count:%d\n", curerr, len(VNUS))
		return nil, curerr
	}
	return VNUS, nil
}

func ListObjects(userid uint32, startVnu primitive.ObjectID, limit int) ([][]byte, primitive.ObjectID, error) {
	source := NewUserMetaSource(userid)
	filter := bson.M{"VNU": bson.M{"$gt": startVnu}}
	fields := bson.M{"VNU": 1, "NLINK": 1, "blocks": 1}
	opt := options.Find().SetProjection(fields).SetSort(bson.M{"VNU": 1})
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()
	cur, err := source.GetObjectColl().Find(ctx, filter, opt)
	defer cur.Close(ctx)
	if err != nil {
		logrus.Errorf("[ObjectMeta]ListObjects ERR:%s\n", err)
		return nil, startVnu, err
	}
	vbis := [][]byte{}
	pms := env.PMS
	if env.SUM_USER_FEE > 0 {
		pms = uint64(env.SUM_USER_FEE)
	}
	stoptime := time.Now().Unix() - int64(pms*24*60*60)
	for cur.Next(ctx) {
		var res = &ObjectMeta{}
		err = cur.Decode(res)
		if err != nil {
			logrus.Errorf("[ObjectMeta]ListObjects Decode ERR:%s\n", err)
			return nil, startVnu, err
		}
		if res.VNU.Timestamp().Unix() > stoptime {
			startVnu = primitive.NilObjectID
			break
		}
		if res.NLINK <= 0 {
			startVnu = res.VNU
			continue
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
		logrus.Errorf("[ObjectMeta]ListObjects Cursor ERR:%s, block count:%d\n", curerr, len(vbis))
		return nil, startVnu, curerr
	}
	return vbis, startVnu, nil
}
