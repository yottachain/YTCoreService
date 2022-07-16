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

func (om *ObjectMeta) GetAndUpdateEnd(usedSpace uint64) error {
	source := NewUserMetaSource(uint32(om.UserId))
	filter := bson.M{"_id": om.VHW}
	update := bson.M{"$set": bson.M{"NLINK": 1, "usedspace": usedSpace}}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err := source.GetObjectColl().FindOneAndUpdate(ctx, filter, update).Decode(om)
	if err != nil {
		logrus.Errorf("[ObjectMeta]GetAndUpdateNlink ERR:%s\n", err)
		return err
	}
	return nil
}

func (om *ObjectMeta) ChecekVNUExists() (bool, error) {
	source := NewUserMetaSource(uint32(om.UserId))
	filter := bson.M{"VNU": om.VNU}
	opt := options.FindOne().SetProjection(bson.M{"NLINK": 1})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err := source.GetObjectColl().FindOne(ctx, filter, opt).Decode(om)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return false, nil
		} else {
			logrus.Errorf("[ObjectMeta]ChecekVNUExists ERR:%s\n", err)
			return false, err
		}
	}
	if om.NLINK < 1 {
		return false, nil
	} else {
		return true, nil
	}
}

func (om *ObjectMeta) GetAndUpdateLink() error {
	source := NewUserMetaSource(uint32(om.UserId))
	filter := bson.M{"_id": om.VHW}
	update := bson.M{"$inc": bson.M{"NLINK": 1}}
	opt := options.FindOneAndUpdate().SetProjection(bson.M{"VNU": 1})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err := source.GetObjectColl().FindOneAndUpdate(ctx, filter, update, opt).Decode(om)
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

func (om *ObjectMeta) IsExists() (bool, error) {
	source := NewUserMetaSource(uint32(om.UserId))
	filter := bson.M{"_id": om.VHW}
	opt := options.FindOne().SetProjection(bson.M{"NLINK": 1, "VNU": 1, "length": 1, "blocks": 1})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err := source.GetObjectColl().FindOne(ctx, filter, opt).Decode(om)
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

func (om *ObjectMeta) UpdateLength() error {
	source := NewUserMetaSource(uint32(om.UserId))
	filter := bson.M{"_id": om.VHW}
	update := bson.M{"$set": bson.M{"length": om.Length}}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := source.GetObjectColl().UpdateOne(ctx, filter, update)
	if err != nil {
		logrus.Errorf("[ObjectMeta]UpdateLength ERR:%s\n", err)
		return err
	}
	return nil
}

func (om *ObjectMeta) Insert() error {
	source := NewUserMetaSource(uint32(om.UserId))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := source.GetObjectColl().InsertOne(ctx, om)
	if err != nil {
		logrus.Errorf("[ObjectMeta]Insert ERR:%s\n", err)
		return err
	}
	return nil
}

func (om *ObjectMeta) DECObjectNLINK() error {
	source := NewUserMetaSource(uint32(om.UserId))
	filter := bson.M{"VNU": om.VNU, "NLINK": bson.M{"$gt": 0}}
	update := bson.M{"$inc": bson.M{"NLINK": -1}}
	opt := options.FindOneAndUpdate().SetProjection(bson.M{"_id": 1, "usedspace": 1, "length": 1})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err := source.GetObjectColl().FindOneAndUpdate(ctx, filter, update, opt).Decode(om)
	if err != nil {
		om.Usedspace = 0
		if err == mongo.ErrNoDocuments {
			return nil
		} else {
			logrus.Errorf("[ObjectMeta]DECObjectNLINK ERR:%s\n", err)
			return err
		}
	}
	return nil
}

func (om *ObjectMeta) INCObjectNLINK() error {
	if om.NLINK >= 255 {
		return nil
	}
	source := NewUserMetaSource(uint32(om.UserId))
	filter := bson.M{"_id": om.VHW}
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

func (om *ObjectMeta) GetByVHW() error {
	source := NewUserMetaSource(uint32(om.UserId))
	filter := bson.M{"_id": om.VHW}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err := source.GetObjectColl().FindOne(ctx, filter).Decode(om)
	if err != nil {
		logrus.Errorf("[ObjectMeta]GetByVHW ERR:%s\n", err)
		return err
	}
	return nil
}

func (om *ObjectMeta) GetByVNU() error {
	source := NewUserMetaSource(uint32(om.UserId))
	filter := bson.M{"VNU": om.VNU}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err := source.GetObjectColl().FindOne(ctx, filter).Decode(om)
	if err != nil {
		logrus.Errorf("[ObjectMeta]GetByVNU ERR:%s\n", err)
		return err
	}
	return nil
}

func (om *ObjectMeta) GetAndUpdate() error {
	source := NewUserMetaSource(uint32(om.UserId))
	filter := bson.M{"VNU": om.VNU}
	update := bson.M{"$inc": bson.M{"NLINK": -1}}
	opt := options.FindOneAndUpdate().SetProjection(bson.M{"_id": 1, "NLINK": 1, "length": 1, "usedspace": 1})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err := source.GetObjectColl().FindOneAndUpdate(ctx, filter, update, opt).Decode(om)
	if err != nil {
		logrus.Errorf("[ObjectMeta]GetAndUpdate ERR:%s\n", err)
		return err
	}
	return nil
}

func AddRefer(userid uint32, VNU primitive.ObjectID, block []byte) error {
	source := NewUserMetaSource(userid)
	filter := bson.M{"VNU": VNU}
	update := bson.M{"$push": bson.M{"blocks": block}}
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
	fields := bson.M{"VNU": 1, "NLINK": 1}
	opt := options.Find().SetProjection(fields).SetSort(bson.M{"VNU": 1})
	ctx, cancel := context.WithTimeout(context.Background(), 540*time.Second)
	defer cancel()
	cur, err := source.GetObjectColl().Find(ctx, filter, opt)
	defer func() {
		if cur != nil {
			cur.Close(ctx)
		}
	}()
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
			continue
		}
		if !InArrears {
			if res.NLINK > 0 {
				continue
			}
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

func ListObjects3(userid uint32, startVnu primitive.ObjectID, limit int) (uint64, primitive.ObjectID, error) {
	source := NewUserMetaSource(userid)
	filter := bson.M{"VNU": bson.M{"$gt": startVnu}}
	fields := bson.M{"VNU": 1, "usedspace": 1}
	opt := options.Find().SetProjection(fields).SetSort(bson.M{"VNU": 1})
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()
	cur, err := source.GetObjectColl().Find(ctx, filter, opt)
	defer func() {
		if cur != nil {
			cur.Close(ctx)
		}
	}()
	if err != nil {
		logrus.Errorf("[ObjectMeta]ListObjects ERR:%s\n", err)
		return 0, startVnu, err
	}
	var usedspace uint64 = 0
	count := 0
	for cur.Next(ctx) {
		var res = &ObjectMeta{}
		err = cur.Decode(res)
		if err != nil {
			logrus.Errorf("[ObjectMeta]ListObjects Decode ERR:%s\n", err)
			continue
			//return 0, startVnu, err
		}
		if count > limit {
			break
		}
		count++
		usedspace = usedspace + res.Usedspace
		startVnu = res.VNU
	}
	if count == 0 {
		startVnu = primitive.NilObjectID
	}
	if curerr := cur.Err(); curerr != nil {
		logrus.Errorf("[ObjectMeta]ListObjects Cursor ERR:%s, file count:%d\n", count)
		return 0, startVnu, curerr
	}
	return usedspace, startVnu, nil
}

func ListObjects2(userid uint32, startVnu primitive.ObjectID, limit int) (uint64, primitive.ObjectID, error) {
	source := NewUserMetaSource(userid)
	filter := bson.M{"VNU": bson.M{"$gt": startVnu}}
	fields := bson.M{"VNU": 1, "NLINK": 1, "usedspace": 1}
	opt := options.Find().SetProjection(fields).SetSort(bson.M{"VNU": 1})
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()
	cur, err := source.GetObjectColl().Find(ctx, filter, opt)
	defer func() {
		if cur != nil {
			cur.Close(ctx)
		}
	}()
	if err != nil {
		logrus.Errorf("[ObjectMeta]ListObjects ERR:%s\n", err)
		return 0, startVnu, err
	}
	pms := env.PMS
	if env.SUM_USER_FEE > 0 {
		pms = uint64(env.SUM_USER_FEE)
	}
	stoptime := time.Now().Unix() - int64(pms*24*60*60)
	var usedspace uint64 = 0
	count := 0
	for cur.Next(ctx) {
		var res = &ObjectMeta{}
		err = cur.Decode(res)
		if err != nil {
			logrus.Errorf("[ObjectMeta]ListObjects Decode ERR:%s\n", err)
			continue
			//return 0, startVnu, err
		}
		if res.VNU.Timestamp().Unix() > stoptime {
			logrus.Infof("[ObjectMeta]Sum Stop Timestamp:%s\n", res.VNU.Timestamp().Format("2006-01-02 15:04:05"))
			startVnu = primitive.NilObjectID
			break
		}
		if res.NLINK <= 0 {
			startVnu = res.VNU
			continue
		}
		if count > limit {
			break
		}
		count++
		usedspace = usedspace + res.Usedspace
		startVnu = res.VNU
	}
	if count == 0 {
		startVnu = primitive.NilObjectID
	}
	if curerr := cur.Err(); curerr != nil {
		logrus.Errorf("[ObjectMeta]ListObjects Cursor ERR:%s, file count:%d\n", count)
		return 0, startVnu, curerr
	}
	return usedspace, startVnu, nil
}

func ListObjects(userid uint32, startVnu primitive.ObjectID, limit int) ([][]byte, primitive.ObjectID, error) {
	source := NewUserMetaSource(userid)
	filter := bson.M{"VNU": bson.M{"$gt": startVnu}}
	fields := bson.M{"VNU": 1, "NLINK": 1, "blocks": 1}
	opt := options.Find().SetProjection(fields).SetSort(bson.M{"VNU": 1})
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()
	cur, err := source.GetObjectColl().Find(ctx, filter, opt)
	defer func() {
		if cur != nil {
			cur.Close(ctx)
		}
	}()
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
			continue
			//return nil, startVnu, err
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
	if len(vbis) == 0 {
		startVnu = primitive.NilObjectID
	}
	return vbis, startVnu, nil
}
