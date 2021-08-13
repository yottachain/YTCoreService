package dao

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/patrickmn/go-cache"
	"github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const Max_Bucket_count = 100

type BucketMeta struct {
	BucketId   primitive.ObjectID `bson:"_id"`
	BucketName string             `bson:"bucketName"`
	Meta       []byte             `bson:"meta"`
	UserId     int32              `bson:"-"`
}

var BUCKET_CACHE = cache.New(5*time.Minute, 3*time.Minute)
var BUCKET_LIST_CACHE = cache.New(5*time.Minute, 3*time.Minute)

func GetBucketIdFromCache(bname string, uid int32) (*BucketMeta, error) {
	key := fmt.Sprintf("%d-%s", uid, bname)
	v, found := BUCKET_CACHE.Get(key)
	if !found {
		meta, err := GetBucketByName(bname, uid)
		if err != nil {
			return nil, err
		} else {
			if meta == nil {
				return nil, errors.New("INVALID_BUCKET_NAME")
			} else {
				BUCKET_CACHE.SetDefault(key, meta)
				return meta, nil
			}
		}
	} else {
		return v.(*BucketMeta), nil
	}
}

func DelBucketListCache(uid int32) {
	key := strconv.Itoa(int(uid))
	BUCKET_LIST_CACHE.Delete(key)
}

func ListBucketFromCache(uid int32) ([]string, error) {
	key := strconv.Itoa(int(uid))
	v, found := BUCKET_LIST_CACHE.Get(key)
	if !found {
		logrus.Debugf("[Listbucket]UID:%d\n", uid)
		ss, err := ListBucket(uid)
		if err != nil {
			return nil, err
		} else {
			BUCKET_LIST_CACHE.SetDefault(key, ss)
			return ss, nil
		}
	} else {
		return v.([]string), nil
	}
}

func ListBucket(uid int32) ([]string, error) {
	source := NewUserMetaSource(uint32(uid))
	opt := options.Find().SetProjection(bson.M{"bucketName": 1})
	var result = []string{}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	cur, err := source.GetBucketColl().Find(ctx, bson.M{}, opt)
	defer func() {
		if cur != nil {
			cur.Close(ctx)
		}
	}()
	if err != nil {
		logrus.Errorf("[BucketMeta]ListBucket ERR:%s\n", err)
		return nil, err
	}
	for cur.Next(ctx) {
		var res = &BucketMeta{}
		err = cur.Decode(res)
		if err != nil {
			logrus.Errorf("[BucketMeta]ListBucket Decode ERR:%s\n", err)
			return nil, err
		}
		result = append(result, res.BucketName)
	}
	if err := cur.Err(); err != nil {
		logrus.Errorf("[BucketMeta]ListBucket Cursor ERR:%s\n", err)
		return nil, err
	}
	return result, nil
}

func GetBucketByName(bname string, uid int32) (*BucketMeta, error) {
	source := NewUserMetaSource(uint32(uid))
	filter := bson.M{"bucketName": bname}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	res := &BucketMeta{}
	err := source.GetBucketColl().FindOne(ctx, filter).Decode(res)
	if err != nil {
		if err != mongo.ErrNoDocuments {
			logrus.Errorf("[BucketMeta]GetBucketByName ERR:%s\n", err)
			return nil, err
		} else {
			return nil, nil
		}
	}
	res.UserId = uid
	return res, nil
}

func GetBucketCount(uid uint32) (int32, error) {
	source := NewUserMetaSource(uid)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	num, err := source.GetBucketColl().CountDocuments(ctx, nil)
	if err != nil {
		if err == mongo.ErrNilDocument {
			return 0, nil
		} else {
			logrus.Errorf("[BucketMeta]GetBucketCount ERR:%s\n", err)
			return 0, err
		}
	} else {
		return int32(num), nil
	}
}

func DeleteBucketMeta(meta *BucketMeta) error {
	source := NewUserMetaSource(uint32(meta.UserId))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	filter := bson.M{"_id": meta.BucketId}
	_, err := source.GetBucketColl().DeleteOne(ctx, filter)
	if err != nil {
		logrus.Errorf("[BucketMeta]DeleteBucketMeta ERR:%s\n", err)
		return err
	} else {
		key := fmt.Sprintf("%d-%s", meta.UserId, meta.BucketName)
		BUCKET_CACHE.Delete(key)
		return nil
	}
}

func UpdateBucketMeta(meta *BucketMeta) error {
	source := NewUserMetaSource(uint32(meta.UserId))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	filter := bson.M{"_id": meta.BucketId}
	update := bson.M{"$set": bson.M{"Meta": meta.Meta}}
	_, err := source.GetBucketColl().UpdateOne(ctx, filter, update)
	if err != nil {
		logrus.Errorf("[BucketMeta]UpdateBucketMeta UserID:%d,Name:%s,ERR:%s\n", meta.UserId, meta.BucketName, err)
		return err
	}
	key := fmt.Sprintf("%d-%s", meta.UserId, meta.BucketName)
	BUCKET_CACHE.SetDefault(key, meta)
	return nil
}

func SaveBucketMeta(meta *BucketMeta) error {
	source := NewUserMetaSource(uint32(meta.UserId))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := source.GetBucketColl().InsertOne(ctx, meta)
	if err != nil {
		errstr := err.Error()
		if !strings.ContainsAny(errstr, "duplicate key error") {
			logrus.Errorf("[BucketMeta]SaveBucketMeta UserID:%d,Name:%s,ERR:%s\n", meta.UserId, meta.BucketName, err)
			return err
		}
	}
	key := fmt.Sprintf("%d-%s", meta.UserId, meta.BucketName)
	BUCKET_CACHE.SetDefault(key, meta)
	return nil
}

func BucketIsEmpty(uid uint32, id primitive.ObjectID) (bool, error) {
	source := NewUserMetaSource(uid)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	res := &FileMetaWithVersion{}
	filter := bson.M{"bucketId": id}
	err := source.GetFileColl().FindOne(ctx, filter).Decode(res)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return true, nil
		} else {
			logrus.Errorf("[BucketMeta]BucketIsEmpty ERR:%s\n", err)
			return false, err
		}
	}
	return false, nil
}
