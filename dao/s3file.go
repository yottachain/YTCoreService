package dao

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"time"

	"github.com/yottachain/YTCoreService/env"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type FileMetaWithVersion struct {
	FileId   primitive.ObjectID `bson:"_id"`
	BucketId primitive.ObjectID `bson:"bucketId"`
	FileName string             `bson:"fileName"`
	Version  []*FileVerion      `bson:"version"`
}

type FileVerion struct {
	VersionId primitive.ObjectID `bson:"versionId"`
	Meta      []byte             `bson:"meta"`
	Acl       []byte             `bson:"acl"`
}

type FileMeta struct {
	FileId    primitive.ObjectID `bson:"_id"`
	BucketId  primitive.ObjectID `bson:"bucketId"`
	FileName  string             `bson:"fileName"`
	VersionId primitive.ObjectID `bson:"version.versionId"`
	Meta      []byte             `bson:"version.meta"`
	Acl       []byte             `bson:"version.acl"`
	UserId    int32              `bson:"-"`
	Latest    bool               `bson:"-"`
}

func (self *FileMeta) GetLastFileMeta(justversion bool) error {
	source := NewUserMetaSource(uint32(self.UserId))
	filter := bson.M{"bucketId": self.BucketId, "fileName": self.FileName}
	opt := options.FindOne().SetProjection(bson.M{"_id": 1, "version.versionId": 1, "version": bson.M{"$slice": -1}})
	if !justversion {
		opt = options.FindOne().SetProjection(bson.M{"_id": 1, "version.versionId": 1, "version.meta": 1, "version": bson.M{"$slice": -1}})
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	res := &FileMetaWithVersion{}
	err := source.GetFileColl().FindOne(ctx, filter, opt).Decode(res)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return err
		} else {
			env.Log.Errorf("GetFileMeta ERR:%s\n", err)
			return err
		}
	}
	self.FileId = res.FileId
	self.VersionId = res.Version[0].VersionId
	self.Meta = res.Version[0].Meta
	self.Latest = true
	return nil
}

func (self *FileMeta) DeleteFileMeta() error {
	source := NewUserMetaSource(uint32(self.UserId))
	filter := bson.M{"bucketId": self.BucketId, "fileName": self.FileName}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := source.GetFileColl().DeleteOne(ctx, filter)
	if err != nil {
		env.Log.Errorf("UserID '%d' DeleteFileMeta ERR:%s\n", self.UserId, err)
		return err
	}
	return nil
}

func (self *FileMeta) DeleteLastFileMeta() error {
	source := NewUserMetaSource(uint32(self.UserId))
	filter := bson.M{"bucketId": self.BucketId, "fileName": self.FileName}
	update := bson.M{"$pull": bson.M{"version": bson.M{"versionId": self.VersionId}}}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := source.GetFileColl().UpdateOne(ctx, filter, update)
	if err != nil {
		env.Log.Errorf("UserID '%d' DeleteLastFileMeta ERR:%s\n", self.UserId, err)
		return err
	}
	filter = bson.M{"bucketId": self.BucketId, "fileName": self.FileName, "version": bson.M{"$size": 0}}
	ctx1, cancel1 := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel1()
	source.GetFileColl().DeleteOne(ctx1, filter)
	return nil
}

func (self *FileMeta) SaveFileMeta() error {
	source := NewUserMetaSource(uint32(self.UserId))
	filter := bson.M{"bucketId": self.BucketId, "fileName": self.FileName}
	update := bson.M{"$set": filter,
		"$addToSet": bson.M{"version": bson.M{"versionId": self.VersionId, "meta": self.Meta, "acl": self.Acl}}}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	opt := options.Update().SetUpsert(true)
	_, err := source.GetFileColl().UpdateOne(ctx, filter, update, opt)
	if err != nil {
		env.Log.Errorf("UserID '%d' SaveFileMeta ERR:%s\n", self.UserId, err)
		return err
	}
	return nil
}

func ListFileMeta(uid uint32, bid primitive.ObjectID, prefix string, nFileName string,
	nversion primitive.ObjectID, maxline int64, wversion bool) ([]*FileMetaWithVersion, error) {
	source := NewUserMetaSource(uid)
	var filter, fields bson.M
	if nFileName == "" {
		if prefix != "" {
			ss := "^" + strings.ReplaceAll(prefix, "\\", "\\\\")
			filter = bson.M{"bucketId": bid, "fileName": bson.M{"$regex": ss}}
		} else {
			filter = bson.M{"bucketId": bid}
		}
	} else {
		meta := &FileMeta{BucketId: bid, FileName: nFileName, UserId: int32(uid)}
		err := meta.GetLastFileMeta(true)
		if err != nil {
			env.Log.Errorf("ListFileMeta.GetLastFileMeta %s/%s ERR:%s\n", bid.Hex(), nFileName, err)
			return nil, err
		}
		if !wversion {
			if prefix != "" {
				ss := "^" + strings.ReplaceAll(prefix, "\\", "\\\\")
				filter = bson.M{"bucketId": bid, "fileName": bson.M{"$regex": ss}, "_id": bson.M{"$gt": meta.FileId}}
			} else {
				filter = bson.M{"bucketId": bid, "_id": bson.M{"$gt": meta.FileId}}
			}
		} else {
			if prefix != "" {
				ss := "^" + strings.ReplaceAll(prefix, "\\", "\\\\")
				filter = bson.M{"bucketId": bid, "fileName": bson.M{"$regex": ss}, "_id": bson.M{"$gte": meta.FileId}}
			} else {
				filter = bson.M{"bucketId": bid, "_id": bson.M{"$gte": meta.FileId}}
			}
		}
	}
	opt := options.Find()
	toFindNextVersionId := false
	if !wversion {
		fields = bson.M{"_id": 1, "bucketId": 1, "fileName": 1, "version.versionId": 1, "version.meta": 1, "version.acl": 1, "version": bson.M{"$slice": -1}}
		opt = opt.SetProjection(fields)
	} else {
		if nversion != primitive.NilObjectID {
			toFindNextVersionId = true
		}
	}
	limit := maxline * int64(env.LsCachePageNum)
	opt = opt.SetSort(bson.M{"_id": 1}).SetLimit(limit)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	cur, err := source.GetFileColl().Find(ctx, filter, opt)
	defer cur.Close(ctx)
	if err != nil {
		env.Log.Errorf("ListFileMeta ERR:%s\n", err)
		return nil, err
	}
	count := 0
	result := []*FileMetaWithVersion{}
	for cur.Next(ctx) {
		res := &FileMetaWithVersion{}
		err = cur.Decode(res)
		if err != nil {
			env.Log.Errorf("ListFileMeta.Decode ERR:%s\n", err)
			return nil, err
		}
		versize := len(res.Version)
		if !toFindNextVersionId {
			count = count + versize
			result = append(result, res)
			if int64(count) >= limit {
				return result, nil
			}
		} else {
			for index, ver := range res.Version {
				if toFindNextVersionId {
					if bytes.Equal(ver.VersionId[:], nversion[:]) {
						toFindNextVersionId = false
					}
				} else {
					count++
					res.Version = res.Version[index:]
					result = append(result, res)
					if int64(count) >= limit {
						return result, nil
					} else {
						break
					}
				}
			}
			if toFindNextVersionId {
				env.Log.Errorf("ListFileMeta ERR:INVALID_NEXTVERSIONID%s\n", nversion.Hex())
				return nil, errors.New("INVALID_NEXTVERSIONID")
			}
		}
	}
	if curerr := cur.Err(); curerr != nil {
		env.Log.Errorf("ListFileMeta ERR:%s\n", curerr)
		return nil, curerr
	}
	return result, nil
}
