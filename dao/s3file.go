package dao

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/sirupsen/logrus"
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
	FileId    primitive.ObjectID
	BucketId  primitive.ObjectID
	FileName  string
	VersionId primitive.ObjectID
	Meta      []byte
	Acl       []byte
	UserId    int32
	Latest    bool
}

func (fm *FileMeta) GetFileMeta() error {
	source := NewUserMetaSource(uint32(fm.UserId))
	var opt *options.FindOneOptions
	var filter bson.M
	if fm.VersionId == primitive.NilObjectID {
		filter = bson.M{"bucketId": fm.BucketId, "fileName": fm.FileName}
		opt = options.FindOne().SetProjection(bson.M{"_id": 1, "version.versionId": 1, "version.meta": 1, "version": bson.M{"$slice": -1}})
	} else {
		filter = bson.M{"bucketId": fm.BucketId, "fileName": fm.FileName, "version.versionId": fm.VersionId}
		opt = options.FindOne().SetProjection(bson.M{"_id": 1, "version.versionId": 1, "version.meta": 1})
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	res := &FileMetaWithVersion{}
	err := source.GetFileColl().FindOne(ctx, filter, opt).Decode(res)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return err
		} else {
			logrus.Errorf("[S3FileMeta]GetFileMeta %s/%s ERR:%s\n", fm.BucketId.Hex(), fm.FileName, err)
			return err
		}
	}
	fm.FileId = res.FileId
	if len(res.Version) == 0 {
		return mongo.ErrNoDocuments
	}
	fm.VersionId = res.Version[0].VersionId
	fm.Meta = res.Version[0].Meta
	return nil
}

func (fm *FileMeta) GetLastFileMeta(justversion bool) error {
	source := NewUserMetaSource(uint32(fm.UserId))
	filter := bson.M{"bucketId": fm.BucketId, "fileName": fm.FileName}
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
			logrus.Errorf("[S3FileMeta]GetLastFileMeta %s/%s ERR:%s\n", fm.BucketId.Hex(), fm.FileName, err)
			return err
		}
	}
	fm.FileId = res.FileId
	fm.VersionId = res.Version[0].VersionId
	fm.Meta = res.Version[0].Meta
	fm.Latest = true
	return nil
}

func (fm *FileMeta) DeleteFileMeta() (*FileMetaWithVersion, error) {
	source := NewUserMetaSource(uint32(fm.UserId))
	filter := bson.M{"bucketId": fm.BucketId, "fileName": fm.FileName}
	opt := options.FindOneAndDelete().SetProjection(bson.M{"_id": 1, "version.versionId": 1})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	res := &FileMetaWithVersion{}
	err := source.GetFileColl().FindOneAndDelete(ctx, filter, opt).Decode(res)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return nil, nil
		} else {
			logrus.Errorf("[S3FileMeta]DeleteFileMeta UserID:%d,ERR:%s\n", fm.UserId, err)
			return nil, err
		}
	}
	return res, nil
}

func (fm *FileMeta) DeleteFileMetaByVersion() (*FileMetaWithVersion, error) {
	source := NewUserMetaSource(uint32(fm.UserId))
	filter := bson.M{"bucketId": fm.BucketId, "fileName": fm.FileName}
	opt := options.FindOneAndUpdate().SetProjection(bson.M{"_id": 1, "version.versionId": 1})
	update := bson.M{"$pull": bson.M{"version": bson.M{"versionId": fm.VersionId}}}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	res := &FileMetaWithVersion{}
	err := source.GetFileColl().FindOneAndUpdate(ctx, filter, update, opt).Decode(res)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return nil, nil
		} else {
			logrus.Errorf("[S3FileMeta]DeleteFileMetaByVersion UserID:%d,ERR:%s\n", fm.UserId, err)
			return nil, err
		}
	}
	if res.Version == nil && len(res.Version) == 0 {
		fm.deleteFileMeta(source)
		return nil, nil
	} else {
		var curver *FileVerion = nil
		for _, ver := range res.Version {
			if ver.VersionId == fm.VersionId {
				curver = ver
				break
			}
		}
		if curver != nil {
			if len(res.Version) == 1 {
				fm.deleteFileMeta(source)
			}
			res.Version = []*FileVerion{curver}
			return res, nil
		} else {
			return nil, nil
		}
	}
}

func (fm *FileMeta) deleteFileMeta(source *UserMetaSource) {
	filter := bson.M{"bucketId": fm.BucketId, "fileName": fm.FileName, "version": bson.M{"$size": 0}}
	ctx1, cancel1 := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel1()
	source.GetFileColl().DeleteOne(ctx1, filter)
}

func (fm *FileMeta) SaveFileMeta() error {
	source := NewUserMetaSource(uint32(fm.UserId))
	filter := bson.M{"bucketId": fm.BucketId, "fileName": fm.FileName}
	update := bson.M{"$set": filter,
		"$addToSet": bson.M{"version": bson.M{"versionId": fm.VersionId, "meta": fm.Meta, "acl": fm.Acl}}}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	opt := options.Update().SetUpsert(true)
	_, err := source.GetFileColl().UpdateOne(ctx, filter, update, opt)
	if err != nil {
		logrus.Errorf("[S3FileMeta]SaveFileMeta UserID:%d,ERR:%s\n", fm.UserId, err)
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
	defer func() {
		if cur != nil {
			cur.Close(ctx)
		}
	}()
	if err != nil {
		logrus.Errorf("[S3FileMeta]ListFileMeta ERR:%s\n", err)
		return nil, err
	}
	count := 0
	result := []*FileMetaWithVersion{}
	for cur.Next(ctx) {
		res := &FileMetaWithVersion{}
		err = cur.Decode(res)
		if err != nil {
			logrus.Errorf("[S3FileMeta]ListFileMeta Decode ERR:%s\n", err)
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
					if ver.VersionId == nversion {
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
				logrus.Errorf("[S3FileMeta]ListFileMeta ERR:INVALID_NEXTVERSIONID%s\n", nversion.Hex())
				return nil, errors.New("INVALID_NEXTVERSIONID")
			}
		}
	}
	if curerr := cur.Err(); curerr != nil {
		logrus.Errorf("[S3FileMeta]ListFileMeta Cursor ERR:%s\n", curerr)
		return nil, curerr
	}
	return result, nil
}
