package dao

import (
	"context"
	"strconv"
	"sync"

	"github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const BUCKET_TABLE_NAME = "buckets"
const BUCKET_INDEX_NAME = "BUKNAME"

const FILE_TABLE_NAME = "files"
const FILE_INDEX_NAME = "BID_NAME"

const OBJECT_TABLE_NAME = "objects"
const OBJECT_INDEX_NAME = "VNU"

var USERBASE_MAP = struct {
	sync.RWMutex
	bases map[uint32]*UserMetaSource
}{bases: make(map[uint32]*UserMetaSource)}

type UserMetaSource struct {
	db       *mongo.Database
	userid   uint32
	bucket_c *mongo.Collection
	file_c   *mongo.Collection
	object_c *mongo.Collection
}

func NewUserMetaSource(uid uint32) *UserMetaSource {
	var base *UserMetaSource
	USERBASE_MAP.RLock()
	base = USERBASE_MAP.bases[uid]
	USERBASE_MAP.RUnlock()
	if base == nil {
		USERBASE_MAP.Lock()
		base = USERBASE_MAP.bases[uid]
		if base == nil {
			base = &UserMetaSource{}
			base.userid = uid
			base.initMetaDB()
			USERBASE_MAP.bases[uid] = base
		}
		USERBASE_MAP.Unlock()
	}
	return base
}

func (source *UserMetaSource) initMetaDB() {
	source.db = session.Database(USER_DATABASENAME + strconv.FormatUint(uint64(source.userid), 10))
	source.bucket_c = source.db.Collection(BUCKET_TABLE_NAME)
	index1 := mongo.IndexModel{
		Keys:    bson.M{"bucketName": 1},
		Options: options.Index().SetUnique(true).SetName(BUCKET_INDEX_NAME),
	}
	source.bucket_c.Indexes().CreateOne(context.Background(), index1)
	source.file_c = source.db.Collection(FILE_TABLE_NAME)
	index2 := mongo.IndexModel{
		Keys:    bson.M{"bucketId": 1, "fileName": 1},
		Options: options.Index().SetUnique(true).SetName(FILE_INDEX_NAME),
	}
	source.file_c.Indexes().CreateOne(context.Background(), index2)
	source.object_c = source.db.Collection(OBJECT_TABLE_NAME)
	index3 := mongo.IndexModel{
		Keys:    bson.M{"VNU": 1},
		Options: options.Index().SetUnique(true).SetName(OBJECT_INDEX_NAME),
	}
	source.object_c.Indexes().CreateOne(context.Background(), index3)
	logrus.Infof("[InitMongo]Create usermeta %d tables Success.\n", source.userid)
}

func (source *UserMetaSource) GetDB() *mongo.Database {
	return source.db
}

func (source *UserMetaSource) GetBucketColl() *mongo.Collection {
	return source.bucket_c
}

func (source *UserMetaSource) GetFileColl() *mongo.Collection {
	return source.file_c
}

func (source *UserMetaSource) GetObjectColl() *mongo.Collection {
	return source.object_c
}
