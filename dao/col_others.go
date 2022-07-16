package dao

import (
	"context"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const DNI_TABLE_NAME = "Shards"
const SPACESUN_TABLE_NAME = "SpaceSum"
const INDEX_SNID_RELATIONSHIP = "SNID_RELATIONSHIP"
const NODE_TABLE_NAME = "Node"

var dniBaseSource *DNIBaseSource = nil

type DNIBaseSource struct {
	db     *mongo.Database
	sum_c  *mongo.Collection
	dni_c  *mongo.Collection
	node_c *mongo.Collection
}

func NewDNIBaseSource() *DNIBaseSource {
	return dniBaseSource
}

func (source *DNIBaseSource) initMetaDB() {
	source.db = session.Database(DNI_DATABASENAME)
	source.sum_c = source.db.Collection(SPACESUN_TABLE_NAME)
	index := mongo.IndexModel{
		Keys:    bson.M{"snid": 1, "mowner": 1},
		Options: options.Index().SetUnique(true).SetName(INDEX_SNID_RELATIONSHIP),
	}
	source.sum_c.Indexes().CreateOne(context.Background(), index)
	source.dni_c = source.db.Collection(DNI_TABLE_NAME)
	source.node_c = source.db.Collection(NODE_TABLE_NAME)
	logrus.Infof("[InitMongo]Create dni tables Success.\n")
}

func (source *DNIBaseSource) GetDB() *mongo.Database {
	return source.db
}

func (source *DNIBaseSource) GetNodeColl() *mongo.Collection {
	return source.node_c
}

func (source *DNIBaseSource) GetSumColl() *mongo.Collection {
	return source.sum_c
}

func (source *DNIBaseSource) GetDNIColl() *mongo.Collection {
	return source.dni_c
}

const DNI_CACHE_NAME = "dnis"
const OBJECT_NEW_TABLE_NAME = "objects_new"
const USERSUM_CACHE_NAME = "userfeesum"
const OBJECT_DEL_TABLE_NAME = "objects_del"

const SHARD_UP_TABLE_NAME = "shards_upload"

var cacheBaseSource *CacheBaseSource = nil

type CacheBaseSource struct {
	db         *mongo.Database
	dni_c      *mongo.Collection
	obj_c      *mongo.Collection
	del_c      *mongo.Collection
	sum_c      *mongo.Collection
	shard_up_c sync.Map
}

func NewCacheBaseSource() *CacheBaseSource {
	return cacheBaseSource
}

func (source *CacheBaseSource) initMetaDB() {
	source.db = session.Database(CACHE_DATABASENAME)
	source.dni_c = source.db.Collection(DNI_CACHE_NAME)
	source.obj_c = source.db.Collection(OBJECT_NEW_TABLE_NAME)
	source.del_c = source.db.Collection(OBJECT_DEL_TABLE_NAME)
	source.sum_c = source.db.Collection(USERSUM_CACHE_NAME)
	logrus.Infof("[InitMongo]Create cache tables Success.\n")
}

func (source *CacheBaseSource) DropShardUploadColl(vbi int64) {
	ss := SHARD_UP_TABLE_NAME + time.Unix(vbi>>32, 0).Format("2006010215")
	v, ok := source.shard_up_c.Load(ss)
	if ok {
		c := v.(*mongo.Collection)
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		c.Drop(ctx)
		source.shard_up_c.Delete(ss)
	}
	logrus.Infof("[DropNodeShardColl]%s.\n", ss)
}

func (source *CacheBaseSource) GetShardUploadColl(vbi int64) *mongo.Collection {
	ss := SHARD_UP_TABLE_NAME + time.Unix(vbi>>32, 0).Format("2006010215")
	v, ok := source.shard_up_c.Load(ss)
	if ok {
		return v.(*mongo.Collection)
	} else {
		c := source.db.Collection(ss)
		source.shard_up_c.Store(ss, c)
		return c
	}
}

func (source *CacheBaseSource) GetDB() *mongo.Database {
	return source.db
}

func (source *CacheBaseSource) GetSumColl() *mongo.Collection {
	return source.sum_c
}

func (source *CacheBaseSource) GetDNIColl() *mongo.Collection {
	return source.dni_c
}

func (source *CacheBaseSource) GetDELColl() *mongo.Collection {
	return source.obj_c
}

func (source *CacheBaseSource) GetOBJColl() *mongo.Collection {
	return source.obj_c
}
