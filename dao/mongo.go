package dao

import (
	"context"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"

	"github.com/sirupsen/logrus"
	"github.com/yottachain/YTCoreService/env"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

var DATABASENAME string
var USER_DATABASENAME string
var DNI_DATABASENAME string
var CACHE_DATABASENAME string

var config *env.Config
var MongoAddress string
var session *mongo.Client = nil
var mutex sync.Mutex

func InitMongo() {
	s := strings.Trim(os.Getenv("IPFS_DBNAME_SNID"), " ")
	logrus.Printf("[InitMongo]READ dev IPFS_DBNAME_SNID:%s\n", s)
	IPFS_DBNAME_SNID := strings.EqualFold(s, "yes")
	if IPFS_DBNAME_SNID {
		DATABASENAME = "metabase" + "_" + strconv.Itoa(env.SuperNodeID)
		USER_DATABASENAME = "usermeta_" + strconv.Itoa(env.SuperNodeID) + "_"
		DNI_DATABASENAME = "yotta" + strconv.Itoa(env.SuperNodeID)
		CACHE_DATABASENAME = "cache_" + strconv.Itoa(env.SuperNodeID)
	} else {
		DATABASENAME = "metabase"
		USER_DATABASENAME = "usermeta_"
		DNI_DATABASENAME = "yotta"
		CACHE_DATABASENAME = "cache"
	}
	confpath := env.YTSN_HOME + "conf/mongo.properties"
	conf, err := env.NewConfig(confpath)
	if err != nil {
		log.Panicf("[InitMongo]No properties file could be found for ytfs service:%s\n", confpath)
	}
	conf.SetSection(env.YTSN_ENV_MONGO_SEC)
	config = conf
	initclient()
}

func Close() {
	mutex.Lock()
	defer mutex.Unlock()
	if session != nil {
		session.Disconnect(context.Background())
		session = nil
	}
}

func GetSession() *mongo.Client {
	return session
}

func initclient() {
	MongoAddress = config.GetString("serverlist", "")
	if MongoAddress == "" {
		logrus.Panicf("[InitMongo]No serverlist is specified in the MongoSource.properties file.")
	}
	username := config.GetString("username", "")
	password := config.GetString("password", "")
	if username != "" {
		MongoAddress = "mongodb://" + username + ":" + password + "@" + MongoAddress
	} else {
		MongoAddress = "mongodb://" + MongoAddress
	}
	opt := options.Client().ApplyURI(MongoAddress)
	var err error
	session, err = mongo.Connect(context.Background(), opt)
	if err != nil {
		logrus.Panicln("[InitMongo]Failed to connect to Mongo server[", MongoAddress, "]")
		session = nil
		return
	}
	logrus.Infof("[InitMongo]Successful connection to Mongo server[%s]\n", MongoAddress)
	metaBaseSource = &MetaBaseSource{}
	metaBaseSource.initMetaDB()
	dniBaseSource = &DNIBaseSource{}
	dniBaseSource.initMetaDB()
	cacheBaseSource = &CacheBaseSource{}
	cacheBaseSource.initMetaDB()
	UpdateNilRelationship()
}

const USER_TABLE_NAME = "users"
const USER_INDEX_NAME = "username"
const USER_REL_INDEX_NAME = "relationship"

const BLOCK_TABLE_NAME = "blocks"
const BLOCK_INDEX_VHP_VHB = "VHP_VHB"
const BLOCK_DAT_TABLE_NAME = "blocks_data"
const BLOCK_CNT_TABLE_NAME = "block_count"

const SHARD_TABLE_NAME = "shards"
const SHARD_CNT_TABLE_NAME = "shard_count"
const SHARD_RBD_TABLE_NAME = "shards_rebuild"

type MetaBaseSource struct {
	db          *mongo.Database
	user_c      *mongo.Collection
	block_c     *mongo.Collection
	block_d_c   *mongo.Collection
	block_cnt_c *mongo.Collection
	shard_c     *mongo.Collection
	shard_cnt_c *mongo.Collection
	shard_rbd_c *mongo.Collection
}

var metaBaseSource *MetaBaseSource = nil

func NewBaseSource() *MetaBaseSource {
	return metaBaseSource
}

func (source *MetaBaseSource) initMetaDB() {
	source.db = session.Database(DATABASENAME)
	source.user_c = source.db.Collection(USER_TABLE_NAME)
	index1 := mongo.IndexModel{
		Keys:    bson.M{"username": 1},
		Options: options.Index().SetUnique(true).SetName(USER_INDEX_NAME),
	}
	source.user_c.Indexes().CreateOne(context.Background(), index1)
	index2 := mongo.IndexModel{
		Keys:    bson.M{"relationship": 1},
		Options: options.Index().SetUnique(false).SetName(USER_REL_INDEX_NAME),
	}
	source.user_c.Indexes().CreateOne(context.Background(), index2)

	source.block_c = source.db.Collection(BLOCK_TABLE_NAME)
	index3 := mongo.IndexModel{
		Keys:    bson.M{"VHP": 1, "VHB": 1},
		Options: options.Index().SetUnique(true).SetName(BLOCK_INDEX_VHP_VHB),
	}
	source.block_c.Indexes().CreateOne(context.Background(), index3)
	source.block_d_c = source.db.Collection(BLOCK_DAT_TABLE_NAME)
	source.block_cnt_c = source.db.Collection(BLOCK_CNT_TABLE_NAME)
	source.shard_c = source.db.Collection(SHARD_TABLE_NAME)
	source.shard_cnt_c = source.db.Collection(SHARD_CNT_TABLE_NAME)
	source.shard_rbd_c = source.db.Collection(SHARD_RBD_TABLE_NAME)
	logrus.Infof("[InitMongo]Create metabase tables Success.\n")
}

func (source *MetaBaseSource) GetDB() *mongo.Database {
	return source.db
}

func (source *MetaBaseSource) GetUserColl() *mongo.Collection {
	return source.user_c
}

func (source *MetaBaseSource) GetBlockColl() *mongo.Collection {
	return source.block_c
}

func (source *MetaBaseSource) GetBlockDataColl() *mongo.Collection {
	return source.block_d_c
}

func (source *MetaBaseSource) GetBlockCountColl() *mongo.Collection {
	return source.block_cnt_c
}

func (source *MetaBaseSource) GetShardColl() *mongo.Collection {
	return source.shard_c
}

func (source *MetaBaseSource) GetShardCountColl() *mongo.Collection {
	return source.shard_cnt_c
}

func (source *MetaBaseSource) GetShardRebuildColl() *mongo.Collection {
	return source.shard_rbd_c
}

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
	USERBASE_MAP.RLock()
	base := USERBASE_MAP.bases[uid]
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

var cacheBaseSource *CacheBaseSource = nil

type CacheBaseSource struct {
	db    *mongo.Database
	dni_c *mongo.Collection
	obj_c *mongo.Collection
	sum_c *mongo.Collection
}

func NewCacheBaseSource() *CacheBaseSource {
	return cacheBaseSource
}

func (source *CacheBaseSource) initMetaDB() {
	source.db = session.Database(CACHE_DATABASENAME)
	source.dni_c = source.db.Collection(DNI_CACHE_NAME)
	source.obj_c = source.db.Collection(OBJECT_NEW_TABLE_NAME)
	source.sum_c = source.db.Collection(USERSUM_CACHE_NAME)
	logrus.Infof("[InitMongo]Create cache tables Success.\n")
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

func (source *CacheBaseSource) GetOBJColl() *mongo.Collection {
	return source.obj_c
}
