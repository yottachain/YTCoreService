package dao

import (
	"context"
	"log"
	"sync"

	"github.com/sirupsen/logrus"
	"github.com/yottachain/YTCoreService/env"
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

func Init() {
	USER_DATABASENAME = "usermeta_"
	CACHE_DATABASENAME = "cache"
	DATABASENAME = "metabase"
	DNI_DATABASENAME = "yotta"
	confpath := env.YTSN_HOME + "conf/mongo.properties"
	conf, err := env.NewConfig(confpath)
	if err != nil {
		log.Panicf("[InitMongo]No properties file could be found for ytfs service:%s\n", confpath)
	}
	conf.SetSection(env.YTSN_ENV_MONGO_SEC)
	config = conf
	initclient()
	initSequence()
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
