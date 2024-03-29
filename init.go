package main

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"io/ioutil"
	"os"
	"strings"
	"time"

	"github.com/aurawing/eos-go/btcsuite/btcutil/base58"
	"github.com/sirupsen/logrus"
	"github.com/yottachain/YTCoreService/codec"
	"github.com/yottachain/YTCoreService/dao"
	"github.com/yottachain/YTCoreService/env"
	ytcrypto "github.com/yottachain/YTCrypto"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

var DATABASENAME string

const TABLE_NAME = "SuperNode"
const INDEX_NAME = "pubkey"

var database *mongo.Database
var collection *mongo.Collection

type SuperNode struct {
	ID      int32    `bson:"_id"`
	Nodeid  string   `bson:"nodeid"`
	Privkey string   `bson:"privkey"`
	Pubkey  string   `bson:"pubkey"`
	Addrs   []string `bson:"addrs"`
}

type JsonSuperNode struct {
	Number     int32
	ID         string
	PrivateKey string
	Addrs      []string
}

func InitSN() {
	env.InitServer()
	dao.Init()
	logrus.SetOutput(os.Stdout)
	UpdatePrivateKey()
	DATABASENAME = "yotta"
	database = dao.GetSession().Database(DATABASENAME)
	collection = database.Collection(TABLE_NAME)
	index1 := mongo.IndexModel{
		Keys:    bson.M{INDEX_NAME: 1},
		Options: options.Index().SetUnique(true).SetName(INDEX_NAME),
	}
	collection.Indexes().CreateOne(context.Background(), index1)
	logrus.Infof("Create table %s\n", TABLE_NAME)
	insertSuperNode()
	dao.Close()
	logrus.Infof("Init OK!\n")
}

func insertSuperNode() {
	path := env.YTSN_HOME + "conf/snlist.properties"
	data, err := ioutil.ReadFile(path)
	if err != nil {
		logrus.Panicf("Failed to read snlist.properties:%s\n", err)
	}
	list := []*JsonSuperNode{}
	err = json.Unmarshal(data, &list)
	if err != nil {
		logrus.Panicf("Failed to unmarshal snlist.properties:%s\n", err)
	}
	if len(list) != 1 {
		logrus.Panicf("Snlist num:%d\n", len(list))
	}
	for _, sn := range list {
		n := &SuperNode{ID: 0, Nodeid: sn.ID, Privkey: sn.PrivateKey, Addrs: sn.Addrs}
		filter := bson.M{"_id": 0}
		n.Pubkey, err = ytcrypto.GetPublicKeyByPrivateKey(sn.PrivateKey)
		if err != nil {
			logrus.Panicf("PrivateKey %s ERR:%s\n", sn.PrivateKey, err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		opt := &options.ReplaceOptions{}
		opt.SetUpsert(true)
		_, err := collection.ReplaceOne(ctx, filter, n, opt)
		if err != nil {
			logrus.Errorf("Save superNode ERR:%s\n", err)
		} else {
			logrus.Infof("Insert OK:%d\n", n.ID)
		}
	}
}

func UpdatePrivateKey() {
	confpath := env.YTSN_HOME + "conf/server.properties"
	if strings.HasPrefix(env.ShadowPriKey, "yotta:") {
		return
	}
	config, err := env.NewConfig(confpath)
	if err != nil {
		logrus.Panicf("Failed to read server.properties:%s\n", err)
	}
	key := readKey()
	bs := codec.ECBEncrypt([]byte(env.ShadowPriKey), key)
	ss := "yotta:" + base58.Encode(bs)
	err = config.SaveValue("ShadowPriKey", ss)
	if err != nil {
		logrus.Panicf("Failed to save profile:%s\n", err)
	} else {
		logrus.Infof("ShadowPriKey encrypted.\n")
	}
}

func readKey() []byte {
	path := env.YTSN_HOME + "res/key"
	data, err := ioutil.ReadFile(path)
	if err != nil {
		logrus.Panicf("Resource file 'ShadowPriKey.key' read failure\n")
	}
	sha256Digest := sha256.New()
	sha256Digest.Write(data)
	return sha256Digest.Sum(nil)
}
