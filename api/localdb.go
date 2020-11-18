package api

import (
	"sync/atomic"

	"github.com/boltdb/bolt"
	"github.com/sirupsen/logrus"
	"github.com/yottachain/YTCoreService/env"
)

var dbname = "cache.db"
var tmpbucket = []byte("tmpobject")
var syncbucket = []byte("syncobject")

var DB *bolt.DB
var TempBuck *bolt.Bucket
var SyncBuck *bolt.Bucket

func InitDB() error {
	path := env.GetDBCache() + dbname
	dbc, err := bolt.Open(path, 0600, nil)
	if err != nil {
		return err
	} else {
		DB = dbc
	}
	err = DB.Update(func(tx *bolt.Tx) error {
		b, err1 := tx.CreateBucket(tmpbucket)
		TempBuck = b
		return err1
	})
	if err != nil {
		return err
	}
	err = DB.Update(func(tx *bolt.Tx) error {
		b, err1 := tx.CreateBucket(syncbucket)
		SyncBuck = b
		return err1
	})
	if err != nil {
		return err
	}
	logrus.Infof("LocalDB init...Path:%s\n", path)
	initCacheSize()
	return nil
}

func initCacheSize() {
	sum := SumSpace()
	logrus.Infof("Sum cache size %d\n", sum)
	atomic.StoreInt64(CurCacheSize, sum)
}
