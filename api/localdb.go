package api

import (
	"errors"
	"sync/atomic"

	"github.com/boltdb/bolt"
	"github.com/sirupsen/logrus"
	"github.com/yottachain/YTCoreService/env"
)

var dbname = "cache.db"
var TempBuck = []byte("tmpobject")
var SyncBuck = []byte("syncobject")
var DB *bolt.DB

func InitDB() error {
	path := env.GetDBCache() + dbname
	dbc, err := bolt.Open(path, 0600, nil)
	if err != nil {
		return err
	} else {
		DB = dbc
	}
	err = DB.Update(func(tx *bolt.Tx) error {
		b, err1 := tx.CreateBucket(TempBuck)
		if err1 != nil {
			b = tx.Bucket(TempBuck)
			if b == nil {
				err1 = errors.New("CreateBucket err.")
			} else {
				err1 = nil
			}
		}
		return err1
	})
	if err != nil {
		return err
	}
	err = DB.Update(func(tx *bolt.Tx) error {
		b, err1 := tx.CreateBucket(SyncBuck)
		if err1 != nil {
			b = tx.Bucket(SyncBuck)
			if b == nil {
				err1 = errors.New("CreateBucket err.")
			} else {
				err1 = nil
			}
		}
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
