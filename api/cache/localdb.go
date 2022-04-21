package cache

import (
	"errors"

	"github.com/boltdb/bolt"
	"github.com/sirupsen/logrus"
	"github.com/yottachain/YTCoreService/env"
)

var cachedbname = "cache.db"
var objdbname = "object.db"
var TempBuck = []byte("tmpobject")
var SyncBuck = []byte("syncobject")
var CacheDB *bolt.DB
var ObjectDB *bolt.DB

func InitDB() error {
	path := env.GetDBCache() + cachedbname
	dbc, err := bolt.Open(path, 0600, nil)
	if err != nil {
		return err
	} else {
		CacheDB = dbc
	}
	err = CacheDB.Update(func(tx *bolt.Tx) error {
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
	path1 := env.GetDBCache() + objdbname
	db, errr := bolt.Open(path1, 0600, nil)
	if errr != nil {
		return errr
	} else {
		ObjectDB = db
	}
	err = ObjectDB.Update(func(tx *bolt.Tx) error {
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
	logrus.Infof("[Cache]LocalDB init...Path:%s\n", path)
	initCacheSize()
	return nil
}

var CurCacheSize *env.AtomInt64 = env.NewAtomInt64(0)

func initCacheSize() {
	sum := SumSpace()
	logrus.Infof("[Cache]Sum cache size %d\n", sum)
	CurCacheSize.Set(sum)
	if sum == 0 {
		Clear()
	}
}

func GetCacheSize() int64 {
	return CurCacheSize.Value()
}
