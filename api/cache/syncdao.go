package cache

import (
	"github.com/boltdb/bolt"
)

type SyncObject struct {
	Sha256 []byte
}

func InsertSyncObject(sha256 []byte) error {
	return ObjectDB.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(SyncBuck)
		err := b.Put(sha256, []byte(""))
		if err != nil {
			return err
		}
		return nil
	})
}
