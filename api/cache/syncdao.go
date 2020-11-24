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

func FindSyncObject(count int, isdoing func(key []byte) bool) [][]byte {
	res := [][]byte{}
	ObjectDB.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(SyncBuck)
		cur := b.Cursor()
		for k, _ := cur.First(); k != nil; k, _ = cur.Next() {
			if isdoing(k) {
				continue
			}
			res = append(res, k)
			if len(res) >= count {
				break
			}
		}
		return nil
	})
	return res
}

func DeleteSyncObject(k []byte) {
	ObjectDB.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(SyncBuck)
		b.Delete(k)
		return nil
	})
}
