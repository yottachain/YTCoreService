package cache

import (
	"sync"

	"github.com/aurawing/eos-go/btcsuite/btcutil/base58"
	"github.com/boltdb/bolt"
)

type SyncObject struct {
	Sha256 []byte
}

func SyncObjectExists(sha256 []byte) bool {
	var val []byte
	ObjectDB.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(SyncBuck)
		val = b.Get(sha256)
		return nil
	})
	if val == nil {
		return false
	}
	return true
}

var SyncList sync.Map

func AddSyncList(sha256 []byte) *sync.Cond {
	cond := sync.NewCond(new(sync.Mutex))
	for {
		c, ok := SyncList.LoadOrStore(string(sha256), cond)
		if ok {
			cc := c.(*sync.Cond)
			cc.L.Lock()
			cc.Wait()
			cc.L.Unlock()
			continue
		} else {
			return cond
		}
	}
}

func DelSyncList(sha256 []byte, c *sync.Cond) {
	SyncList.Delete(string(sha256))
	c.Broadcast()
}

func InsertSyncObject(sha256 []byte) error {
	return ObjectDB.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(SyncBuck)
		err := b.Put(sha256, []byte("1"))
		if err != nil {
			return err
		}
		return nil
	})
}

func FindSyncObject(count int, isdoing func(key string) bool) []string {
	res := []string{}
	ObjectDB.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(SyncBuck)
		cur := b.Cursor()
		for k, _ := cur.First(); k != nil; k, _ = cur.Next() {
			ss := base58.Encode(k)
			if isdoing(ss) {
				continue
			}
			res = append(res, ss)
			if len(res) >= count {
				break
			}
		}
		return nil
	})
	return res
}

func DeleteSyncObject(k []byte) {
	if k == nil {
		return
	}
	ObjectDB.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(SyncBuck)
		b.Delete(k)
		return nil
	})
}
