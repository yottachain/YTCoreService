package backend

import (
	"time"

	"github.com/yottachain/YTCoreService/api"
	"github.com/yottachain/YTCoreService/pkt"
	"github.com/yottachain/YTCoreService/s3"
)

func (db *YTFS) BucketExists(publicKey, name string) (exists bool, err error) {
	backmap, err := db.listBuckets(publicKey)
	if err != nil {
		return false, err
	}
	if _, ok := backmap.Load(name); ok {
		return true, nil
	} else {
		return false, nil
	}
}

func (db *YTFS) CreateBucket(publicKey, name string) error {
	backmap, err := db.listBuckets(publicKey)
	if err != nil {
		return err
	}
	if _, ok := backmap.Load(name); ok {
		return s3.ResourceError(s3.ErrBucketAlreadyExists, name)
	}
	c := api.GetClient(publicKey)
	if c == nil {
		return s3.ResourceError(s3.ErrInvalidAccessKeyID, "YTA"+publicKey)
	}
	bucketAccessor := c.NewBucketAccessor()
	header := make(map[string]string)
	header["version_status"] = "Enabled"
	meta, err := api.BucketMetaMapToBytes(header)
	if err != nil {
		return err
	}
	err1 := bucketAccessor.CreateBucket(name, meta)
	if err1 != nil {
		return pkt.ToError(err1)
	}
	buck := s3.BucketInfo{Name: name, CreationDate: s3.NewContentTime(time.Now())}
	backmap.Store(name, buck)
	return nil
}
