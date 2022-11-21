package backend

import (
	"errors"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/patrickmn/go-cache"
	"github.com/sirupsen/logrus"
	"github.com/yottachain/YTCoreService/api"
	"github.com/yottachain/YTCoreService/pkt"
	"github.com/yottachain/YTCoreService/s3"

	"go.mongodb.org/mongo-driver/bson/primitive"
)

var Bucket_CACHE = cache.New(5*time.Second, 5*time.Second)

func (db *YTFS) delBucket(publicKey, bucketname string) {
	backmap, err := db.listBuckets(publicKey)
	if err != nil {
		return
	}
	backmap.Delete(bucketname)
}

func (db *YTFS) getBucket(publicKey, bucketname string) (s3.BucketInfo, error) {
	backmap, err := db.listBuckets(publicKey)
	if err != nil {
		return s3.BucketInfo{}, err
	}
	if b, ok := backmap.Load(bucketname); ok {
		bu, _ := b.(s3.BucketInfo)
		return bu, nil
	} else {
		return s3.BucketInfo{}, s3.BucketNotFound(bucketname)
	}
}

func (db *YTFS) listBuckets(publicKey string) (*sync.Map, error) {
	if bs, has := Bucket_CACHE.Get(publicKey); has {
		bucks, _ := bs.(*sync.Map)
		return bucks, nil
	} else {
		c := api.GetClient(publicKey)
		if c == nil {
			return nil, s3.ResourceError(s3.ErrInvalidAccessKeyID, "YTA"+publicKey)
		}
		bucketAccessor := c.NewBucketAccessor()
		names, err1 := bucketAccessor.ListBucket()
		if err1 != nil {
			logrus.Errorf("[ListBucket]AuthSuper ERR:%s\n", err1)
			return nil, pkt.ToError(err1)
		}
		var buckmap sync.Map
		len := len(names)
		for i := 0; i < len; i++ {
			bucket := s3.BucketInfo{Name: names[i], CreationDate: s3.NewContentTime(time.Now())}
			buckmap.Store(bucket.Name, bucket)
		}
		Bucket_CACHE.SetDefault(publicKey, &buckmap)
		return &buckmap, nil
	}
}

func (db *YTFS) ListBuckets(publicKey string) ([]s3.BucketInfo, error) {
	backmap, err := db.listBuckets(publicKey)
	if err != nil {
		return nil, err
	}
	var buckets []s3.BucketInfo
	backmap.Range(func(key, value interface{}) bool {
		bucket, _ := value.(s3.BucketInfo)
		buckets = append(buckets, bucket)
		return true
	})
	return buckets, nil
}

func (me *YTFS) ListBucket(publicKey, name string, prefix *s3.Prefix, page s3.ListBucketPage) (*s3.ObjectList, error) {
	count := atomic.AddInt32(ListBucketNum, 1)
	defer atomic.AddInt32(ListBucketNum, -1)
	if count > int32(MaxListNum) {
		return nil, errors.New("listBucket request too frequently")
	}
	var response = s3.NewObjectList()
	c := api.GetClient(publicKey)
	if c == nil {
		return nil, s3.ResourceError(s3.ErrInvalidAccessKeyID, "YTA"+publicKey)
	}
	objectAccessor := c.NewObjectAccessor()
	startFile := ""
	if page.HasMarker {
		startFile = page.Marker
	}
	pfix := ""
	if prefix.HasPrefix {
		pfix = prefix.Prefix
	}
	items, err := objectAccessor.ListObject(name, startFile, pfix, false, primitive.NilObjectID, uint32(page.MaxKeys))
	if err != nil {
		return response, fmt.Errorf(err.String())
	}
	logrus.Infof("[ListObjects]Response %d items\n", len(items))
	lastFile := ""
	num := 0
	for _, v := range items {
		num++
		meta, err := api.BytesToFileMetaMap(v.Meta, primitive.ObjectID{})
		if err != nil {
			logrus.Warnf("[ListObjects]ERR meta,filename:%s\n", v.FileName)
			continue
		}
		t := time.Unix(v.FileId.Timestamp().Unix(), 0)
		meta["x-amz-meta-s3b-last-modified"] = t.Format("20060102T150405Z")
		content := GetContentByMeta(meta)
		content.Key = v.FileName
		content.Owner = &s3.UserInfo{
			ID:          c.Username,
			DisplayName: c.Username,
		}
		response.Contents = append(response.Contents, content)
		lastFile = v.FileName
	}
	if int64(num) >= page.MaxKeys {
		response.NextMarker = lastFile
		response.IsTruncated = true
	}
	return response, nil
}

func GetContentByMeta(meta map[string]string) *s3.Content {
	var content s3.Content
	content.ETag = meta["ETag"]
	if contentLengthString, ok := meta["content-length"]; ok {
		size, err := strconv.ParseInt(contentLengthString, 10, 64)
		if err == nil {
			content.Size = size
		}
	}
	if contentLengthString, ok := meta["contentLength"]; ok {
		size, err := strconv.ParseInt(contentLengthString, 10, 64)
		if err == nil {
			content.Size = size
		}
	}
	if lastModifyString, ok := meta["x-amz-meta-s3b-last-modified"]; ok {
		lastModifyTime, err := time.ParseInLocation("20060102T150405Z", lastModifyString, time.Local)
		if err == nil {
			content.LastModified = s3.ContentTime{Time: lastModifyTime}
		}
	}
	return &content
}
