package backend

import (
	"encoding/hex"
	"errors"
	"io"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/yottachain/YTCoreService/api"
	"github.com/yottachain/YTCoreService/pkt"
	"github.com/yottachain/YTCoreService/s3"

	"go.mongodb.org/mongo-driver/bson/primitive"
)

type bucketData struct {
	name         string
	lastModified time.Time
	versionID    s3.VersionID
	deleteMarker bool
	hash         []byte
	metadata     map[string]string
}

func (bi *bucketData) toObject(rangeRequest *s3.ObjectRangeRequest, withBody bool) (obj *s3.Object, err error) {
	szStr := bi.metadata["contentLength"]
	sz, err := strconv.ParseInt(szStr, 10, 64)
	if err != nil {
		logrus.Errorf("[Bucket]toObject err:%s\n", err)
	}
	var contents io.ReadCloser
	var rnge *s3.ObjectRange
	if withBody {
		rnge, err = rangeRequest.Range(sz)
		if err != nil {
			return nil, err
		}
	}
	return &s3.Object{
		Name:           bi.name,
		Hash:           bi.hash,
		Metadata:       bi.metadata,
		Size:           sz,
		Range:          rnge,
		IsDeleteMarker: bi.deleteMarker,
		VersionID:      bi.versionID,
		Contents:       contents,
	}, nil
}

func (db *YTFS) getObjectV2(publicKey, bucketName, objectName string, rangeRequest *s3.ObjectRangeRequest, prefix *s3.Prefix, page s3.ListBucketPage) (*s3.Object, error) {
	count := atomic.AddInt32(GetObjectNum, 1)
	defer atomic.AddInt32(GetObjectNum, -1)
	if count > int32(MaxGetObjNum) {
		return nil, errors.New("getObject request too frequently")
	}
	_, err := db.getBucket(publicKey, bucketName)
	if err != nil {
		return nil, err
	}
	c := api.GetClient(publicKey)
	if c == nil {
		return nil, s3.ResourceError(s3.ErrInvalidAccessKeyID, "YTA"+publicKey)
	}
	var metabs []byte
	var t time.Time
	download, errMsg := c.NewDownloadLastVersion(bucketName, objectName)
	if errMsg != nil {
		logrus.Errorf("[S3Download]NewDownloadLastVersion err:%s\n", errMsg)
		if errMsg.Code == pkt.INVALID_OBJECT_NAME {
			items, err := c.NewObjectAccessor().ListObject(bucketName, "", objectName, false, primitive.NilObjectID, uint32(page.MaxKeys))
			if err != nil {
				return nil, pkt.ToError(errMsg)
			}
			if len(items) > 0 {
				metabs = items[0].Meta
				t = items[0].FileId.Timestamp()
			} else {
				return nil, s3.ErrNoSuchKey
			}
		} else {
			return nil, pkt.ToError(errMsg)
		}
	} else {
		metabs = download.Meta
		t = download.GetTime()
	}
	meta, err := api.BytesToFileMetaMap(metabs, primitive.NilObjectID)
	if err != nil {
		return nil, err
	}
	meta["x-amz-meta-s3b-last-modified"] = t.Format("20060102T150405Z")
	content := GetContentByMeta(meta)
	content.Key = objectName
	content.Owner = &s3.UserInfo{
		ID:          c.Username,
		DisplayName: c.Username,
	}
	hash, _ := hex.DecodeString(meta["ETag"])
	obj := &bucketData{
		name:         objectName,
		hash:         hash,
		metadata:     meta,
		lastModified: content.LastModified.Time,
	}
	result, err := obj.toObject(rangeRequest, true)
	if err != nil {
		logrus.Errorf("[S3Download]toObject err:%s\n", err)
		return nil, err
	}
	content = GetContentByMeta(result.Metadata)
	result.Size = content.Size
	if result.Size > 0 {
		if rangeRequest != nil {
			if rangeRequest.End == -1 {
				rangeRequest.End = content.Size
				rangeRequest.FromEnd = true
			}
			result.Contents = download.LoadRange(rangeRequest.Start, rangeRequest.End)
			result.Range = &s3.ObjectRange{
				Start:  rangeRequest.Start,
				Length: rangeRequest.End - rangeRequest.Start,
			}
		} else {
			result.Contents = download.Load()
		}
	} else if result.Size == 0 {
		result.Contents = &ZeroReader{}
	}
	hash, _ = hex.DecodeString(content.ETag)
	result.Hash = hash
	return result, nil
}

type ZeroReader struct {
	io.ReadCloser
}

func (cr *ZeroReader) Close() error {
	return nil
}

func (cr *ZeroReader) Read(buf []byte) (int, error) {
	return 0, io.EOF
}

func (db *YTFS) HeadObject(publicKey, bucketName, objectName string) (*s3.Object, error) {
	return db.getObjectV2(publicKey, bucketName, objectName, nil, nil, s3.ListBucketPage{})
}

func (db *YTFS) GetObject(publicKey, bucketName, objectName string, rangeRequest *s3.ObjectRangeRequest) (*s3.Object, error) {
	return db.getObjectV2(publicKey, bucketName, objectName, rangeRequest, nil, s3.ListBucketPage{})
}
