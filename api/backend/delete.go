package backend

import (
	"github.com/sirupsen/logrus"
	"github.com/yottachain/YTCoreService/api"
	"github.com/yottachain/YTCoreService/pkt"
	"github.com/yottachain/YTCoreService/s3"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

func (db *YTFS) deleteObject(publicKey, bucketName, objectName string, c *api.Client) (result s3.ObjectDeleteResult, rerr error) {
	objectAccessor := c.NewObjectAccessor()
	err := objectAccessor.DeleteObject(bucketName, objectName, primitive.ObjectID{})
	if err != nil {
		logrus.Errorf("[S3Delete]/%s/%s,Err:%s\n", bucketName, objectName, err)
		return
	}
	return result, nil
}

func (db *YTFS) DeleteMulti(publicKey, bucketName string, objects ...string) (result s3.MultiDeleteResult, err error) {
	_, er := db.getBucket(publicKey, bucketName)
	if er != nil {
		return result, er
	}
	c := api.GetClient(publicKey)
	if c == nil {
		return result, s3.ResourceError(s3.ErrInvalidAccessKeyID, "YTA"+publicKey)
	}
	for _, object := range objects {
		dresult, err := db.deleteObject(publicKey, bucketName, object, c)
		_ = dresult
		if err != nil {
			errres := s3.ErrorResultFromError(err)
			result.Error = append(result.Error, errres)
		} else {
			result.Deleted = append(result.Deleted, s3.ObjectID{
				Key: object,
			})
		}
	}
	return result, nil
}

func (db *YTFS) DeleteObject(publicKey, bucketName, objectName string) (result s3.ObjectDeleteResult, rerr error) {
	_, er := db.getBucket(publicKey, bucketName)
	if er != nil {
		return result, er
	}
	c := api.GetClient(publicKey)
	if c == nil {
		return result, s3.ResourceError(s3.ErrInvalidAccessKeyID, "YTA"+publicKey)
	}
	return db.deleteObject(publicKey, bucketName, objectName, c)
}

func (db *YTFS) DeleteBucket(publicKey, bucketName string) error {
	_, er := db.getBucket(publicKey, bucketName)
	if er != nil {
		return er
	}
	c := api.GetClient(publicKey)
	if c == nil {
		return s3.ResourceError(s3.ErrInvalidAccessKeyID, "YTA"+publicKey)
	}
	bucketAccessor := c.NewBucketAccessor()
	err := bucketAccessor.DeleteBucket(bucketName)
	if err != nil {
		if err.Code == pkt.BUCKET_NOT_EMPTY {
			return s3.ResourceError(s3.ErrBucketNotEmpty, bucketName)
		} else if err.Code == pkt.INVALID_BUCKET_NAME {
			return s3.ResourceError(s3.ErrNoSuchBucket, bucketName)
		}
		logrus.Errorf("[S3Delete]Bucket:%s,Error msg: %s\n", bucketName, err)
	}
	db.delBucket(publicKey, bucketName)
	return nil
}
