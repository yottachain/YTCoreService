package dao

import "go.mongodb.org/mongo-driver/bson/primitive"

type S3BucketMeta struct {
	BucketId   primitive.ObjectID `bson:"_id"`
	UserId     int32              `bson:"uid"`
	BucketName string             `bson:"buckname"`
	Meta       []byte             `bson:"meta"`
}
