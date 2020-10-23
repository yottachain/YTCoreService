package dao

import "go.mongodb.org/mongo-driver/bson/primitive"

type BucketMetaV2 struct {
	BucketId   primitive.ObjectID `bson:"_id"`
	UserId     int32              `bson:"UID"`
	BucketName string             `bson:"BName"`
	Meta       []byte             `bson:"meta"`
}
