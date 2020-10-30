package dao

import "go.mongodb.org/mongo-driver/bson/primitive"

type S3ObjectMeta struct {
	VNU       primitive.ObjectID `bson:"_id"`
	UserId    int32              `bson:"uid"`
	VHW       []byte             `bson:"VHW"`
	NLINK     int32              `bson:"NLINK"`
	Length    uint64             `bson:"length"`
	Usedspace uint64             `bson:"uspace"`
	BlockList [][]byte           `bson:"blocks"`
}
