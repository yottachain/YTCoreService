package dao

import "go.mongodb.org/mongo-driver/bson/primitive"

type ObjectMeta struct {
	VHW       []byte             `bson:"_id"`
	VNU       primitive.ObjectID `bson:"VNU"`
	NLINK     int32              `bson:"NLINK"`
	Length    uint64             `bson:"length"`
	Usedspace uint64             `bson:"usedspace"`
	BlockList [][]byte           `bson:"blocks"`
	UserId    int32              `bson:"-"`
}
