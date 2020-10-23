package dao

import "go.mongodb.org/mongo-driver/bson/primitive"

type File struct {
	FileId   primitive.ObjectID `bson:"_id"`
	DirID    primitive.ObjectID `bson:"Did"`
	FileName string             `bson:"FName"`
}
