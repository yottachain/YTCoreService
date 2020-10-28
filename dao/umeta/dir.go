package dao

import "go.mongodb.org/mongo-driver/bson/primitive"

type Directory struct {
	DirId    primitive.ObjectID `bson:"_id"`
	ParentID primitive.ObjectID `bson:"pid"`
	DirName  string             `bson:"dirname"`
}
