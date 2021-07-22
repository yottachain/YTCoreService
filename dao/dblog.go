package dao

import "go.mongodb.org/mongo-driver/bson/primitive"

type DBLog struct {
	Id     primitive.ObjectID `bson:"_id"`
	Coll   string             `bson:"coll"`
	Type   int8               `bson:"type"`
	filter string             `bson:"filter"`
	update string             `bson:"filter"`
}
