package dao

import "go.mongodb.org/mongo-driver/bson/primitive"

type S3File struct {
	FileId   primitive.ObjectID `bson:"_id"`
	DirID    primitive.ObjectID `bson:"dirid"`
	FileName string             `bson:"filename"`
	Version  []*S3FileVerion    `bson:"vers"`
}

type S3FileVerion struct {
	VersionId primitive.ObjectID `bson:"verid"`
	Meta      []byte             `bson:"meta"`
	Acl       []byte             `bson:"acl"`
}
