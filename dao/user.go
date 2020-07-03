package dao

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/mr-tron/base58"
	"github.com/patrickmn/go-cache"
	"github.com/sirupsen/logrus"
	"github.com/yottachain/YTCoreService/env"
	"github.com/yottachain/YTCoreService/net"
	"github.com/yottachain/YTCrypto"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type User struct {
	UserID       int32    `bson:"_id"`
	KUEp         [][]byte `bson:"KUEp"`
	Usedspace    uint64   `bson:"usedspace"`
	SpaceTotal   uint64   `bson:"spaceTotal"`
	FileTotal    uint64   `bson:"fileTotal"`
	Username     string   `bson:"username"`
	CostPerCycle uint64   `bson:"costPerCycle"`
	NextCycle    uint64   `bson:"nextCycle"`
	Relationship string   `bson:"relationship"`
	Routine      *int32   `bson:"-"`
}

func (user *User) GetTotalJson() string {
	usemap := &bson.M{
		"userID":     user.UserID,
		"fileTotal":  user.FileTotal,
		"spaceTotal": user.SpaceTotal,
		"usedspace":  user.Usedspace,
	}
	res, _ := json.Marshal(usemap)
	return string(res)
}

var USER_CACHE = cache.New(10*time.Minute, 10*time.Minute)

func AddUserCache(userid int32, keyNumber int, user *User) {
	key := fmt.Sprintf("%d-%d", userid, keyNumber)
	user.KUEp = [][]byte{user.KUEp[keyNumber]}
	user.Routine = new(int32)
	*user.Routine = 0
	USER_CACHE.Set(key, user, cache.DefaultExpiration)
}

func GetUserCache(userid int32, keyNumber int, signdata string) *User {
	key := fmt.Sprintf("%d-%d", userid, keyNumber)
	var user *User
	v, found := USER_CACHE.Get(key)
	if !found {
		user = GetUserByUserId(userid)
		if user == nil {
			return nil
		} else {
			user.KUEp = [][]byte{user.KUEp[keyNumber]}
			user.Routine = new(int32)
			*user.Routine = 0
			USER_CACHE.Set(key, user, cache.DefaultExpiration)
		}
	} else {
		user = v.(*User)
	}
	data := fmt.Sprintf("%d%d", userid, keyNumber)
	pkey := base58.Encode(user.KUEp[0])
	pass := YTCrypto.Verify(pkey, []byte(data), signdata)
	if !pass {
		logrus.Errorf("[GetUserCache]Signature verification failed,UserId:%d\n", userid)
		return nil
	}
	return user
}

func UpdateNilRelationship() {
	source := NewBaseSource()
	filter := bson.M{"relationship": nil}
	update := bson.M{"$set": bson.M{"relationship": ""}}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := source.GetUserColl().UpdateMany(ctx, filter, update)
	if err != nil {
		logrus.Errorf("[UpdateNilRelationship]ERR:%s\n", err)
	}
}

func GetUserByUserId(userid int32) *User {
	source := NewBaseSource()
	filter := bson.M{"_id": userid}
	var result = &User{}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err := source.GetUserColl().FindOne(ctx, filter).Decode(&result)
	if err != nil {
		if err != mongo.ErrNoDocuments {
			logrus.Errorf("[GetUserByUserId]ERR:%s\n", err)
		}
		return nil
	}
	return result
}

func GetUserByUsername(username string) *User {
	source := NewBaseSource()
	filter := bson.M{"username": username}
	var result = &User{}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err := source.GetUserColl().FindOne(ctx, filter).Decode(&result)
	if err != nil {
		if err != mongo.ErrNoDocuments {
			logrus.Errorf("[GetUserByUsername]ERR:%s\n", err)
		}
		return nil
	}
	return result
}

func AddUserKUEp(userid int32, kuep []byte) error {
	source := NewBaseSource()
	filter := bson.M{"_id": userid}
	data := bson.M{"$addToSet": bson.M{"KUEp": kuep}}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := source.GetUserColl().UpdateOne(ctx, filter, data)
	if err != nil {
		logrus.Errorf("[AddUserKUEp]UserID:%d,ERR:%s\n", userid, err)
		return err
	}
	return nil
}

func UpdateUserSpace(userid int32, usedSpace uint64, fileTotal uint64, spaceTotal uint64) error {
	source := NewBaseSource()
	filter := bson.M{"_id": userid}
	data := bson.M{"$inc": bson.M{"usedspace": usedSpace, "fileTotal": fileTotal, "spaceTotal": spaceTotal}}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := source.GetUserColl().UpdateOne(ctx, filter, data)
	if err != nil {
		logrus.Errorf("[UpdateUserSpace]UserID:%d,ERR:%s\n", userid, err)
		return err
	}
	return nil
}

func AddUser(user *User) error {
	source := NewBaseSource()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := source.GetUserColl().InsertOne(ctx, user)
	if err != nil {
		logrus.Errorf("[AddUser]UserID:%d,ERR:%s\n", user.UserID, err)
		return err
	}
	return nil
}

func ListUsers(lastId int, limit int, fields bson.M) ([]*User, error) {
	source := NewBaseSource()
	mod := bson.M{"_id": bson.M{"$mod": []interface{}{net.GetSuperNodeCount(), env.SuperNodeID}}}
	gt := bson.M{"_id": bson.M{"$gt": lastId}}
	filter := bson.M{"$and": []bson.M{mod, gt}}
	opt := options.Find().SetProjection(fields)
	opt = opt.SetSort(bson.M{"_id": 1}).SetLimit(int64(limit))
	var result = []*User{}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	cur, err := source.GetUserColl().Find(ctx, filter, opt)
	defer cur.Close(ctx)
	if err != nil {
		logrus.Errorf("[ListUsers]ERR:%s\n", err)
		return nil, err
	}
	for cur.Next(ctx) {
		var res = &User{}
		err = cur.Decode(res)
		if err != nil {
			logrus.Errorf("[ListUsers]Decode ERR:%s\n", err)
			return nil, err
		}
		result = append(result, res)
	}
	if err := cur.Err(); err != nil {
		logrus.Errorf("[ListUsers]Cursor ERR:%s\n", err)
		return nil, err
	}
	return result, nil
}

func TotalUsers() (*User, error) {
	source := NewBaseSource()
	o := bson.M{"$group": bson.M{
		"_id":        0,
		"usedspace":  bson.M{"$sum": "$usedspace"},
		"spaceTotal": bson.M{"$sum": "$spaceTotal"},
		"fileTotal":  bson.M{"$sum": "$fileTotal"},
	}}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	cur, err := source.GetUserColl().Aggregate(ctx, []bson.M{o})
	defer cur.Close(ctx)
	if err != nil {
		logrus.Errorf("[TotalUsers]ERR:%s\n", err)
		return nil, err
	}
	if cur.Next(ctx) {
		var res = &User{}
		err = cur.Decode(res)
		if err != nil {
			logrus.Errorf("[TotalUsers]Decode ERR:%s\n", err)
			return nil, err
		}
		return res, nil
	}
	if curerr := cur.Err(); curerr != nil {
		logrus.Errorf("[TotalUsers]Cursor ERR:%s\n", curerr)
		return nil, curerr
	}
	return &User{Usedspace: 0, SpaceTotal: 0, FileTotal: 0}, nil
}

func GetUserCount() (int32, error) {
	source := NewBaseSource()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	num, err := source.GetUserColl().CountDocuments(ctx, bson.M{})
	if err != nil {
		errstr := err.Error()
		if strings.ContainsAny(errstr, "document is nil") {
			return 0, nil
		} else {
			logrus.Errorf("[GetUserCount]ERR:%s\n", err)
			return 0, err
		}
	} else {
		return int32(num), nil
	}
}

func SetRelationship(username, relationship string) error {
	source := NewBaseSource()
	filter := bson.M{"username": username}
	update := bson.M{"$set": bson.M{"relationship": relationship}}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := source.GetUserColl().UpdateOne(ctx, filter, update)
	if err != nil {
		logrus.Errorf("[SetRelationship]ERR:%s\n", err)
		return err
	}
	return nil
}

func SumRelationship() (map[string]int64, error) {
	source := NewBaseSource()
	filter := bson.M{"$match": bson.M{
		"relationship": bson.M{"$ne": ""},
	}}
	o := bson.M{"$group": bson.M{
		"_id":       "$relationship",
		"usedspace": bson.M{"$sum": "$usedspace"},
	}}
	type SumType struct {
		Relationship string `bson:"_id"`
		Usedspace    int64  `bson:"usedspace"`
	}
	resmap := make(map[string]int64)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	cur, err := source.GetUserColl().Aggregate(ctx, []bson.M{filter, o})
	defer cur.Close(ctx)
	if err != nil {
		logrus.Errorf("[SumRelationship]ERR:%s\n", err)
		return nil, err
	}
	for cur.Next(ctx) {
		var res = &SumType{}
		err = cur.Decode(res)
		if err != nil {
			logrus.Errorf("[SumRelationship]Decode ERR:%s\n", err)
			return nil, err
		}
		resmap[res.Relationship] = res.Usedspace
	}
	if curerr := cur.Err(); curerr != nil {
		logrus.Errorf("[SumRelationship]Cursor ERR:%s\n", curerr)
		return nil, curerr
	}
	return resmap, nil
}

func SetSpaceSum(snid int32, mowner string, usedspace uint64) error {
	source := NewDNIBaseSource()
	filter := bson.M{"snid": snid, "mowner": mowner}
	update := bson.M{"$set": bson.M{"usedspace": usedspace}}
	opt := options.Update().SetUpsert(true)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := source.GetSumColl().UpdateOne(ctx, filter, update, opt)
	if err != nil {
		logrus.Errorf("[SetSpaceSum]ERR:%s\n", err)
		return err
	}
	return nil
}
