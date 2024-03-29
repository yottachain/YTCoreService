package dao

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/aurawing/eos-go/btcsuite/btcutil/base58"
	"github.com/patrickmn/go-cache"
	"github.com/sirupsen/logrus"
	"github.com/yottachain/YTCrypto"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type User struct {
	UserID           int32    `bson:"_id"`
	KUEp             [][]byte `bson:"KUEp"`
	Usedspace        int64    `bson:"usedspace"`
	SpaceTotal       int64    `bson:"spaceTotal"`
	FileTotal        int64    `bson:"fileTotal"`
	Username         string   `bson:"username"`
	CostPerCycle     int64    `bson:"costPerCycle"`
	NextCycle        int64    `bson:"nextCycle"`
	Relationship     string   `bson:"relationship"`
	Balance          int64    `bson:"balance"`
	Routine          *int32   `bson:"-"`
	PledgeFreeAmount float64  `bson:"pledgeFreeAmount"`
	PledgeFreeSpace  int64    `bson:"pledgeFreeSpace"`
	PledgeUpdateTime int64    `bson:"pledgeUpdateTime"`
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

var USER_CACHE = cache.New(10*time.Minute, 5*time.Minute)

func AddUserCache(userid int32, user *User) {
	key := strconv.Itoa(int(userid))
	user.Routine = new(int32)
	*user.Routine = 0
	USER_CACHE.Set(key, user, cache.DefaultExpiration)
}

func GetUserCache(userid int32, keyNumber int, signdata string) *User {
	key := strconv.Itoa(int(userid))
	var user *User
	v, found := USER_CACHE.Get(key)
	if !found {
		user = GetUserByUserId(userid)
		if user == nil {
			return nil
		} else {
			user.Routine = new(int32)
			*user.Routine = 0
			USER_CACHE.Set(key, user, cache.DefaultExpiration)
		}
	} else {
		user = v.(*User)
	}
	if keyNumber < 0 || keyNumber > len(user.KUEp)-1 {
		logrus.Errorf("[UserMeta]GetUserCache failed,keyNumber:%d,UserId:%d\n", keyNumber, userid)
		return nil
	}
	data := fmt.Sprintf("%d%d", userid, keyNumber)
	pkey := base58.Encode(user.KUEp[keyNumber])
	pass := YTCrypto.Verify(pkey, []byte(data), signdata)
	if !pass {
		logrus.Errorf("[UserMeta]GetUserCache Signature verification failed,UserId:%d\n", userid)
		return nil
	}
	return user
}

func UpdateBalance(uid int32, balance int64) {
	source := NewBaseSource()
	filter := bson.M{"_id": uid}
	update := bson.M{"$set": bson.M{"balance": balance}}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := source.GetUserColl().UpdateOne(ctx, filter, update)
	if err != nil {
		logrus.Errorf("[UserMeta]UpdateBalance ERR:%s\n", err)
	}
}

func UpdateNilRelationship() {
	source := NewBaseSource()
	filter := bson.M{"relationship": nil}
	update := bson.M{"$set": bson.M{"relationship": ""}}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := source.GetUserColl().UpdateMany(ctx, filter, update)
	if err != nil {
		logrus.Errorf("[UserMeta]UpdateNilRelationship ERR:%s\n", err)
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
			logrus.Errorf("[UserMeta]GetUserByUserId ERR:%s\n", err)
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
			logrus.Errorf("[UserMeta]GetUserByUsername ERR:%s\n", err)
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
		logrus.Errorf("[UserMeta]AddUserKUEp UserID:%d,ERR:%s\n", userid, err)
		return err
	}
	return nil
}

func UpdateUserCost(userid int32, costPerCycle uint64) error {
	source := NewBaseSource()
	filter := bson.M{"_id": userid}
	data := bson.M{"$set": bson.M{"costPerCycle": costPerCycle}}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := source.GetUserColl().UpdateOne(ctx, filter, data)
	if err != nil {
		logrus.Errorf("[UserMeta]UpdateUserCost UserID:%d,ERR:%s\n", userid, err)
		return err
	}
	return nil
}

func UpdateUserSpace(userid int32, usedSpace int64, fileTotal int64, spaceTotal int64) error {
	source := NewBaseSource()
	filter := bson.M{"_id": userid}
	data := bson.M{"$inc": bson.M{"usedspace": usedSpace, "fileTotal": fileTotal, "spaceTotal": spaceTotal}}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := source.GetUserColl().UpdateOne(ctx, filter, data)
	if err != nil {
		logrus.Errorf("[UserMeta]UpdateUserSpace UserID:%d,ERR:%s\n", userid, err)
		return err
	}
	return nil
}

func UpdateUser(user *User) error {
	source := NewBaseSource()
	filter := bson.M{"_id": user.UserID}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	opt := &options.ReplaceOptions{}
	opt.SetUpsert(true)
	_, err := source.GetUserColl().ReplaceOne(ctx, filter, user, opt)
	if err != nil {
		logrus.Errorf("[UserMeta]UpdateUser UserID:%d,ERR:%s\n", user.UserID, err)
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
		logrus.Errorf("[UserMeta]AddUser UserID:%d,ERR:%s\n", user.UserID, err)
		return err
	}
	return nil
}

func ListUsers(lastId int32, limit int, fields bson.M) ([]*User, error) {
	source := NewBaseSource()
	filter := bson.M{"_id": bson.M{"$gt": lastId}}
	opt := options.Find().SetProjection(fields)
	opt = opt.SetSort(bson.M{"_id": 1}).SetLimit(int64(limit))
	var result = []*User{}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	cur, err := source.GetUserColl().Find(ctx, filter, opt)
	defer func() {
		if cur != nil {
			cur.Close(ctx)
		}
	}()
	if err != nil {
		logrus.Errorf("[UserMeta]ListUsers ERR:%s\n", err)
		return nil, err
	}
	for cur.Next(ctx) {
		var res = &User{}
		err = cur.Decode(res)
		if err != nil {
			logrus.Errorf("[UserMeta]ListUsers Decode ERR:%s\n", err)
			return nil, err
		}
		result = append(result, res)
	}
	if err := cur.Err(); err != nil {
		logrus.Errorf("[UserMeta]ListUsers Cursor ERR:%s\n", err)
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
	defer func() {
		if cur != nil {
			cur.Close(ctx)
		}
	}()
	if err != nil {
		logrus.Errorf("[UserMeta]TotalUsers ERR:%s\n", err)
		return nil, err
	}
	if cur.Next(ctx) {
		var res = &User{}
		err = cur.Decode(res)
		if err != nil {
			logrus.Errorf("[UserMeta]TotalUsers Decode ERR:%s\n", err)
			return nil, err
		}
		return res, nil
	}
	if curerr := cur.Err(); curerr != nil {
		logrus.Errorf("[UserMeta]TotalUsers Cursor ERR:%s\n", curerr)
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
		if err == mongo.ErrNilDocument {
			return 0, nil
		} else {
			logrus.Errorf("[UserMeta]GetUserCount ERR:%s\n", err)
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
		logrus.Errorf("[UserMeta]SetRelationship ERR:%s\n", err)
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
	defer func() {
		if cur != nil {
			cur.Close(ctx)
		}
	}()
	if err != nil {
		logrus.Errorf("[UserMeta]SumRelationship ERR:%s\n", err)
		return nil, err
	}
	for cur.Next(ctx) {
		var res = &SumType{}
		err = cur.Decode(res)
		if err != nil {
			logrus.Errorf("[UserMeta]SumRelationship Decode ERR:%s\n", err)
			return nil, err
		}
		resmap[res.Relationship] = res.Usedspace
	}
	if curerr := cur.Err(); curerr != nil {
		logrus.Errorf("[UserMeta]SumRelationship Cursor ERR:%s\n", curerr)
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
		logrus.Errorf("[UserMeta]SetSpaceSum ERR:%s\n", err)
		return err
	}
	return nil
}

func UpdateUserPledgeInfo(userID int32, pledgeFreeAmount float64, pledgeFreeSpace int64) error {
	source := NewBaseSource()
	filter := bson.M{"_id": userID}
	update := bson.M{"$set": bson.M{"pledgeFreeAmount": pledgeFreeAmount, "pledgeFreeSpace": pledgeFreeSpace, "pledgeUpdateTime": time.Now().Unix()}}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := source.GetUserColl().UpdateOne(ctx, filter, update)
	if err != nil {
		logrus.Errorf("[PledgeSpace]UpdateUserPledgeInfo ERR:%s\n", err)
		return err
	}
	return nil
}
