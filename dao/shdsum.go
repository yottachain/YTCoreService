package dao

import (
	"bytes"
	"context"
	"encoding/binary"
	"strings"
	"time"

	"github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func ToBytes(nodes map[int32]int16) []byte {
	bs := bytes.NewBuffer([]byte{})
	size := len(nodes)
	binary.Write(bs, binary.BigEndian, int32(size))
	for k, v := range nodes {
		binary.Write(bs, binary.BigEndian, k)
		binary.Write(bs, binary.BigEndian, v)
	}
	return bs.Bytes()
}

func ToMap(bs []byte) map[int32]int16 {
	buf := bytes.NewBuffer(bs)
	m := make(map[int32]int16)
	var size int32
	binary.Read(buf, binary.BigEndian, &size)
	for ii := 0; ii < int(size); ii++ {
		var k int32
		var v int16
		binary.Read(buf, binary.BigEndian, &k)
		binary.Read(buf, binary.BigEndian, &v)
		m[k] = v
	}
	return m
}

func MergeMap(from map[int32]int16, to map[int32]int64) {
	for k, v := range from {
		num, ok := to[k]
		if ok {
			to[k] = num + int64(v)
		} else {
			to[k] = int64(v)
		}
	}
}

func NextTime(firstid int64) int64 {
	id := firstid >> 32
	s1 := time.Unix(id, 0).Format("2006010215")
	s2 := time.Now().Format("2006010215")
	if s1 != s2 {
		id = (id/3600 + 1) * 3600
		return GenerateZeroID(id)
	} else {
		return firstid
	}
}

type result struct {
	ID   int64  `bson:"_id"`
	Data []byte `bson:"Data"`
}

const LS_MAXSIZE = 50000

func ListNodeShardCount(firstid int64, lastid int64) (map[int32]int64, int64, bool, error) {
	source := NewCacheBaseSource()
	filter := bson.M{"_id": bson.M{"$gt": firstid}}
	opt := options.Find().SetSort(bson.M{"_id": 1})
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()
	cur, err := source.GetShardUploadColl(firstid).Find(ctx, filter, opt)
	defer cur.Close(ctx)
	if err != nil {
		logrus.Errorf("[ShardMeta]ListNodeShardCount ERR:%s\n", err)
		return nil, 0, false, err
	}
	ii := 0
	count := make(map[int32]int64)
	var lid int64 = firstid
	var hasnext bool = false
	for {
		hasnext = cur.Next(ctx)
		if !hasnext {
			lid = NextTime(lid)
			break
		}
		var res = &result{}
		err = cur.Decode(res)
		if err != nil {
			logrus.Errorf("[ShardMeta]ListNodeShardCount Decode ERR:%s\n", err)
			return nil, 0, false, err
		}
		ii++
		if res.ID > lastid {
			hasnext = false
			break
		}
		m := ToMap(res.Data)
		MergeMap(m, count)
		lid = res.ID
		if len(count) >= LS_MAXSIZE || ii >= LS_MAXSIZE {
			break
		}
	}
	if curerr := cur.Err(); curerr != nil {
		logrus.Errorf("[ShardMeta]ListNodeShardCount Cursor ERR:%s, at line :%d\n", curerr, ii)
		return nil, 0, false, curerr
	}
	return count, lid, hasnext, nil
}

func SaveNodeShardCount(vbi int64, bs []byte) error {
	source := NewCacheBaseSource()
	res := &result{ID: vbi, Data: bs}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := source.GetShardUploadColl(vbi).InsertOne(ctx, res)
	if err != nil {
		errstr := err.Error()
		if !strings.ContainsAny(errstr, "duplicate key error") {
			logrus.Errorf("[ShardMeta]SaveShardCounts ERR:%s\n", err)
			return err
		}
	}
	return nil
}

func DropNodeShardColl(vbi int64) {
	source := NewCacheBaseSource()
	source.DropShardUploadColl(vbi)
}
