package http

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"sync/atomic"
	"time"

	"github.com/patrickmn/go-cache"
	"github.com/sirupsen/logrus"
	"github.com/yottachain/YTCoreService/dao"
	"github.com/yottachain/YTCoreService/env"
	"go.mongodb.org/mongo-driver/bson"
)

var TotalCache = struct {
	Value     atomic.Value
	LastTimes *int64
}{LastTimes: new(int64)}

func TotalHandle(w http.ResponseWriter, req *http.Request) {
	b := checkRoutine()
	defer atomic.AddInt32(RoutineConter, -1)
	if !b {
		WriteErr(w, "HTTP_ROUTINE:Too many routines")
		return
	}
	if !checkIp(req.RemoteAddr) {
		WriteErr(w, fmt.Sprintf("Invalid IP:%s", req.RemoteAddr))
		return
	}
	if time.Now().Unix()-atomic.LoadInt64(TotalCache.LastTimes) < CacheExpiredTime {
		v := TotalCache.Value.Load()
		if v != nil {
			ss, _ := v.(string)
			WriteJson(w, ss)
			return
		}
	}
	userTotal, err := dao.GetUserCount()
	if err != nil {
		WriteErr(w, "TotalHandle err:"+err.Error())
		return
	}
	m, err := dao.TotalUsers()
	if err != nil {
		WriteErr(w, "TotalHandle err:"+err.Error())
		return
	}
	usedspace := uint64(m.Usedspace)
	spaceTotal := uint64(m.SpaceTotal)
	fileTotal := uint64(m.FileTotal)
	blkCount, err := dao.GetBlockCount()
	if err != nil {
		WriteErr(w, "TotalHandle err:"+err.Error())
		return
	}
	blk_LinkCount, err := dao.GetBlockNlinkCount()
	if err != nil {
		WriteErr(w, "TotalHandle err:"+err.Error())
		return
	}
	blk_LinkCount = blk_LinkCount + blkCount
	resmap := make(map[string]uint64)
	resmap["userTotal"] = uint64(userTotal)
	resmap["fileTotal"] = fileTotal
	resmap["spaceTotal"] = spaceTotal
	resmap["usedspace"] = usedspace
	resmap["bkTotal"] = blkCount
	resmap["actualBkTotal"] = blk_LinkCount
	res, _ := json.Marshal(resmap)
	ss := string(res)
	TotalCache.Value.Store(ss)
	atomic.StoreInt64(TotalCache.LastTimes, time.Now().Unix())
	WriteJson(w, ss)
}

func RelationshipHandle(w http.ResponseWriter, req *http.Request) {
	b := checkRoutine()
	defer atomic.AddInt32(RoutineConter, -1)
	if !b {
		WriteErr(w, "HTTP_ROUTINE:Too many routines")
		return
	}
	if !checkIp(req.RemoteAddr) {
		WriteErr(w, fmt.Sprintf("Invalid IP:%s", req.RemoteAddr))
		return
	}
	var username string = ""
	var mPoolOwner string = ""
	queryForm, err := url.ParseQuery(req.URL.RawQuery)
	if err == nil && len(queryForm["username"]) > 0 && len(queryForm["mPoolOwner"]) > 0 {
		username = queryForm["username"][0]
		mPoolOwner = queryForm["mPoolOwner"][0]
	}
	if username == "" || mPoolOwner == "" {
		WriteErr(w, "Paramter 'mPoolOwner' or 'username' is NULL")
		return
	}
	err = dao.SetRelationship(username, mPoolOwner)
	if err != nil {
		WriteErr(w, "RelationshipHandle err:"+err.Error())
		return
	}
	WriteText(w, "OK")
}

func UserTotalHandle(w http.ResponseWriter, req *http.Request) {
	b := checkRoutine()
	defer atomic.AddInt32(RoutineConter, -1)
	if !b {
		WriteErr(w, "HTTP_ROUTINE:Too many routines")
		return
	}
	if !checkIp(req.RemoteAddr) {
		WriteErr(w, fmt.Sprintf("Invalid IP:%s", req.RemoteAddr))
		return
	}
	var username string = ""
	var user *dao.User = nil
	queryForm, err := url.ParseQuery(req.URL.RawQuery)
	if err == nil && len(queryForm["username"]) > 0 {
		username = queryForm["username"][0]
	}
	if username != "" {
		user = dao.GetUserByUsername(username)
	}
	if user == nil {
		WriteJson(w, "Invalid username:"+username)
	} else {
		WriteJson(w, user.GetTotalJson())
	}
}

var DEFAULT_EXPIRE_TIME time.Duration
var USER_LIST_CACHE *cache.Cache

func initCache() {
	DEFAULT_EXPIRE_TIME = time.Duration(env.LsCacheExpireTime) * time.Second
	USER_LIST_CACHE = cache.New(DEFAULT_EXPIRE_TIME, time.Duration(5)*time.Second)
}

func ListHandle(w http.ResponseWriter, req *http.Request) {
	b := checkRoutine()
	defer atomic.AddInt32(RoutineConter, -1)
	if !b {
		WriteErr(w, "HTTP_ROUTINE:Too many routines")
		return
	}
	if !checkIp(req.RemoteAddr) {
		WriteErr(w, fmt.Sprintf("Invalid IP:%s", req.RemoteAddr))
		return
	}
	lastId := -1
	count := 1000
	queryForm, err := url.ParseQuery(req.URL.RawQuery)
	if err == nil {
		if len(queryForm["lastId"]) > 0 {
			lastId = env.ToInt(queryForm["lastId"][0], -1)
		}
		if len(queryForm["count"]) > 0 {
			count = env.StringToInt(queryForm["count"][0], 100, 10000, 10000)
		}
	}
	key := fmt.Sprintf("%d-%d", lastId, count)
	v, found := USER_LIST_CACHE.Get(key)
	if found {
		logrus.Infof("[ListUserHandle]From Cache\n")
		WriteJson(w, v.(string))
		return
	}
	ls, err := dao.ListUsers(int32(lastId), count, bson.M{"_id": 1, "username": 1, "spaceTotal": 1})
	if err != nil {
		WriteErr(w, "ListUserHandle err:"+err.Error())
		return
	}
	objs := []*bson.M{}
	for _, u := range ls {
		total := uint64(0)
		if u.SpaceTotal > 0 {
			total = uint64(u.SpaceTotal)
		}
		umap := &bson.M{
			"userId":     u.UserID,
			"userName":   u.Username,
			"spaceTotal": total,
		}
		objs = append(objs, umap)
	}
	res, _ := json.Marshal(objs)
	logrus.Infof("[ListUserHandle]Return %d\n", len(ls))
	USER_LIST_CACHE.Set(key, string(res), DEFAULT_EXPIRE_TIME)
	WriteJson(w, string(res))
}
