package http

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"

	"github.com/yottachain/YTCoreService/dao"
	"github.com/yottachain/YTCoreService/env"
	"github.com/yottachain/YTCoreService/handle"
	"github.com/yottachain/YTCoreService/net"
	"github.com/yottachain/YTCoreService/pkt"
	"go.mongodb.org/mongo-driver/bson"
)

func TotalHandle(w http.ResponseWriter, req *http.Request) {
	var errmsg string = ""
	if !checkIp(req.RemoteAddr) {
		errmsg = fmt.Sprintf("Invalid IP:%s", req.RemoteAddr)
	} else {
		userTotal, err := dao.GetUserCount()
		if err != nil {
			errmsg = "TotalHandle err:" + err.Error()
		} else {
			req := &pkt.TotalReq{B: new(bool)}
			var FileTotal uint64 = 0
			var SpaceTotal uint64 = 0
			var Usedspace uint64 = 0
			var BkTotal uint64 = 0
			var ActualBkTotal uint64 = 0
			data, err := handle.SyncRequest(req, -1, 3)
			if err != nil {
				errmsg = "TotalHandle err:" + err.Error()
			} else {
				for _, res := range data {
					if res != nil {
						if res.Error() != nil {
							errmsg = "TotalHandle err:" + res.Error().Msg
							break
						} else {
							total, _ := res.Response().(*pkt.TotalResp)
							FileTotal = FileTotal + *total.FileTotal
							SpaceTotal = SpaceTotal + *total.SpaceTotal
							Usedspace = Usedspace + *total.Usedspace
							BkTotal = BkTotal + *total.BkTotal
							ActualBkTotal = ActualBkTotal + *total.ActualBkTotal
						}
					}
				}
			}
			if errmsg == "" {
				resmap := make(map[string]uint64)
				resmap["userTotal"] = uint64(userTotal)
				resmap["fileTotal"] = FileTotal
				resmap["spaceTotal"] = SpaceTotal
				resmap["usedspace"] = Usedspace
				resmap["bkTotal"] = BkTotal
				resmap["actualBkTotal"] = ActualBkTotal
				res, _ := json.Marshal(resmap)
				WriteJson(w, string(res))
				return
			}
		}
	}
	WriteErr(w, errmsg)
}

func RelationshipHandle(w http.ResponseWriter, req *http.Request) {
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
	requset := &pkt.Relationship{Username: &username, MpoolOwner: &mPoolOwner}
	sn := net.GetRegSuperNode(username)
	var errmsg string = ""
	if sn.ID == int32(env.SuperNodeID) {
		handler := &handle.RelationshipHandler{}
		handler.SetPubkey(sn.PubKey)
		err := handler.SetMessage(requset)
		if err != nil {
			errmsg = "RelationshipHandle err:" + err.Msg
		} else {
			msg := handler.Handle()
			if errm, ok := msg.(*pkt.ErrorMessage); ok {
				errmsg = "RelationshipHandle err:" + errm.Msg
			}
		}
	} else {
		_, errm := net.RequestSN(requset, sn, "", 0)
		if err != nil {
			errmsg = "RelationshipHandle err:" + errm.Msg
		}
	}
	if errmsg != "" {
		WriteErr(w, errmsg)
	} else {
		WriteText(w, "OK")
	}
}

func UserTotalHandle(w http.ResponseWriter, req *http.Request) {
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
		var ress string = ""
		sn := net.GetUserSuperNode(user.UserID)
		if sn.ID == int32(env.SuperNodeID) {
			ress = user.GetTotalJson()
		} else {
			var errstr string = ""
			req := &pkt.UserSpaceReq{Userid: new(uint32)}
			*req.Userid = uint32(user.UserID)
			resp, err := net.RequestSN(req, sn, "", 0)
			if err != nil {
				errstr = "UserTotalHandle Err:" + err.Msg
			} else {
				msg, ok := resp.(*pkt.UserSpaceResp)
				if !ok {
					errstr = "Return Type Err"
				} else {
					ress = *msg.Jsonstr
				}
			}
			if errstr != "" {
				WriteJson(w, errstr)
				return
			}
		}
		WriteErr(w, ress)
	}
}

func ListHandle(w http.ResponseWriter, req *http.Request) {
	if !checkIp(req.RemoteAddr) {
		WriteErr(w, fmt.Sprintf("Invalid IP:%s", req.RemoteAddr))
		return
	}
	lsreq := &pkt.UserListReq{LastId: new(int32), Count: new(int32)}
	queryForm, err := url.ParseQuery(req.URL.RawQuery)
	if err == nil && len(queryForm["lastId"]) > 0 && len(queryForm["count"]) > 0 {
		*lsreq.LastId = int32(env.ToInt(queryForm["lastId"][0], -1))
		*lsreq.Count = int32(env.StringToInt(queryForm["lastId"][0], 100, 1000, 1000))
	}
	data, err := handle.SyncRequest(lsreq, -1, 3)
	users := []*pkt.UserListResp_UserSpace{}
	var errmsg string = ""
	if err != nil {
		errmsg = "ListHandle Err:" + err.Error()
	} else {
		for _, res := range data {
			if res != nil {
				if res.Error() != nil {
					errmsg = "ListHandle err:" + res.Error().Msg
					break
				} else {
					u, _ := res.Response().(*pkt.UserListResp)
					users = append(users, u.Userspace...)
				}
			}
		}
		if errmsg == "" {
			sortusers := &UserListSort{Users: users}
			WriteJson(w, sortusers.ToJson())
			return
		}
	}
	WriteErr(w, errmsg)
}

type UserListSort struct {
	Users []*pkt.UserListResp_UserSpace
}

func (acw UserListSort) ToJson() string {
	objs := []*bson.M{}
	for _, u := range acw.Users {
		umap := &bson.M{
			"userId":     u.UserId,
			"userName":   u.UserName,
			"spaceTotal": u.SpaceTotal,
		}
		objs = append(objs, umap)
	}
	res, _ := json.Marshal(objs)
	return string(res)
}

func (acw UserListSort) Len() int {
	return len(acw.Users)
}

func (acw UserListSort) Swap(i, j int) {
	acw.Users[i], acw.Users[j] = acw.Users[j], acw.Users[i]
}

func (acw UserListSort) Less(i, j int) bool {
	return *acw.Users[i].UserId < *acw.Users[j].UserId
}
