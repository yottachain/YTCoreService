package http

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/yottachain/YTCoreService/env"
	"github.com/yottachain/YTCoreService/net"
)

var ActiveNodesCache = struct {
	Value     atomic.Value
	LastTimes *int64
}{LastTimes: new(int64)}

func ActiveNodesHandle(w http.ResponseWriter, req *http.Request) {
	b := checkRoutine()
	defer atomic.AddInt32(RoutineConter, -1)
	if !b {
		WriteErr(w, "HTTP_ROUTINE:Too many routines")
		return
	}
	if time.Now().Unix()-atomic.LoadInt64(ActiveNodesCache.LastTimes) < CacheExpiredTime {
		v := ActiveNodesCache.Value.Load()
		if v != nil {
			ss, _ := v.(string)
			WriteJson(w, ss)
			return
		}
	}
	nodes, err := net.NodeMgr.ActiveNodesList()
	if err != nil {
		WriteErr(w, "ActiveNodesList err:"+err.Error())
	} else {
		ns := make([]map[string]interface{}, len(nodes))
		for index, n := range nodes {
			m := make(map[string]interface{})
			m["id"] = strconv.Itoa(int(n.ID))
			m["ip"] = n.Addrs
			m["nodeid"] = n.NodeID
			m["weight"] = strconv.FormatFloat(n.Weight, 'f', -1, 64)
			ns[index] = m
		}
		txt, err := json.Marshal(ns)
		if err != nil {
			WriteErr(w, "ActiveNodesList Marshal err:"+err.Error())
		} else {
			ss := string(txt)
			ActiveNodesCache.Value.Store(ss)
			atomic.StoreInt64(ActiveNodesCache.LastTimes, time.Now().Unix())
			WriteJson(w, ss)
		}
	}
}

var ReadableNodesCache = struct {
	Value     atomic.Value
	LastTimes *int64
}{LastTimes: new(int64)}

func ReadableNodesHandle(w http.ResponseWriter, req *http.Request) {
	b := checkRoutine()
	defer atomic.AddInt32(RoutineConter, -1)
	if !b {
		WriteErr(w, "HTTP_ROUTINE:Too many routines")
		return
	}
	if time.Now().Unix()-atomic.LoadInt64(ReadableNodesCache.LastTimes) < CacheExpiredTime {
		v := ReadableNodesCache.Value.Load()
		if v != nil {
			ss, _ := v.(string)
			WriteJson(w, ss)
			return
		}
	}
	timerange := 0
	queryForm, err := url.ParseQuery(req.URL.RawQuery)
	if err == nil && len(queryForm["timerange"]) > 0 {
		timerange = env.ToInt(queryForm["timerange"][0], 0)
	}
	nodes, err := net.NodeMgr.ReadableNodesList(timerange)
	if err != nil {
		WriteErr(w, "ReadableNodesList err:"+err.Error())
	} else {
		ns := make([]map[string]interface{}, len(nodes))
		for index, n := range nodes {
			m := make(map[string]interface{})
			m["id"] = strconv.Itoa(int(n.ID))
			//m["ip"] = n.Addrs
			//m["nodeid"] = n.NodeID
			m["weight"] = strconv.FormatFloat(n.Weight, 'f', -1, 64)
			ns[index] = m
		}
		txt, err := json.Marshal(ns)
		if err != nil {
			WriteErr(w, "ReadableNodesList Marshal err:"+err.Error())
		} else {
			ss := string(txt)
			ReadableNodesCache.Value.Store(ss)
			atomic.StoreInt64(ReadableNodesCache.LastTimes, time.Now().Unix())
			WriteJson(w, ss)
		}
	}
}

var StatisticsCache = struct {
	Value     atomic.Value
	LastTimes *int64
}{LastTimes: new(int64)}

func StatisticsHandle(w http.ResponseWriter, req *http.Request) {
	b := checkRoutine()
	defer atomic.AddInt32(RoutineConter, -1)
	if !b {
		WriteErr(w, "HTTP_ROUTINE:Too many routines")
		return
	}
	if time.Now().Unix()-atomic.LoadInt64(StatisticsCache.LastTimes) < CacheExpiredTime {
		v := StatisticsCache.Value.Load()
		if v != nil {
			ss, _ := v.(string)
			WriteJson(w, ss)
			return
		}
	}
	stat, err := net.NodeMgr.Statistics()
	if err != nil {
		WriteErr(w, "Statistics err:"+err.Error())
	} else {
		txt, err := json.Marshal(stat)
		if err != nil {
			WriteErr(w, "Statistics Marshal err:"+err.Error())
		} else {
			ss := string(txt)
			StatisticsCache.Value.Store(ss)
			atomic.StoreInt64(StatisticsCache.LastTimes, time.Now().Unix())
			WriteJson(w, ss)
		}
	}
}

func NewnodeidHandle(w http.ResponseWriter, req *http.Request) {
	b := checkRoutine()
	defer atomic.AddInt32(RoutineConter, -1)
	if !b {
		WriteErr(w, "HTTP_ROUTINE:Too many routines")
		return
	}
	id, err := net.NodeMgr.NewNodeID()
	if err != nil {
		WriteErr(w, "NewNodeID err:"+err.Error())
	} else {
		txt := fmt.Sprintf("{\"nodeid\": %d}", id)
		WriteJson(w, txt)
	}
}

func ReadRequest(req *http.Request) (string, error) {
	rbody := req.Body
	rdata := make([]byte, 8192)
	content := []byte{}
	for {
		nRead, err := rbody.Read(rdata)
		if err != nil {
			if err == io.EOF {
				if nRead > 0 {
					content = append(content, rdata[:nRead]...)
				}
				break
			} else {
				return "", err
			}
		} else {
			if nRead > 0 {
				content = append(content, rdata[:nRead]...)
			}
		}
	}
	return string(content), nil
}

func CallApiHandle(w http.ResponseWriter, req *http.Request, callname string) {
	b := checkRoutine()
	defer atomic.AddInt32(RoutineConter, -1)
	if !b {
		WriteErr(w, "HTTP_ROUTINE:Too many routines")
		return
	}
	if !checkPostMethod(req) {
		WriteErr(w, "Post method required")
	} else {
		trx, err := ReadRequest(req)
		if err != nil {
			WriteErr(w, "ReadRequest err:"+err.Error())
		} else {
			apiname := callname
			logrus.Infof("[HttpDN]Call API:%s,trx:%s\n", apiname, trx)
			err := net.NodeMgr.CallAPI(trx, apiname)
			if err != nil {
				emsg := fmt.Sprintf("[HttpDN]Call API:%s,ERR:%s\n", apiname, err.Error())
				logrus.Errorf(emsg)
				WriteErr(w, emsg)
			} else {
				if callname == "PreRegisterNode" {
					WriteJson(w, fmt.Sprintf("{\"shardsize\":%d}", env.PFL/1024))
				} else {
					WriteText(w, "OK")
				}
			}
		}
	}
}

func PreregnodeHandle(w http.ResponseWriter, req *http.Request) {
	CallApiHandle(w, req, "PreRegisterNode")
}

func ChangeminerpoolHandle(w http.ResponseWriter, req *http.Request) {
	CallApiHandle(w, req, "ChangeMinerPool")
}

func ChangeAdminAccHandle(w http.ResponseWriter, req *http.Request) {
	CallApiHandle(w, req, "ChangeAdminAcc")
}

func ChangeProfitAccHandle(w http.ResponseWriter, req *http.Request) {
	CallApiHandle(w, req, "ChangeProfitAcc")
}

func ChangePoolIDHandle(w http.ResponseWriter, req *http.Request) {
	CallApiHandle(w, req, "ChangePoolID")
}

func ChangeAssignedSpaceHandle(w http.ResponseWriter, req *http.Request) {
	CallApiHandle(w, req, "ChangeAssignedSpace")
}

func ChangeDepAccHandle(w http.ResponseWriter, req *http.Request) {
	CallApiHandle(w, req, "ChangeDepAcc")
}

func ChangeDepositHandle(w http.ResponseWriter, req *http.Request) {
	CallApiHandle(w, req, "ChangeDeposit")
}

func IncreaseDepositHandle(w http.ResponseWriter, req *http.Request) {
	CallApiHandle(w, req, "IncreaseDeposit")
}

func NodeQuitHandle(w http.ResponseWriter, req *http.Request) {
	b := checkRoutine()
	defer atomic.AddInt32(RoutineConter, -1)
	if !b {
		WriteErr(w, "HTTP_ROUTINE:Too many routines")
		return
	}
	var nodeID int = -1
	var nonce, signature string
	queryForm, err := url.ParseQuery(req.URL.RawQuery)
	if err != nil {
		WriteErr(w, "Bad request:"+err.Error())
		return
	}
	if len(queryForm["nodeID"]) > 0 {
		nodeID, _ = strconv.Atoi(queryForm["nodeID"][0])
	}
	if len(queryForm["nonce"]) > 0 {
		nonce = queryForm["nonce"][0]
	}
	if len(queryForm["signature"]) > 0 {
		signature = queryForm["signature"][0]
	}
	if nodeID == -1 || nonce == "" || signature == "" {
		logrus.Errorf("[HttpDN]Bad request:NodeQuit,nodeID=%d&nonce=%s&signature=%s\n", nodeID, nonce, signature)
		WriteErr(w, "Bad request")
		return
	}
	logrus.Infof("[HttpDN]Call API:NodeQuit,nodeID=%s&nonce=%s&signature=%s\n", nodeID, nonce, signature)
	err = net.NodeMgr.NodeQuit(int32(nodeID), nonce, signature)
	if err != nil {
		emsg := fmt.Sprintf("[HttpDN]Call API:NodeQuit,ERR:%s\n", err.Error())
		logrus.Errorf(emsg)
		WriteErr(w, emsg)
	} else {
		WriteText(w, "OK")
	}
}
