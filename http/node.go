package http

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/yottachain/YTCoreService/net"
)

var ActiveNodesCache = struct {
	Value     atomic.Value
	LastTimes *int64
}{LastTimes: new(int64)}

func ActiveNodesHandle(w http.ResponseWriter, req *http.Request) {
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

var StatisticsCache = struct {
	Value     atomic.Value
	LastTimes *int64
}{LastTimes: new(int64)}

func StatisticsHandle(w http.ResponseWriter, req *http.Request) {
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
				logrus.Infof(emsg)
				WriteErr(w, emsg)
			} else {
				WriteText(w, "OK")
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
