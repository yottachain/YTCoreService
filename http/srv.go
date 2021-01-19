package http

import (
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"regexp"
	"strings"
	"sync/atomic"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/yottachain/YTCoreService/env"
)

var home_page string
var ip_list []string
var server *http.Server
var RoutineConter *int32 = new(int32)

const CacheExpiredTime = 10

func Stop() {
	server.Close()
}

func Start(port int) error {
	path := env.YTSN_HOME + "res/statapi.html"
	data, err := ioutil.ReadFile(path)
	if err != nil {
		logrus.Errorf("[Http]Resource file 'statapi.html' read failure\n")
		return errors.New("Resource file 'statapi.html' read failure\n")
	}
	home_page = string(data)
	list := strings.Split(env.HttpRemoteIp, ";")
	for _, ip := range list {
		s := strings.TrimSpace(ip)
		if s != "" {
			ip_list = append(ip_list, s)
		}
	}
	http.HandleFunc("/total", TotalHandle)
	http.HandleFunc("/usertotal", UserTotalHandle)
	http.HandleFunc("/list", ListHandle)
	http.HandleFunc("/active_nodes", ActiveNodesHandle)
	http.HandleFunc("/readable_nodes", ReadableNodesHandle)

	http.HandleFunc("/statistics", StatisticsHandle)
	http.HandleFunc("/relationship", RelationshipHandle)
	http.HandleFunc("/newnodeid", NewnodeidHandle)
	http.HandleFunc("/preregnode", PreregnodeHandle)
	http.HandleFunc("/changeminerpool", ChangeminerpoolHandle)
	http.HandleFunc("/ChangeAdminAcc", ChangeAdminAccHandle)
	http.HandleFunc("/ChangeProfitAcc", ChangeProfitAccHandle)
	http.HandleFunc("/ChangePoolID", ChangePoolIDHandle)
	http.HandleFunc("/ChangeAssignedSpace", ChangeAssignedSpaceHandle)
	http.HandleFunc("/ChangeDepAcc", ChangeDepAccHandle)
	http.HandleFunc("/ChangeDeposit", ChangeDepositHandle)
	http.HandleFunc("/", RootHandle)
	InitCache()
	server = &http.Server{
		Addr:         fmt.Sprintf(":%d", port),
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 15 * time.Second,
	}
	err = server.ListenAndServe()
	if err != nil {
		logrus.Panicf("[Http]ListenAndServe: %s\n", err)
	}
	return nil
}

func RootHandle(w http.ResponseWriter, req *http.Request) {
	w.Header().Set("Content-Type", "text/html")
	io.WriteString(w, home_page)
}

func WriteText(w http.ResponseWriter, content string) {
	w.Header().Set("Content-Type", "text/plain")
	io.WriteString(w, content)
}

func WriteJson(w http.ResponseWriter, content string) {
	w.Header().Set("Content-Type", "text/json")
	io.WriteString(w, content)
}

func WriteErr(w http.ResponseWriter, err string) {
	w.WriteHeader(500)
	w.Header().Set("Content-Type", "text/plain")
	io.WriteString(w, err)
}

func checkPostMethod(req *http.Request) bool {
	m := strings.ToLower(req.Method)
	return m == "post"
}

func checkIp(ip string) bool {
	if ip_list == nil || len(ip_list) == 0 {
		return true
	}
	index := strings.Index(ip, ":")
	ip = ip[:index]
	for _, v := range ip_list {
		if v == "" || strings.Trim(v, " ") == "" {
			continue
		}
		b, _ := regexp.MatchString(v, ip)
		if b {
			return true
		}
	}
	return false
}

func checkRoutine() bool {
	atomic.AddInt32(RoutineConter, 1)
	if atomic.LoadInt32(RoutineConter) > env.MAX_HTTP_ROUTINE {
		return false
	} else {
		return true
	}
}
