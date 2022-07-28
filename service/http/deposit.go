package http

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"sync/atomic"

	"github.com/sirupsen/logrus"
	"github.com/yottachain/YTCoreService/net/eos"
)

func UndepStoreHandle(w http.ResponseWriter, req *http.Request) {
	b := checkRoutine()
	defer atomic.AddInt32(RoutineConter, -1)
	if !b {
		WriteErr(w, "HTTP_ROUTINE:Too many routines")
		return
	}
	var username string
	queryForm, err := url.ParseQuery(req.URL.RawQuery)
	if err != nil {
		WriteErr(w, "Bad request:"+err.Error())
		return
	}

	if len(queryForm["username"]) > 0 {
		username = queryForm["username"][0]
	}

	if username == "" {
		logrus.Errorf("[HttpDN]Bad request:UndepStore,username=%s\n", username)
		WriteErr(w, "Bad request")
		return
	}
	logrus.Infof("[HttpDN]Call API:UndepStore,username=%s\n", username)
	err = eos.UndepStore(username)
	if err != nil {
		emsg := fmt.Sprintf("[HttpDN]Call API:UndepStore,ERR:%s\n", err.Error())
		logrus.Errorf(emsg)
		WriteErr(w, emsg)
	} else {
		WriteText(w, "OK")
	}
}

func QueryDepositHandle(w http.ResponseWriter, req *http.Request) {
	b := checkRoutine()
	defer atomic.AddInt32(RoutineConter, -1)
	if !b {
		WriteErr(w, "HTTP_ROUTINE:Too many routines")
		return
	}
	var username string
	queryForm, err := url.ParseQuery(req.URL.RawQuery)
	if err != nil {
		WriteErr(w, "Bad request:"+err.Error())
		return
	}

	if len(queryForm["username"]) > 0 {
		username = queryForm["username"][0]
	}

	if username == "" {
		logrus.Errorf("[HttpDN]Bad request:QueryDepositHandle,username=%s\n", username)
		WriteErr(w, "Bad request")
		return
	}
	logrus.Infof("[HttpDN]Call API:QueryDepositHandle,username=%s\n", username)
	user, err := eos.QueryDeposit(username)
	if err != nil {
		emsg := fmt.Sprintf("[HttpDN]Call API:QueryDepositHandle,ERR:%s\n", err.Error())
		logrus.Errorf(emsg)
		WriteErr(w, emsg)
	} else {
		res, _ := json.Marshal(user)
		WriteJson(w, string(res))
	}
}
