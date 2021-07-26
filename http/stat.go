package http

import (
	"fmt"
	"net/http"
	"sync/atomic"

	"github.com/yottachain/YTCoreService/env"
	"github.com/yottachain/YTCoreService/handle"
)

var log *env.NoFmtLog

func UserStatHandle(w http.ResponseWriter, req *http.Request) {
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
	s := handle.UserSTATCache.Value.Load()
	if s == nil {
		WriteText(w, "")
	} else {
		ss, _ := s.(string)
		WriteText(w, ss)
	}
}
