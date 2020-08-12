package apisrv

import (
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"pkg/io/ioutil"
)

func Start() int {
	port, err := GetFreePort()
	if err != nil {
		port = 8030
	}
	http.HandleFunc("/api", ApiHandle)
	server := &http.Server{
		Addr: fmt.Sprintf(":%d", port),
	}
	go server.ListenAndServe()
	return port
}

func GetFreePort() (int, error) {
	addr, err := net.ResolveTCPAddr("tcp", "localhost:0")
	if err != nil {
		return 0, err
	}
	l, err := net.ListenTCP("tcp", addr)
	if err != nil {
		return 0, err
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port, nil
}

func WriteErr(w http.ResponseWriter, err string) {
	w.WriteHeader(500)
	w.Header().Set("Content-Type", "text/plain")
	io.WriteString(w, err)
}

func ApiHandle(w http.ResponseWriter, req *http.Request) {
	bs, err := ioutil.ReadAll(req.Body)
	if err != nil {
		WriteErr(w, err.Error())
		return
	}
	m := make(map[string]string)
	err = json.Unmarshal(bs, &m)
	if err != nil {
		WriteErr(w, err.Error())
		return
	}
	methodname := m["method"]
	if methodname == "regist" {
		//
	}
}
