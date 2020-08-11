package apisrv

import (
	"fmt"
	"net"
	"net/http"
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

func ApiHandle(w http.ResponseWriter, req *http.Request) {
	//
}
