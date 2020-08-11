package apisrv

import (
	"net"
	"net/http"
)

func Start() {

	server := &http.Server{
		//Addr: fmt.Sprintf(":%d", port),
	}
	go server.ListenAndServe()
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
