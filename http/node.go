package http

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"github.com/yottachain/YTCoreService/env"
	"github.com/yottachain/YTCoreService/net"
)

func ActiveNodesHandle(w http.ResponseWriter, req *http.Request) {
	nodes, err := net.NodeMgr.ActiveNodesList()
	if err != nil {
		WriteErr(w, "ActiveNodesList err:"+err.Error())
	} else {
		txt, err := json.Marshal(nodes)
		if err != nil {
			WriteErr(w, "ActiveNodesList Marshal err:"+err.Error())
		} else {
			WriteJson(w, string(txt))
		}
	}
}

func StatisticsHandle(w http.ResponseWriter, req *http.Request) {
	stat, err := net.NodeMgr.Statistics()
	if err != nil {
		WriteErr(w, "Statistics err:"+err.Error())
	} else {
		txt, err := json.Marshal(stat)
		if err != nil {
			WriteErr(w, "Statistics Marshal err:"+err.Error())
		} else {
			WriteJson(w, string(txt))
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
			env.Log.Infof("Call API:%s,trx:%s\n", apiname, trx)
			err := net.NodeMgr.CallAPI(trx, apiname)
			if err != nil {
				emsg := fmt.Sprintf("Call API:%s,ERR:%s\n", apiname, err.Error())
				env.Log.Infof(emsg)
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
