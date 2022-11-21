package v2

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/sirupsen/logrus"
	"github.com/yottachain/YTCoreService/env"
	"github.com/yottachain/YTCoreService/net"
	"github.com/yottachain/YTCoreService/pkt"
)

func Register(g *gin.Context) {
	defer env.TracePanic("[S3EXT][Register]")
	data, err := ioutil.ReadAll(g.Request.Body)
	if err != nil {
		g.AbortWithError(http.StatusBadRequest, err)
		return
	}
	req := make(map[string]interface{})
	err = json.Unmarshal(data, &req)
	if err != nil {
		logrus.Errorf("[S3EXT]Marshal Register req ERR:%s\n", err)
		g.AbortWithError(http.StatusBadRequest, err)
		return
	}
	username, _ := req["userName"].(string)
	pubkeys, _ := req["pubKeys"].([]interface{})
	pub := make([]string, len(pubkeys))
	for index, p := range pubkeys {
		pub[index], _ = p.(string)
	}
	res, err := requestSN(pub, username)
	if err != nil {
		g.AbortWithError(http.StatusBadRequest, err)
	} else {
		g.JSON(http.StatusOK, res)
	}
}

func requestSN(pubs []string, username string) (map[string]interface{}, error) {
	req := &pkt.RegUserReqV3{VersionId: &env.Version, Username: &username, PubKey: pubs}
	res, err := net.RequestSN(req)
	if err != nil {
		emsg := fmt.Sprintf("User '%s' registration failed!%s", username, pkt.ToError(err))
		logrus.Errorf("[S3EXT][Register]%s\n", emsg)
		return nil, errors.New(emsg)
	} else {
		resp, ok := res.(*pkt.RegUserRespV2)
		if ok {
			if resp.UserId != nil && resp.KeyNumber != nil {
				if len(pubs) == len(resp.KeyNumber) {
					var numbers []int32
					for index, k := range pubs {
						num := resp.KeyNumber[index]
						if num == -1 {
							logrus.Infof("[S3EXT][Register]User '%s',publickey %s authentication error\n", username, k)
							return nil, fmt.Errorf("User '%s',publickey %s authentication error\n", username, k)
						}
						numbers = append(numbers, num)
					}
					res := make(map[string]interface{})
					res["userId"] = *resp.UserId
					res["keyNumbers"] = numbers
					logrus.Infof("[S3EXT][Register]User '%s' registration successful,ID:%d\n", username, *resp.UserId)
					return res, nil
				}
			}
		}
		logrus.Errorf("[S3EXT][Register]Return err msg.\n")
		return nil, errors.New("return err msg")
	}
}
