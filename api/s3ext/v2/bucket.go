package v2

import (
	"encoding/json"
	"io/ioutil"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/sirupsen/logrus"
	"github.com/yottachain/YTCoreService/api"
	"github.com/yottachain/YTCoreService/env"
	"github.com/yottachain/YTCoreService/pkt"
)

func CreateBucket(g *gin.Context) {
	defer env.TracePanic("[S3EXT][ListBuckets]")
	data, err := ioutil.ReadAll(g.Request.Body)
	if err != nil {
		g.AbortWithError(http.StatusBadRequest, err)
		return
	}
	req := make(map[string]interface{})
	err = json.Unmarshal(data, &req)
	if err != nil {
		g.AbortWithError(http.StatusBadRequest, err)
		logrus.Errorf("[S3EXT]Marshal ListBuckets req ERR:%s\n", err)
		return
	}
	userid, _ := req["userid"].(float64)
	keynumber, _ := req["keynumber"].(float64)
	sign, _ := req["sign"].(string)
	bucketname, _ := req["bucketName"].(string)
	c, err := api.AddClient(uint32(userid), uint32(keynumber), uint32(keynumber), sign)
	if err != nil {
		g.AbortWithError(http.StatusBadRequest, err)
		return
	}
	bucketAccessor := c.NewBucketAccessor()
	errmsg := bucketAccessor.CreateBucket(bucketname, []byte{})
	if errmsg != nil {
		g.AbortWithError(http.StatusBadRequest, pkt.ToError(errmsg))
		return
	} else {
		g.JSON(http.StatusOK, nil)
	}
}

func ListBuckets(g *gin.Context) {
	defer env.TracePanic("[S3EXT][ListBuckets]")
	data, err := ioutil.ReadAll(g.Request.Body)
	if err != nil {
		g.AbortWithError(http.StatusBadRequest, err)
		return
	}
	req := make(map[string]interface{})
	err = json.Unmarshal(data, &req)
	if err != nil {
		g.AbortWithError(http.StatusBadRequest, err)
		logrus.Errorf("[S3EXT]Marshal ListBuckets req ERR:%s\n", err)
		return
	}
	userid, _ := req["userid"].(float64)
	keynumber, _ := req["keynumber"].(float64)
	sign, _ := req["sign"].(string)
	c, err := api.AddClient(uint32(userid), uint32(keynumber), uint32(keynumber), sign)
	if err != nil {
		g.AbortWithError(http.StatusBadRequest, err)
		return
	}
	bucketAccessor := c.NewBucketAccessor()
	res, errmsg := bucketAccessor.ListBucket()
	if errmsg != nil {
		g.AbortWithError(http.StatusBadRequest, pkt.ToError(errmsg))
		return
	} else {
		g.JSON(http.StatusOK, res)
	}
}
