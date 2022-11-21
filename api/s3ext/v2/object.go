package v2

import (
	"encoding/json"
	"io/ioutil"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/sirupsen/logrus"
	"github.com/yottachain/YTCoreService/api"
	"github.com/yottachain/YTCoreService/api/backend"
	"github.com/yottachain/YTCoreService/env"
	"github.com/yottachain/YTCoreService/pkt"
	"github.com/yottachain/YTCoreService/s3"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

func DeleteObject(g *gin.Context) {
	defer env.TracePanic("[S3EXT][DeleteObject]")
	data, err := ioutil.ReadAll(g.Request.Body)
	if err != nil {
		g.AbortWithError(http.StatusBadRequest, err)
		return
	}
	req := make(map[string]interface{})
	err = json.Unmarshal(data, &req)
	if err != nil {
		g.AbortWithError(http.StatusBadRequest, err)
		logrus.Errorf("[S3EXT]Marshal DeleteObject req ERR:%s\n", err)
		return
	}
	userid, _ := req["userid"].(float64)
	keynumber, _ := req["keynumber"].(float64)
	sign, _ := req["sign"].(string)
	bucketname, _ := req["bucketName"].(string)
	fileName, _ := req["fileName"].(string)
	c, err := api.AddClient(uint32(userid), uint32(keynumber), uint32(keynumber), sign)
	if err != nil {
		g.AbortWithError(http.StatusBadRequest, err)
		return
	}
	accessor := c.NewObjectAccessor()
	errmsg := accessor.DeleteObject(bucketname, fileName, primitive.NilObjectID)
	if errmsg != nil {
		g.AbortWithError(http.StatusBadRequest, pkt.ToError(errmsg))
		return
	} else {
		g.JSON(http.StatusOK, nil)
	}
}

func ListObjects(g *gin.Context) {
	defer env.TracePanic("[S3EXT][ListObjects]")
	data, err := ioutil.ReadAll(g.Request.Body)
	if err != nil {
		g.AbortWithError(http.StatusBadRequest, err)
		return
	}
	req := make(map[string]interface{})
	err = json.Unmarshal(data, &req)
	if err != nil {
		g.AbortWithError(http.StatusBadRequest, err)
		logrus.Errorf("[S3EXT]Marshal ListObjects req ERR:%s\n", err)
		return
	}
	userid, _ := req["userid"].(float64)
	keynumber, _ := req["keynumber"].(float64)
	sign, _ := req["sign"].(string)
	bucketname, _ := req["bucketName"].(string)
	fileName, _ := req["fileName"].(string)
	limit, _ := req["limit"].(float64)
	c, err := api.AddClient(uint32(userid), uint32(keynumber), uint32(keynumber), sign)
	if err != nil {
		g.AbortWithError(http.StatusBadRequest, err)
		return
	}
	accessor := c.NewObjectAccessor()
	items, errmsg := accessor.ListObject(bucketname, fileName, "", false, primitive.NilObjectID, uint32(limit))
	if errmsg != nil {
		g.AbortWithError(http.StatusBadRequest, pkt.ToError(errmsg))
		return
	} else {
		response := s3.NewObjectList()
		lastFile := ""
		num := 0
		for _, v := range items {
			num++
			meta, err := api.BytesToFileMetaMap(v.Meta, primitive.ObjectID{})
			if err != nil {
				g.AbortWithError(http.StatusBadRequest, err)
				logrus.Warnf("[ListObjects]ERR meta,filename:%s\n", v.FileName)
				continue
			}
			t := time.Unix(v.FileId.Timestamp().Unix(), 0)
			meta["x-amz-meta-s3b-last-modified"] = t.Format("20060102T150405Z")
			content := backend.GetContentByMeta(meta)
			content.Key = v.FileName
			content.Owner = &s3.UserInfo{
				ID:          c.Username,
				DisplayName: c.Username,
			}
			response.Contents = append(response.Contents, content)
			lastFile = v.FileName
		}
		if int64(num) >= int64(limit) {
			response.NextMarker = lastFile
			response.IsTruncated = true
		}
		g.JSON(http.StatusOK, response)
	}
}
