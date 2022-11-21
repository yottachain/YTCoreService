package v2

import (
	"encoding/json"
	"io/ioutil"
	"net/http"

	"github.com/aurawing/eos-go/btcsuite/btcutil/base58"
	"github.com/gin-gonic/gin"
	"github.com/sirupsen/logrus"
	"github.com/yottachain/YTCoreService/api"
	"github.com/yottachain/YTCoreService/env"
	"github.com/yottachain/YTCoreService/pkt"
)

func Download(g *gin.Context) {
	defer env.TracePanic("[S3EXT][Download]")
	data, err := ioutil.ReadAll(g.Request.Body)
	if err != nil {
		g.AbortWithError(http.StatusBadRequest, err)
		return
	}
	req := make(map[string]interface{})
	err = json.Unmarshal(data, &req)
	if err != nil {
		logrus.Errorf("[S3EXT]Marshal Download req ERR:%s\n", err)
		g.AbortWithError(http.StatusBadRequest, err)
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
	init := g.Query("init")
	if init == "true" {
		bucketName, _ := req["bucketName"].(string)
		fileName, _ := req["fileName"].(string)
		sgx, errmsg := c.DownloadToSGX(bucketName, fileName)
		if errmsg != nil {
			g.AbortWithError(http.StatusBadRequest, pkt.ToError(errmsg))
			return
		}
		res := make([]map[string]interface{}, len(sgx.Refs))
		for index, ref := range sgx.Refs {
			r := make(map[string]interface{})
			r["REF"] = base58.Encode(ref.Bytes())
			r["KeyNumber"] = ref.KeyNumber
			r["KEU"] = base58.Encode(ref.KEU)
			res[index] = r
		}
		g.JSON(http.StatusOK, res)
	} else {
		ref, _ := req["ref"].(string)
		refer := pkt.NewRefer(base58.Decode(ref))
		dn := &api.DownloadBlock{UClient: c, Ref: refer}
		eb, errmsg := dn.LoadEncryptedBlock()
		if errmsg != nil {
			logrus.Errorf("[S3EXT]Download ERR:%s\n", err)
			g.AbortWithError(http.StatusBadRequest, pkt.ToError(errmsg))
			return
		}
		g.Writer.Write(eb.Data)
	}
}
