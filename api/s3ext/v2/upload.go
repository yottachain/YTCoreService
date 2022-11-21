package v2

import (
	"errors"
	"io"
	"net/http"
	"os"

	"github.com/aurawing/eos-go/btcsuite/btcutil/base58"
	"github.com/gin-gonic/gin"
	"github.com/sirupsen/logrus"
	"github.com/yottachain/YTCoreService/api"
	"github.com/yottachain/YTCoreService/api/cache"
	"github.com/yottachain/YTCoreService/env"
)

func Upload(g *gin.Context) {
	defer env.TracePanic("[S3EXT][Upload]")
	vhw := g.Query("VHW")
	path := api.MakePath(vhw)
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_TRUNC|os.O_CREATE, 0644)
	if err != nil {
		g.AbortWithError(http.StatusBadRequest, err)
		return
	}
	defer func() {
		if f != nil {
			f.Close()
		}
	}()
	read := g.Request.Body
	var lastread []byte
	for {
		readbuf := make([]byte, 1024)
		num, err := read.Read(readbuf)
		if err != nil && err != io.EOF {
			g.AbortWithError(http.StatusBadRequest, err)
			return
		}
		if num > 0 {
			if lastread == nil {
				lastread = readbuf[0:num]
			} else {
				if num >= 8 {
					f.Write(lastread)
					lastread = readbuf[0:num]
				} else {
					lastread = append(lastread, readbuf[0:num]...)
				}
			}
		}
		if err != nil && err == io.EOF {
			break
		}
	}
	size := len(lastread)
	if size < 8 {
		g.AbortWithError(http.StatusBadRequest, errors.New("encode err"))
		return
	}
	if size > 8 {
		f.Write(lastread[0 : size-8])
		f.WriteAt(lastread[size-8:], 0)
	} else {
		f.Seek(0, io.SeekStart)
		f.WriteAt(lastread, 0)
	}
	f.Close()
	f = nil
	sha := base58.Decode(vhw)
	inserterr := cache.InsertSyncObject(sha)
	if inserterr != nil {
		logrus.Errorf("[S3EXT][Upload]InsertSyncObject ERR:%s\n", inserterr)
		g.AbortWithError(http.StatusGatewayTimeout, err)
		return
	}
	logrus.Infof("[S3EXT][Upload]Upload object %s to cache OK.\n", vhw)
	g.JSON(http.StatusOK, nil)
}
