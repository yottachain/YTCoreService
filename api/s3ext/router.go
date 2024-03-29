package s3ext

import (
	"net/http"
	"strconv"

	"github.com/gin-contrib/cors"
	"github.com/gin-gonic/gin"
	"github.com/sirupsen/logrus"
	"github.com/unrolled/secure"
	"github.com/yottachain/YTCoreService/api/s3ext/controller"
	api "github.com/yottachain/YTCoreService/api/s3ext/v2"
	"github.com/yottachain/YTCoreService/env"
)

func StartServer() {
	go func() {
		port := env.GetConfig().GetInt("S3ExtPort", 8080)
		router := InitRouter()
		var e error
		if env.CertFilePath == "" {
			e = router.Run(":" + strconv.Itoa(port))
		} else {
			e = router.Run(":" + strconv.Itoa(port))
			//e = router.RunTLS(":"+strconv.Itoa(port), env.CertFilePath, env.KeyFilePath)
		}
		if e != nil {
			logrus.Errorf("[S3EXT]Port %d,err:%s \n", port, e)
		}
	}()

}

func InitRouter() (router *gin.Engine) {
	router = gin.Default()
	config := cors.DefaultConfig()
	config.AllowAllOrigins = true

	router.Handle(http.MethodGet, "/", controller.Login)
	v1 := router.Group("/api/v1")
	{
		v1.POST("/insertuser", controller.Register)
		v1.GET("/addPubkey", controller.AddPubkey)
		v1.GET("/createBucket", controller.CreateBucket)
		v1.POST("/upload", controller.UploadFile)
		v1.GET("/getObject", controller.DownloadFile)
		v1.GET("/getBlockForSGX", controller.DownloadFileForSGX)
		v1.GET("/getObjectProgress", controller.GetDownloadProgress)
		v1.GET("/listBucket", controller.GetObjects)
		v1.GET("/listAllBucket", controller.ListBucket)
		v1.GET("/getProgress", controller.GetProgress)
		v1.GET("/getYts3Version", controller.GetProgramVersion)
		v1.GET("/getFileInfo", controller.GetFileBlockDetails)
		v1.GET("/getFileAllInfo", controller.GetFileAllInfo)
		v1.POST("/importAuthFile", controller.ImporterAuth)
		v1.GET("/exporterAuthData", controller.ExporterAuthData)
		v1.GET("/licensedTo", controller.LicensedTo)
		v1.POST("/saveFileToLocal", controller.SaveFileToLocal)
		v1.POST("/account/create", controller.CreateAccountCli)

	}
	v2 := router.Group("/api/v2")
	{
		v2.POST("/Register", api.Register)
		v2.POST("/ListBuckets", api.ListBuckets)
		v2.POST("/CreateBucket", api.CreateBucket)
		v2.POST("/Upload", api.Upload)
		v2.POST("/Download", api.Download)
		v2.POST("/ListObjects", api.ListObjects)
		v2.POST("/DeleteObject", api.DeleteObject)
	}
	return
}

func TlsHandler() gin.HandlerFunc {
	return func(c *gin.Context) {
		secureMiddleware := secure.New(secure.Options{
			SSLRedirect: true,
			SSLHost:     "127.0.0.1:8080",
		})
		err := secureMiddleware.Process(c.Writer, c.Request)
		if err != nil {
			logrus.Errorf("[S3EXT]Https err:%s\n", err)
			return
		}

		c.Next()
	}
}
