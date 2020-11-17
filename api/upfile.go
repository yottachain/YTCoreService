package api

import (
	"fmt"
	"sync"
)

var UPLOADING sync.Map

func PutUploadObject(userid int32, buck, key string, obj interface{}) {
	ss := fmt.Sprintf("%d/%s/%s", userid, buck, key)
	UPLOADING.Store(ss, obj)
}

func GetUploadObject(userid int32, buck, key string) interface{} {
	ss := fmt.Sprintf("%d/%s/%s", userid, buck, key)
	if vv, ok := UPLOADING.Load(ss); ok {
		return vv
	}
	return nil
}

func DelUploadObject(userid int32, buck, key string) {
	ss := fmt.Sprintf("%d/%s/%s", userid, buck, key)
	UPLOADING.Delete(ss)
}
