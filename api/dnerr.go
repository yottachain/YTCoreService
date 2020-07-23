package api

import (
	"strconv"
	"time"

	"github.com/patrickmn/go-cache"
	"github.com/yottachain/YTCoreService/env"
)

var ERR_LIST_CACHE = cache.New(time.Duration(180)*time.Second, time.Duration(5)*time.Second)

func GetExpiredTime() time.Duration {
	if env.PTR == 0 {
		return time.Duration(180) * time.Second
	} else {
		return time.Duration(env.PTR*60*2) * time.Second
	}
}

func AddError(id int32) {
	ERR_LIST_CACHE.Set(strconv.Itoa(int(id)), "", GetExpiredTime())
}

func ErrorList() []int32 {
	var ids []int32
	ls := ERR_LIST_CACHE.Items()
	for idstr := range ls {
		id, err := strconv.Atoi(idstr)
		if err == nil {
			ids = append(ids, int32(id))
			if len(ids) >= 300 {
				break
			}
		}
	}
	return ids
}
