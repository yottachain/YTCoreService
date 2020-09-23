package stat

import (
	"fmt"
	"log"
	"os"
	"reflect"
	"sync"
)

type ccstat struct {
	sync.Mutex
	ccShards	int32
	fd 	*os.File
}

var Ccstat = ccstat{ccShards:0}

func (ccs *ccstat) shardCcAdd() {
	ccs.Lock()
	defer ccs.Unlock()
	ccs.ccShards++
}

func (ccs *ccstat) init() {
	fd, err := os.OpenFile("stat.log", os.O_WRONLY|os.O_CREATE|os.O_TRUNC, os.ModePerm)
	if err != nil {
		log.Fatalln("open stat.log fail  "+ err.Error())
	}
	ccs.fd = fd
}

func (ccs *ccstat) Println(key string, v interface{}) {
	t := reflect.TypeOf(v)
	vt := reflect.ValueOf(v)
	var str = ""

	for i := 0; i < t.NumField(); i++ {
		key := t.Field(i)
		value := vt.Field(i).Interface()

		s := fmt.Sprintf("%s = %v ", key.Name, value)
		str = str + s
	}
	_, _ = fmt.Fprintf(ccs.fd, "%s=(%v)\n", key, str)
	_ = ccs.fd.Sync()
}

func (ccs *ccstat) Clean() {
	_ = ccs.fd.Close()
}


