package stat

import (
	"fmt"
	"log"
	"os"
	"reflect"
	"sync"
	"time"
)

type ccstat struct {
	sync.Mutex
	ccShardsG   int32
	ccShards	int32
	sendShs		uint64
	sendShSucs	uint64
	ccBlks		int32
	ccGts		int32
	gts 		uint64
	gtSucs 		uint64
	fd 	*os.File
}

var Ccstat = ccstat{ccShardsG:0, ccShards:0, sendShs:0, sendShSucs:0, ccGts:0, ccBlks:0, gts:0, gtSucs:0}

func (ccs *ccstat) ShardCcAdd() {
	ccs.Lock()
	defer ccs.Unlock()
	ccs.ccShards++
}


func (ccs *ccstat) ShardCcSub() {
	ccs.Lock()
	defer ccs.Unlock()
	ccs.ccShards--
}

//并发上传分片的协程数
func (ccs *ccstat) ShardCcGAdd() {
	ccs.Lock()
	defer ccs.Unlock()
	ccs.ccShardsG++
}

//并发上传分片的协程数
func (ccs *ccstat) ShardCcGSub() {
	ccs.Lock()
	defer ccs.Unlock()
	ccs.ccShardsG--
}

func (ccs *ccstat) Shards() {
	ccs.Lock()
	defer ccs.Unlock()
	ccs.sendShs++
}

func (ccs *ccstat) ShardSucs() {
	ccs.Lock()
	defer ccs.Unlock()
	ccs.sendShSucs++
}

func (ccs *ccstat) BlkCcAdd() {
	ccs.Lock()
	defer ccs.Unlock()
	ccs.ccBlks++
}

func (ccs *ccstat) BlkCcSub() {
	ccs.Lock()
	defer ccs.Unlock()
	ccs.ccBlks--
}

func (ccs *ccstat) GtCcAdd() {
	ccs.Lock()
	defer ccs.Unlock()
	ccs.ccGts++
}

func (ccs *ccstat) GtCcSub() {
	ccs.Lock()
	defer ccs.Unlock()
	ccs.ccGts--
}

func (ccs *ccstat) GtsAdd() {
	ccs.Lock()
	defer ccs.Unlock()
	ccs.gts++
}

func (ccs *ccstat) GtSucsAdd() {
	ccs.Lock()
	defer ccs.Unlock()
	ccs.gtSucs++
}

func init() {
	fd, err := os.OpenFile("stat.log", os.O_WRONLY|os.O_CREATE|os.O_TRUNC, os.ModePerm)
	if err != nil {
		log.Fatalln("open stat.log fail  "+ err.Error())
	}
	Ccstat.fd = fd
}

func (ccs *ccstat) PrintCc() {
	var sshs = uint64(0)
	var sshsucs = uint64(0)
	var gts = uint64(0)
	var gtSucs = uint64(0)

	for {
		<- time.After(time.Second*1)
		ccs.Lock()
		_, _ = fmt.Fprintf(ccs.fd, "send-blk-goroutine-cc=%d get-token-cc=%d send-shard-go-cc=%d send-shard-rungo-cc=%d\n",
			ccs.ccBlks, ccs.ccGts, ccs.ccShards, ccs.ccShardsG)
		_, _ = fmt.Fprintf(ccs.fd, "gts=%d/s  gt-success=%d/s send-shards=%d/s  send-success-shards=%d/s\n",
			ccs.gts - gts, ccs.gtSucs - gtSucs, ccs.sendShs - sshs, ccs.sendShSucs - sshsucs)
		gts = ccs.gts
		gtSucs = ccs.gtSucs
		sshs = ccs.sendShs
		sshsucs = ccs.sendShSucs
		ccs.Unlock()
	}
}

func (ccs *ccstat) Println(key string, v interface{}) {
	t := reflect.TypeOf(v)
	vt := reflect.ValueOf(v)
	var str = ""

	if k := t.Kind(); k == reflect.Struct {
		for i := 0; i < t.NumField(); i++ {
			key := t.Field(i)
			value := vt.Field(i).Interface()

			s := fmt.Sprintf("%s = %v ", key.Name, value)
			str = str + s
		}
		_, _ = fmt.Fprintf(ccs.fd, "%s=(%v)\n", key, str)
	}else {
		_, _ = fmt.Fprintf(ccs.fd, "%s=%v\n", key, vt)
	}

	_ = ccs.fd.Sync()
}

func (ccs *ccstat) Clean() {
	_ = ccs.fd.Close()
}


