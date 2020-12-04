package stat

import (
	"fmt"
	"github.com/yottachain/YTCoreService/env"
	"log"
	"os"
	"reflect"
	"sync"
	"time"
)

type ccstat struct {
	IsOpenStat bool
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

var Ccstat = &ccstat{IsOpenStat: env.Openstat, ccShardsG:0, ccShards:0, sendShs:0, sendShSucs:0, ccGts:0, ccBlks:0, gts:0, gtSucs:0}

func (ccs *ccstat) ShardCcAdd() {
	if !ccs.IsOpenStat {
		return
	}
	ccs.Lock()
	defer ccs.Unlock()
	ccs.ccShards++
}


func (ccs *ccstat) ShardCcSub() {
	if !ccs.IsOpenStat {
		return
	}
	ccs.Lock()
	defer ccs.Unlock()
	ccs.ccShards--
}

//并发上传分片的协程数
func (ccs *ccstat) ShardCcGAdd() {
	if !ccs.IsOpenStat {
		return
	}
	ccs.Lock()
	defer ccs.Unlock()
	ccs.ccShardsG++
}

//并发上传分片的协程数
func (ccs *ccstat) ShardCcGSub() {
	if !ccs.IsOpenStat {
		return
	}
	ccs.Lock()
	defer ccs.Unlock()
	ccs.ccShardsG--
}

func (ccs *ccstat) Shards() {
	if !ccs.IsOpenStat {
		return
	}
	ccs.Lock()
	defer ccs.Unlock()
	ccs.sendShs++
}

func (ccs *ccstat) ShardSucs() {
	if !ccs.IsOpenStat {
		return
	}
	ccs.Lock()
	defer ccs.Unlock()
	ccs.sendShSucs++
}

func (ccs *ccstat) BlkCcAdd() {
	if !ccs.IsOpenStat {
		return
	}
	ccs.Lock()
	defer ccs.Unlock()
	ccs.ccBlks++
}

func (ccs *ccstat) BlkCcSub() {
	if !ccs.IsOpenStat {
		return
	}
	ccs.Lock()
	defer ccs.Unlock()
	ccs.ccBlks--
}

func (ccs *ccstat) GtCcAdd() {
	if !ccs.IsOpenStat {
		return
	}
	ccs.Lock()
	defer ccs.Unlock()
	ccs.ccGts++
}

func (ccs *ccstat) GtCcSub() {
	if !ccs.IsOpenStat {
		return
	}
	ccs.Lock()
	defer ccs.Unlock()
	ccs.ccGts--
}

func (ccs *ccstat) GtsAdd() {
	if !ccs.IsOpenStat {
		return
	}
	ccs.Lock()
	defer ccs.Unlock()
	ccs.gts++
}

func (ccs *ccstat) GtSucsAdd() {
	if !ccs.IsOpenStat {
		return
	}
	ccs.Lock()
	defer ccs.Unlock()
	ccs.gtSucs++
}

func init() {
	fd, err := os.OpenFile(env.YTFS_HOME+"stat.log", os.O_WRONLY|os.O_CREATE|os.O_TRUNC, os.ModePerm)
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
		ccs.IsOpenStat = env.Openstat
		if !ccs.IsOpenStat {
			continue
		}
		ccs.Lock()
		_, _ = fmt.Fprintf(ccs.fd, "send-blk-goroutine-cc=%d get-token-cc=%d send-shard-go-cc=%d send-shard-rungo-cc=%d\n",
			ccs.ccBlks, ccs.ccGts, ccs.ccShards, ccs.ccShardsG)

		gts = ccs.gts
		ccs.gts = 0
		gtSucs = ccs.gtSucs
		ccs.gtSucs = 0
		sshs = ccs.sendShs
		ccs.sendShs = 0
		sshsucs = ccs.sendShSucs
		ccs.sendShSucs = 0
		_, _ = fmt.Fprintf(ccs.fd, "gts=%d/s  gt-success=%d/s send-shards=%d/s  send-success-shards=%d/s\n",
			gts, gtSucs, sshs, sshsucs)

		ccs.Unlock()
	}
}

func (ccs *ccstat) Println(key string, v interface{}) {
	ccs.IsOpenStat = env.Openstat
	if !ccs.IsOpenStat {
		return
	}
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
	if !ccs.IsOpenStat {
		return
	}
	_ = ccs.fd.Close()
}


