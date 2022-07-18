package env

import (
	"log"
	"strconv"
	"strings"

	"github.com/sirupsen/logrus"
)

const YTSN_ENV_SEC = "YTSN"
const YTSN_ENV_BPLIST_SEC = "YTSN.BPLS"
const YTSN_ENV_MONGO_SEC = "YTSN.MONGO"

var (
	LsCacheExpireTime int
	LsCachePageNum    int
	LsCursorLimit     int
	LsCacheMaxSize    int
)

var (
	DE_DUPLICATION bool   = true
	SPOTCHECK_ADDR string = ""
	REBUILD_ADDR   string = ""
	DelLogPath     string = ""
)

var HttpPort = 8082
var HttpRemoteIp string

var SUM_SERVICE bool = false

func readSnProperties() {
	confpath := YTSN_HOME + "conf/server.properties"
	config, err := NewConfig(confpath)
	if err != nil {
		logrus.Panicf("[Init]No properties file could be found for ytsn service:%s\n", confpath)
	}
	config.SetSection(YTSN_ENV_SEC)
	logConfig(config)

	SPOTCHECK_ADDR = config.GetString("SPOTCHECK_ADDR", "")
	REBUILD_ADDR = config.GetString("REBUILD_ADDR", "")

	HttpPort = config.GetRangeInt("httpPort", 8000, 20000, 8082)
	HttpRemoteIp = config.GetString("httpRemoteIp", "")

	DE_DUPLICATION = config.GetBool("DE_DUPLICATION", true)
	ShardNumPerNode = config.GetRangeInt("shardNumPerNode", 1, 200, 1)
	LsCacheExpireTime = config.GetRangeInt("lsCacheExpireTime", 5, 60*5, 30)
	LsCachePageNum = config.GetRangeInt("lsCachePageNum", 1, 100, 10)
	LsCursorLimit = config.GetRangeInt("lsCursorLimit", 0, 5, 1)
	LsCacheMaxSize = config.GetRangeInt("lsCacheMaxSize", 1000, 500000, 20000)
	Version = config.GetString("s3Version", "")
	DelLogPath = config.GetString("DelLogPath", "")
	if !strings.HasSuffix(DelLogPath, "/") {
		DelLogPath = DelLogPath + "/"
	}

	p2pConfig(config)
	routineConfig(config)
	eosConfig(config)
	feeConfig(config)

	SUM_SERVICE = config.GetBool("SUM_SERVICE", false)
}

var (
	MAX_HTTP_ROUTINE int32

	MAX_AYNC_ROUTINE          int32
	MAX_READ_ROUTINE          int32
	MAX_WRITE_ROUTINE         int32
	MAX_STAT_ROUTINE          int32
	MAX_DELBLK_ROUTINE        int32
	MAX_AUTH_ROUTINE          int32
	PER_USER_MAX_READ_ROUTINE int32
	SLOW_OP_TIMES             int
)

func routineConfig(config *Config) {
	MAX_HTTP_ROUTINE = int32(config.GetRangeInt("MAX_HTTP_ROUTINE", 500, 2000, 1000))
	MAX_DELBLK_ROUTINE = int32(config.GetRangeInt("MAX_DELBLK_ROUTINE", 3, 21*50, 21))
	MAX_AYNC_ROUTINE = int32(config.GetRangeInt("MAX_AYNC_ROUTINE", 500, 5000, 2000))
	MAX_WRITE_ROUTINE = int32(config.GetRangeInt("MAX_WRITE_ROUTINE", 500, 5000, 2000))
	MAX_READ_ROUTINE = int32(config.GetRangeInt("MAX_READ_ROUTINE", 200, 2000, 1000))
	MAX_STAT_ROUTINE = int32(config.GetRangeInt("MAX_STAT_ROUTINE", 200, 2000, 1000))
	PER_USER_MAX_READ_ROUTINE = int32(config.GetRangeInt("PER_USER_MAX_READ_ROUTINE", 1, 20, 5))
	SLOW_OP_TIMES = config.GetRangeInt("SLOW_OP_TIMES", 10, 200, 50)
}

const BP_ENABLE bool = true

var (
	EOSURI          string
	EOSAPI          string
	BPAccount       string
	ShadowAccount   string
	ShadowPriKey    string
	ContractAccount string
	ContractOwnerD  string
)

func eosConfig(config *Config) {
	EOSURI = config.GetString("eosURI", "")
	if EOSURI == "" {
		log.Panicf("The 'eosURI' parameter is not configured.\n")
	}
	//EOSAPI ="http://yts3api.yottachain.net:8888/v1/history/get_key_accounts"
	EOSAPI = config.GetString("eosAPI", "NA")
	BPAccount = config.GetString("BPAccount", "")
	if BPAccount == "" {
		log.Panicf("The 'BPAccount' parameter is not configured.\n")
	}
	ShadowAccount = config.GetString("ShadowAccount", "")
	if ShadowAccount == "" {
		log.Panicf("The 'ShadowAccount' parameter is not configured.\n")
	}
	ShadowPriKey = config.GetString("ShadowPriKey", "")
	if ShadowPriKey == "" {
		log.Panicf("The 'ShadowPriKey' parameter is not configured.\n")
	}
	ContractAccount = config.GetString("contractAccount", "")
	if ContractAccount == "" {
		log.Panicf("The 'contractAccount' parameter is not configured.\n")
	}
	ContractOwnerD = config.GetString("contractOwnerD", "")
	if ContractOwnerD == "" {
		log.Panicf("The 'contractOwnerD' parameter is not configured.\n")
	}
}

type PledgeSpaceFee struct {
	Level int
	Fee   int
}

var PLEDGE_SPACE_FEE []PledgeSpaceFee
var PLEDGE_SPACE_UPDATE_INTERVAL int
var SUM_USER_FEE int = 0
var PayInterval int
var Space_factor int

func feeConfig(config *Config) {
	var err error
	Space_factor = config.GetRangeInt("space_factor", 0, 100, 100)
	PayInterval = config.GetRangeInt("payInterval", 10000, 600000, 60000)
	SUM_USER_FEE = config.GetRangeInt("SUM_USER_FEE", 0, 90, 0)
	pledgeSpaceFeeStr := config.GetString("PLEDGE_SPACE_FEE", "")
	levelInfo := strings.Split(pledgeSpaceFeeStr, "|")
	if len(levelInfo) == 0 {
		log.Panicf("The 'PLEDGE_SPACE_FEE' parameter parses failed.\n")
	}
	PLEDGE_SPACE_FEE = make([]PledgeSpaceFee, len(levelInfo))
	for n, info := range levelInfo {
		levelFee := strings.Split(info, ",")
		if len(levelFee) == 0 {
			log.Panicf("The 'PLEDGE_SPACE_FEE' parameter parses failed.\n")
		}
		PLEDGE_SPACE_FEE[n].Level, err = strconv.Atoi(levelFee[0])
		if err != nil {
			log.Panicf("The 'PLEDGE_SPACE_FEE' parameter parses failed.\n")
		}
		PLEDGE_SPACE_FEE[n].Fee, err = strconv.Atoi(levelFee[1])
		if err != nil {
			log.Panicf("The 'PLEDGE_SPACE_FEE' parameter parses failed.\n")
		}
	}
	PLEDGE_SPACE_UPDATE_INTERVAL = config.GetRangeInt("PLEDGE_SPACE_UPDATE_INTERVAL", 3600, 86400, 86400)
}
