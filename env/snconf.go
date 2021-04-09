package env

import (
	"log"
	"strings"
)

const BP_ENABLE bool = true
const SPOTCHECKNUM = 3

var STAT_SERVICE bool = true
var DE_DUPLICATION bool = true
var GC bool = false
var SPOTCHECK_ADDR string = ""
var REBUILD_ADDR string = ""
var SUM_USER_FEE int = 0

var StdLog string

var SuperNodeID int
var Port int
var Port2 int

var Space_factor int
var IsBackup int
var SelfIp string

var S3Version string
var LsCacheExpireTime int
var LsCachePageNum int
var LsCursorLimit int
var LsCacheMaxSize int
var PayInterval int

var HttpPort int
var HttpRemoteIp string

var EOSURI string
var EOSAPI string
var BPAccount string
var ShadowAccount string
var ShadowPriKey string
var ContractAccount string
var ContractOwnerD string

var MAX_HTTP_ROUTINE int32
var MAX_AYNC_ROUTINE int32
var MAX_SYNC_ROUTINE int32
var MAX_READ_ROUTINE int32
var MAX_WRITE_ROUTINE int32
var MAX_STAT_ROUTINE int32
var MAX_SUMFEE_ROUTINE int32 = 21
var MAX_DELBLK_ROUTINE int32 = 21
var MAX_AUTH_ROUTINE int32 = 21
var PER_USER_MAX_READ_ROUTINE int32
var SLOW_OP_TIMES int

var DelLogPath string = ""

func readSnProperties() {
	confpath := YTSN_HOME + "conf/server.properties"
	config, err := NewConfig(confpath)
	if err != nil {
		log.Panicf("[Init]No properties file could be found for ytfs service:%s\n", confpath)
	}
	config.SetSection(YTSN_ENV_SEC)
	LogLevel = config.GetString("logLevel", "trace,stdout")
	num, ok := config.HasIntValue("superNodeID")
	if !ok {
		log.Panicf("The 'superNodeID' parameter is not configured.\n")
	}
	SuperNodeID = num
	if SuperNodeID < 0 || SuperNodeID > 31 {
		log.Panicf("The 'superNodeID' parameter is not configured.\n")
	}
	Port = config.GetRangeInt("port", -1, 20000, 9999)
	Port2 = config.GetRangeInt("port2", -1, 20000, 9998)

	DE_DUPLICATION = config.GetBool("DE_DUPLICATION", true)
	GC = config.GetBool("GC", false)
	STAT_SERVICE = config.GetBool("STAT_SERVICE", true)
	SPOTCHECK_ADDR = config.GetString("SPOTCHECK_ADDR", "")
	REBUILD_ADDR = config.GetString("REBUILD_ADDR", "")
	SUM_USER_FEE = config.GetRangeInt("SUM_USER_FEE", 0, 90, 0)

	StdLog = config.GetUpperString("stdLog", "")

	IsBackup = config.GetRangeInt("isBackup", 0, 1, 0)
	SelfIp = config.GetString("selfIp", "")

	S3Version = config.GetString("s3Version", "")
	ShardNumPerNode = config.GetRangeInt("shardNumPerNode", 1, 200, 1)
	Space_factor = config.GetRangeInt("space_factor", 0, 100, 100)

	HttpPort = config.GetRangeInt("httpPort", 8000, 20000, 8082)
	HttpRemoteIp = config.GetString("httpRemoteIp", "")

	EOSURI = config.GetString("eosURI", "")
	if EOSURI == "" {
		log.Panicf("The 'eosURI' parameter is not configured.\n")
	}
	//EOSAPI = config.GetString("eosAPI", "http://yts3api.yottachain.net:8888/v1/history/get_key_accounts")
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

	LsCacheExpireTime = config.GetRangeInt("lsCacheExpireTime", 5, 60*5, 30)
	LsCachePageNum = config.GetRangeInt("lsCachePageNum", 1, 100, 10)
	LsCursorLimit = config.GetRangeInt("lsCursorLimit", 0, 5, 1)
	LsCacheMaxSize = config.GetRangeInt("lsCacheMaxSize", 1000, 500000, 20000)
	PayInterval = config.GetRangeInt("payInterval", 10000, 600000, 60000)

	MAX_DELBLK_ROUTINE = int32(config.GetRangeInt("MAX_DELBLK_ROUTINE", 21, 21*10, 21))
	MAX_AYNC_ROUTINE = int32(config.GetRangeInt("MAX_AYNC_ROUTINE", 500, 5000, 2000))
	MAX_HTTP_ROUTINE = int32(config.GetRangeInt("MAX_HTTP_ROUTINE", 500, 2000, 1000))
	MAX_WRITE_ROUTINE = int32(config.GetRangeInt("MAX_WRITE_ROUTINE", 500, 5000, 2000))
	MAX_SYNC_ROUTINE = int32(config.GetRangeInt("MAX_SYNC_ROUTINE", 200, 3000, 2000))
	MAX_READ_ROUTINE = int32(config.GetRangeInt("MAX_READ_ROUTINE", 200, 2000, 1000))
	MAX_STAT_ROUTINE = int32(config.GetRangeInt("MAX_STAT_ROUTINE", 200, 2000, 1000))
	PER_USER_MAX_READ_ROUTINE = int32(config.GetRangeInt("PER_USER_MAX_READ_ROUTINE", 1, 20, 5))
	SLOW_OP_TIMES = config.GetRangeInt("SLOW_OP_TIMES", 10, 200, 50)

	Conntimeout = config.GetRangeInt("P2PHOST_CONNECTTIMEOUT", 1000, 60000, 15000)
	DirectConntimeout = CheckInt(Conntimeout/10, 500, 5000)
	Writetimeout = config.GetRangeInt("P2PHOST_WRITETIMEOUT", 1000, 60000, 15000)
	DirectWritetimeout = CheckInt(Writetimeout/10, 500, 5000)

	DelLogPath = config.GetString("DelLogPath", "")
	if !strings.HasSuffix(DelLogPath, "/") {
		DelLogPath = DelLogPath + "/"
	}
}
