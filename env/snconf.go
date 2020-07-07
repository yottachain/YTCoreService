package env

import (
	"log"
	"strconv"
	"strings"
)

const BP_ENABLE bool = true
const SPOTCHECKNUM = 3

var DE_DUPLICATION bool = true
var SPOTCHECK_ADDR string = ""
var REBUILD_ADDR string = ""

var ServerLogLevel string
var nodemgrLog string

var SuperNodeID int
var Port int

var Space_factor int
var IsBackup int
var SelfIp string

var ShardNumPerNode int

var S3Version string
var LsCacheExpireTime int
var LsCachePageNum int
var LsCursorLimit int
var LsCacheMaxSize int
var LsShardInterval int

var HttpPort int
var HttpRemoteIp string

var EOSURI string
var BPAccount string
var ShadowAccount string
var ShadowPriKey string
var ContractAccount string
var ContractOwnerD string

var MAX_AYNC_ROUTINE int32
var MAX_READ_ROUTINE int32
var MAX_WRITE_ROUTINE int32
var MAX_STAT_ROUTINE int32
var PER_USER_MAX_READ_ROUTINE int32
var SLOW_OP_TIMES int

func readSnProperties() {
	confpath := YTSN_HOME + "conf/server.properties"
	config := ReadConfig(confpath)
	var err error
	ServerLogLevel = strings.Trim(config["logLevel"], " ")
	SuperNodeID, err = strconv.Atoi(strings.Trim(config["superNodeID"], " "))
	if err != nil {
		log.Panicf("The 'superNodeID' parameter is not configured.\n")
	}
	if SuperNodeID < 0 || SuperNodeID > 31 {
		log.Panicf("The 'superNodeID' parameter is not configured.\n")
	}
	ss := strings.ToUpper(strings.Trim(config["DE_DUPLICATION"], " "))
	if ss == "FALSE" {
		DE_DUPLICATION = false
	}
	SPOTCHECK_ADDR = strings.Trim(config["SPOTCHECK_ADDR"], " ")
	REBUILD_ADDR = strings.Trim(config["REBUILD_ADDR"], " ")
	ss = strings.ToUpper(strings.Trim(config["nodemgrLog"], " "))
	Port = StringToInt(config["port"], 8888, 9999, 9999)
	HttpPort = StringToInt(config["httpPort"], 8000, 12000, 8082)
	Space_factor = StringToInt(config["space_factor"], 0, 100, 100)
	IsBackup = StringToInt(config["isBackup"], 0, 1, 0)
	SelfIp = strings.Trim(config["selfIp"], " ")
	S3Version = strings.Trim(config["s3Version"], " ")
	ShardNumPerNode = StringToInt(config["shardNumPerNode"], 1, 200, 8)
	LsCacheExpireTime = StringToInt(config["lsCacheExpireTime"], 5, 60*5, 30)
	LsCachePageNum = StringToInt(config["lsCachePageNum"], 1, 100, 10)
	LsCursorLimit = StringToInt(config["lsCursorLimit"], 0, 5, 1)
	LsCacheMaxSize = StringToInt(config["lsCacheMaxSize"], 1000, 500000, 20000)
	LsShardInterval = StringToInt(config["lsShardInterval"], 1, 10, 1)
	HttpRemoteIp = strings.Trim(config["httpRemoteIp"], " ")
	EOSURI = strings.Trim(config["eosURI"], " ")
	if EOSURI == "" {
		log.Panicf("The 'eosURI' parameter is not configured.\n")
	}
	BPAccount = strings.Trim(config["BPAccount"], " ")
	if BPAccount == "" {
		log.Panicf("The 'BPAccount' parameter is not configured.\n")
	}
	ShadowAccount = strings.Trim(config["ShadowAccount"], " ")
	if ShadowAccount == "" {
		log.Panicf("The 'ShadowAccount' parameter is not configured.\n")
	}
	ShadowPriKey = strings.Trim(config["ShadowPriKey"], " ")
	if ShadowPriKey == "" {
		log.Panicf("The 'ShadowPriKey' parameter is not configured.\n")
	}
	ContractAccount = strings.Trim(config["contractAccount"], " ")
	if ContractAccount == "" {
		log.Panicf("The 'contractAccount' parameter is not configured.\n")
	}
	ContractOwnerD = strings.Trim(config["contractOwnerD"], " ")
	if ContractOwnerD == "" {
		log.Panicf("The 'contractOwnerD' parameter is not configured.\n")
	}
	MAX_AYNC_ROUTINE = int32(StringToInt(config["MAX_AYNC_ROUTINE"], 200, 2000, 1000))
	MAX_READ_ROUTINE = int32(StringToInt(config["MAX_READ_ROUTINE"], 200, 2000, 1000))
	MAX_WRITE_ROUTINE = int32(StringToInt(config["MAX_WRITE_ROUTINE"], 500, 5000, 2000))
	MAX_STAT_ROUTINE = int32(StringToInt(config["MAX_STAT_ROUTINE"], 200, 2000, 1000))
	PER_USER_MAX_READ_ROUTINE = int32(StringToInt(config["PER_USER_MAX_READ_ROUTINE"], 1, 20, 5))
	SLOW_OP_TIMES = StringToInt(config["SLOW_OP_TIMES"], 10, 200, 50)
}
