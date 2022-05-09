package env

const YTSN_ENV_SEC = "YTSN"
const YTSN_ENV_BPLIST_SEC = "YTSN.BPLS"
const YTSN_ENV_MONGO_SEC = "YTSN.MONGO"

const PL2 = 256
const PFL = 16 * 1024
const PMS uint64 = 90
const PPC uint64 = 1
const UnitCycleCost uint64 = 100000000 * PPC / 365
const UnitFirstCost uint64 = 100000000 * PMS / 365
const UnitSpace uint64 = 1024 * 1024 * 1024
const CostSumCycle uint64 = PPC * 1 * 1000 * 60 * 60 * 24
const PCM uint64 = 16 * 1024
const PNF uint32 = 3

const Max_Shard_Count = 128
const Default_PND = 36

const READFILE_BUF_SIZE = 64 * 1024
const Max_Memory_Usage = 1024 * 1024 * 10
const Default_Block_Size = 1024*1024*2 - 1 - 128

const Compress_Reserve_Size = 16 * 1024

const SN_RETRY_WAIT = 5
const SN_RETRYTIMES = 12 * 5
const DN_RETRY_WAIT = 3
const CONN_EXPIRED = 60 * 2

var ShardNumPerNode int

var Conntimeout = 30000
var DirectConntimeout = 1000
var Writetimeout = 60000
var DirectWritetimeout = 1000
