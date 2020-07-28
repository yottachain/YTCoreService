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
const CostSumCycle uint64 = PPC * 7 * 1000 * 60 * 60 * 24
const PCM uint64 = 16 * 1024
const PNF uint32 = 3

const Max_Shard_Count = 128
const Default_PND = 36

const READFILE_BUF_SIZE = 64 * 1024
const Max_Memory_Usage = 1024 * 1024 * 6
const Default_Block_Size = 1024*1024*2 - 1 - 128

const Compress_Reserve_Size = 16 * 1024

const CONN_EXPIRED int64 = 60 * 3

const LRCMAXHANDLERS = 50

var ShardNumPerNode int
