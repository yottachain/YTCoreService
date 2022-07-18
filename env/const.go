package env

const (
	PL2                  = 256
	PFL                  = 16 * 1024
	PMS           uint64 = 90
	PPC           uint64 = 1
	UnitCycleCost uint64 = 100000000 * PPC / 365
	UnitFirstCost uint64 = 100000000 * PMS / 365
	UnitSpace     uint64 = 1024 * 1024 * 1024
	CostSumCycle  uint64 = PPC * 1 * 1000 * 60 * 60 * 24
	PCM           uint64 = 16 * 1024
	PNF           uint32 = 3

	Max_Shard_Count = 128
	Default_PND     = 36

	READFILE_BUF_SIZE  = 64 * 1024
	Max_Memory_Usage   = 1024 * 1024 * 10
	Default_Block_Size = 1024*1024*2 - 1 - 128

	Compress_Reserve_Size = 16 * 1024
)

const SN_RETRY_WAIT = 5
const SN_RETRY_TIMES = 12 * 5

var ShardNumPerNode = 1
