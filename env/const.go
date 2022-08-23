package env

import "github.com/sirupsen/logrus"

const (
	PL2 = 256

	PMS           uint64 = 90
	PPC           uint64 = 1
	UnitCycleCost uint64 = 100000000 * PPC / 365
	UnitFirstCost uint64 = 100000000 * PMS / 365
	UnitSpace     uint64 = 1024 * 1024 * 1024

	CostSumCycle uint64 = PPC * 1 * 1000 * 60 * 60 * 24

	READFILE_BUF_SIZE     = 64 * 1024
	Max_Memory_Usage      = 1024 * 1024 * 10
	Compress_Reserve_Size = 16 * 1024
)

const SN_RETRY_WAIT = 5
const SN_RETRY_TIMES = 12 * 5

var ShardNumPerNode = 1
var Default_Block_Size int64 = 1024*1024*2 - 1 - 128
var Max_Shard_Count int64 = 128
var Default_PND int64 = 36
var PFL int64 = 16 * 1024
var LRCInit = 13

func initCodecArgs(config *Config) {
	pfl := config.GetRangeInt("PFL", 16, 1024*2, 16)
	if pfl%16 > 0 {
		logrus.Panicf("Invalid parameter value.PFL=%d\n", pfl)
	}
	PFL = int64(pfl) * 1024

	Max_Shard_Count = int64(config.GetRangeInt("Max_Shard_Count", 8, 128, 128))
	Default_PND = int64(config.GetRangeInt("PND", 8, 128, 36))

	Default_Block_Size = (PFL - 1) * Max_Shard_Count
	if Default_Block_Size%16 == 0 {
		Default_Block_Size = Default_Block_Size - 1
	}

	LRCInit = config.GetRangeInt("LRCInit", 1, 100, 13)

}
