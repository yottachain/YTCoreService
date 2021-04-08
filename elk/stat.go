package elk

type ElkLog struct {
	GetTokenTimes int64
	UpShardTimes  int64
	Id 			  int32
	GetNodeTimes  uint32
}

type ElkBlockLog struct {
	InitTimes	int64
	EndTimes	int64
}
