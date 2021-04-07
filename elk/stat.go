package elk

type ElkLog struct {
	GetTokenTimes int64
	UpShardTimes  int64
	Time 		  int64
}

type ElkBlockLog struct {
	InitTimes	int64
	EndTimes	int64
	Time 		int64
}
