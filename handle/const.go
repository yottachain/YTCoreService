package handle

type HandlerInitor func() MessageEvent

var ID_HANDLER_MAP = make(map[uint16]HandlerInitor)

func init() {
	ID_HANDLER_MAP[0xdbc2] = func() MessageEvent { return MessageEvent(&CreateBucketHandler{}) }
	ID_HANDLER_MAP[0x3288] = func() MessageEvent { return MessageEvent(&GetBucketHandler{}) }
	ID_HANDLER_MAP[0xd6f3] = func() MessageEvent { return MessageEvent(&DeleteBucketHandler{}) }
	ID_HANDLER_MAP[0xde6c] = func() MessageEvent { return MessageEvent(&UpdateBucketHandler{}) }
	ID_HANDLER_MAP[0xfd39] = func() MessageEvent { return MessageEvent(&ListBucketHandler{}) }

	ID_HANDLER_MAP[0xc9a9] = func() MessageEvent { return MessageEvent(&StatusRepHandler{}) }
	ID_HANDLER_MAP[0xa583] = func() MessageEvent { return MessageEvent(&SpotCheckRepHandler{}) }
	ID_HANDLER_MAP[0x1b31] = func() MessageEvent { return MessageEvent(&TaskOpResultListHandler{}) }

	ID_HANDLER_MAP[0x9edf] = func() MessageEvent { return MessageEvent(&NodeSyncHandler{}) }

	ID_HANDLER_MAP[0x75c5] = func() MessageEvent { return MessageEvent(&DownloadObjectInitHandler{}) }
	ID_HANDLER_MAP[0xbef5] = func() MessageEvent { return MessageEvent(&DownloadFileHandler{}) }
	ID_HANDLER_MAP[0xe66e] = func() MessageEvent { return MessageEvent(&DownloadBlockInitHandler{}) }

	ID_HANDLER_MAP[0x76a8] = func() MessageEvent { return MessageEvent(&ListSuperNodeHandler{}) }
	ID_HANDLER_MAP[0xf8a9] = func() MessageEvent { return MessageEvent(&RegUserHandler{}) }
	ID_HANDLER_MAP[0x197f] = func() MessageEvent { return MessageEvent(&QueryUserHandler{}) }
	ID_HANDLER_MAP[0x1d20] = func() MessageEvent { return MessageEvent(&PreAllocNodeHandler{}) }

	ID_HANDLER_MAP[0x48bf] = func() MessageEvent { return MessageEvent(&UploadFileHandler{}) }
	ID_HANDLER_MAP[0xd09e] = func() MessageEvent { return MessageEvent(&CopyObjectHandler{}) }
	ID_HANDLER_MAP[0x4076] = func() MessageEvent { return MessageEvent(&DeleteFileHandler{}) }
	ID_HANDLER_MAP[0x0d8e] = func() MessageEvent { return MessageEvent(&GetObjectHandler{}) }
	ID_HANDLER_MAP[0xc23f] = func() MessageEvent { return MessageEvent(&ListObjectHandler{}) }

	ID_HANDLER_MAP[0x71ae] = func() MessageEvent { return MessageEvent(&TotalHandler{}) }
	ID_HANDLER_MAP[0x78c3] = func() MessageEvent { return MessageEvent(&UserSpaceHandler{}) }
	ID_HANDLER_MAP[0x4d27] = func() MessageEvent { return MessageEvent(&UserListHandler{}) }
	ID_HANDLER_MAP[0xb172] = func() MessageEvent { return MessageEvent(&RelationshipHandler{}) }

	ID_HANDLER_MAP[0xd927] = func() MessageEvent { return MessageEvent(&BlockUsedSpaceHandler{}) }

	ID_HANDLER_MAP[0xe5e2] = func() MessageEvent { return MessageEvent(&RelationshipSumHandler{}) }

	ID_HANDLER_MAP[0xe299] = func() MessageEvent { return MessageEvent(&UploadBlockInitHandler{}) }
	ID_HANDLER_MAP[0x9517] = func() MessageEvent { return MessageEvent(&UploadBlockDBHandler{}) }
	ID_HANDLER_MAP[0x657e] = func() MessageEvent { return MessageEvent(&UploadBlockDupHandler{}) }
	ID_HANDLER_MAP[0xbc17] = func() MessageEvent { return MessageEvent(&UploadBlockEndHandler{}) }
	ID_HANDLER_MAP[0x5753] = func() MessageEvent { return MessageEvent(&UploadBlockEndSyncHandler{}) }

	ID_HANDLER_MAP[0xf380] = func() MessageEvent { return MessageEvent(&UploadObjectInitHandler{}) }
	ID_HANDLER_MAP[0x2e42] = func() MessageEvent { return MessageEvent(&SaveObjectMetaHandler{}) }
	ID_HANDLER_MAP[0x775e] = func() MessageEvent { return MessageEvent(&ActiveCacheHandler{}) }
	ID_HANDLER_MAP[0xa52b] = func() MessageEvent { return MessageEvent(&UploadObjectEndHandler{}) }

}
