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


	ID_HANDLER_MAP[0x1b31] = func() MessageEvent { return MessageEvent(&TaskOpResultListHandler{}) }

	ID_HANDLER_MAP[0xa583] = func() MessageEvent { return MessageEvent(&SpotCheckRepHandler{}) }

	ID_HANDLER_MAP[0x75c5] = func() MessageEvent { return MessageEvent(&DownloadObjectInitHandler{}) }
	ID_HANDLER_MAP[0xbef5] = func() MessageEvent { return MessageEvent(&DownloadFileHandler{}) }
	ID_HANDLER_MAP[0xe66e] = func() MessageEvent { return MessageEvent(&DownloadBlockInitHandler{}) }

	ID_HANDLER_MAP[0x76a8] = func() MessageEvent { return MessageEvent(&ListSuperNodeHandler{}) }
	ID_HANDLER_MAP[0x1d20] = func() MessageEvent { return MessageEvent(&PreAllocNodeHandler{}) }

	ID_HANDLER_MAP[0x3868] = func() MessageEvent { return MessageEvent(&RegUserV3Handler{}) }

	ID_HANDLER_MAP[0x48bf] = func() MessageEvent { return MessageEvent(&UploadFileHandler{}) }
	ID_HANDLER_MAP[0xd09e] = func() MessageEvent { return MessageEvent(&CopyObjectHandler{}) }
	ID_HANDLER_MAP[0x4076] = func() MessageEvent { return MessageEvent(&DeleteFileHandler{}) }
	ID_HANDLER_MAP[0x0d8e] = func() MessageEvent { return MessageEvent(&GetObjectHandler{}) }
	ID_HANDLER_MAP[0xc23f] = func() MessageEvent { return MessageEvent(&ListObjectHandler{}) }


	ID_HANDLER_MAP[0x47fb] = func() MessageEvent { return MessageEvent(&AuthHandler{}) }
	ID_HANDLER_MAP[0x1c45] = func() MessageEvent { return MessageEvent(&GetFileMetaHandler{}) }
	ID_HANDLER_MAP[0x6b4e] = func() MessageEvent { return MessageEvent(&UploadBlockAuthHandler{}) }

	ID_HANDLER_MAP[0x56cb] = func() MessageEvent { return MessageEvent(&CheckBlockDupHandler{}) }
	ID_HANDLER_MAP[0xe299] = func() MessageEvent { return MessageEvent(&UploadBlockInitHandler{}) }
	ID_HANDLER_MAP[0x9517] = func() MessageEvent { return MessageEvent(&UploadBlockDBHandler{}) }
	ID_HANDLER_MAP[0x657e] = func() MessageEvent { return MessageEvent(&UploadBlockDupHandler{}) }
	ID_HANDLER_MAP[0xbc17] = func() MessageEvent { return MessageEvent(&UploadBlockEndHandler{}) }
	ID_HANDLER_MAP[0x7cd6] = func() MessageEvent { return MessageEvent(&UploadBlockEndV3Handler{}) }

	ID_HANDLER_MAP[0xf380] = func() MessageEvent { return MessageEvent(&UploadObjectInitHandler{}) }
	ID_HANDLER_MAP[0xa52b] = func() MessageEvent { return MessageEvent(&UploadObjectEndHandler{}) }

}