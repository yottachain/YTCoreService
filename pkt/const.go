package pkt

import (
	"google.golang.org/protobuf/proto"
)

type MessageInitor func() proto.Message

var ID_CLASS_MAP = make(map[uint16]MessageInitor)
var CLASS_ID_MAP = make(map[string]uint16)

func init() {
	init_id_class()
	init_class_id()
}

func init_id_class() {
	ID_CLASS_MAP[0x1757] = func() proto.Message { return &DownloadShardReq{} }
	ID_CLASS_MAP[0x7a56] = func() proto.Message { return &DownloadShardResp{} }
	ID_CLASS_MAP[0x5913] = func() proto.Message { return &ErrorMessage{} }
	ID_CLASS_MAP[0xa583] = func() proto.Message { return &SpotCheckStatus{} }
	ID_CLASS_MAP[0x26d0] = func() proto.Message { return &SpotCheckTask{} }
	ID_CLASS_MAP[0x903a] = func() proto.Message { return &SpotCheckTaskList{} }
	ID_CLASS_MAP[0xc9a9] = func() proto.Message { return &StatusRepReq{} }
	ID_CLASS_MAP[0xfa09] = func() proto.Message { return &StatusRepResp{} }
	ID_CLASS_MAP[0x2cb0] = func() proto.Message { return &TaskList{} }
	ID_CLASS_MAP[0x8b4d] = func() proto.Message { return &TaskDispatchList{} }
	ID_CLASS_MAP[0x93e4] = func() proto.Message { return &MultiTaskOpResultRes{} }
	ID_CLASS_MAP[0x1b31] = func() proto.Message { return &TaskOpResultList{} }
	ID_CLASS_MAP[0x1978] = func() proto.Message { return &UploadShard2CResp{} }
	ID_CLASS_MAP[0xcb05] = func() proto.Message { return &UploadShardReq{} }
	ID_CLASS_MAP[0xe64f] = func() proto.Message { return &VoidResp{} }
	ID_CLASS_MAP[0x47fb] = func() proto.Message { return &AuthReq{} }
	ID_CLASS_MAP[0x1c45] = func() proto.Message { return &GetFileAuthReq{} }
	ID_CLASS_MAP[0x77dd] = func() proto.Message { return &GetFileAuthResp{} }
	ID_CLASS_MAP[0x6b4e] = func() proto.Message { return &UploadBlockAuthReq{} }
	ID_CLASS_MAP[0xe231] = func() proto.Message { return &DownloadBlockDBResp{} }
	ID_CLASS_MAP[0x4cf2] = func() proto.Message { return &DownloadObjectInitResp{} }
	ID_CLASS_MAP[0x35e6] = func() proto.Message { return &DownloadBlockInitResp3{} }
	ID_CLASS_MAP[0xf527] = func() proto.Message { return &DownloadBlockInitResp2{} }
	ID_CLASS_MAP[0x267e] = func() proto.Message { return &DownloadBlockInitResp{} }
	ID_CLASS_MAP[0x78eb] = func() proto.Message { return &PreAllocNodeResp{} }
	ID_CLASS_MAP[0x68d2] = func() proto.Message { return &RegUserRespV2{} }
	ID_CLASS_MAP[0x1d98] = func() proto.Message { return &UploadBlockDupResp{} }
	ID_CLASS_MAP[0x35bb] = func() proto.Message { return &UploadBlockEndResp{} }
	ID_CLASS_MAP[0x5893] = func() proto.Message { return &UploadBlockInitResp{} }
	ID_CLASS_MAP[0x014c] = func() proto.Message { return &UploadObjectInitResp{} }
	ID_CLASS_MAP[0xc487] = func() proto.Message { return &GetNodeCapacityReq{} }
	ID_CLASS_MAP[0xe684] = func() proto.Message { return &GetNodeCapacityResp{} }
	ID_CLASS_MAP[0x67fc] = func() proto.Message { return &CopyObjectResp{} }
	ID_CLASS_MAP[0x43f2] = func() proto.Message { return &GetBucketResp{} }
	ID_CLASS_MAP[0x85a7] = func() proto.Message { return &GetObjectResp{} }
	ID_CLASS_MAP[0xc090] = func() proto.Message { return &ListBucketResp{} }
	ID_CLASS_MAP[0x06c5] = func() proto.Message { return &ListObjectResp{} }
	ID_CLASS_MAP[0x276d] = func() proto.Message { return &ListObjectRespV2{} }
	ID_CLASS_MAP[0xe26d] = func() proto.Message { return &StringMap{} }
	ID_CLASS_MAP[0xd09e] = func() proto.Message { return &CopyObjectReqV2{} }
	ID_CLASS_MAP[0xdbc2] = func() proto.Message { return &CreateBucketReqV2{} }
	ID_CLASS_MAP[0xd6f3] = func() proto.Message { return &DeleteBucketReqV2{} }
	ID_CLASS_MAP[0x4076] = func() proto.Message { return &DeleteFileReqV2{} }
	ID_CLASS_MAP[0x3288] = func() proto.Message { return &GetBucketReqV2{} }
	ID_CLASS_MAP[0x0d8e] = func() proto.Message { return &GetObjectReqV2{} }
	ID_CLASS_MAP[0xfd39] = func() proto.Message { return &ListBucketReqV2{} }
	ID_CLASS_MAP[0xc23f] = func() proto.Message { return &ListObjectReqV2{} }
	ID_CLASS_MAP[0xde6c] = func() proto.Message { return &UpdateBucketReqV2{} }
	ID_CLASS_MAP[0x48bf] = func() proto.Message { return &UploadFileReqV2{} }
	ID_CLASS_MAP[0x775e] = func() proto.Message { return &ActiveCacheV2{} }
	ID_CLASS_MAP[0xe66e] = func() proto.Message { return &DownloadBlockInitReqV2{} }
	ID_CLASS_MAP[0xbef5] = func() proto.Message { return &DownloadFileReqV2{} }
	ID_CLASS_MAP[0x75c5] = func() proto.Message { return &DownloadObjectInitReqV2{} }
	ID_CLASS_MAP[0x1d20] = func() proto.Message { return &PreAllocNodeReqV2{} }
	ID_CLASS_MAP[0x3868] = func() proto.Message { return &RegUserReqV3{} }
	ID_CLASS_MAP[0x9517] = func() proto.Message { return &UploadBlockDBReqV2{} }
	ID_CLASS_MAP[0x657e] = func() proto.Message { return &UploadBlockDupReqV2{} }
	ID_CLASS_MAP[0xbc17] = func() proto.Message { return &UploadBlockEndReqV2{} }
	ID_CLASS_MAP[0x7cd6] = func() proto.Message { return &UploadBlockEndReqV3{} }
	ID_CLASS_MAP[0x56cb] = func() proto.Message { return &CheckBlockDupReq{} }
	ID_CLASS_MAP[0xe299] = func() proto.Message { return &UploadBlockInitReqV2{} }
	ID_CLASS_MAP[0xa52b] = func() proto.Message { return &UploadObjectEndReqV2{} }
	ID_CLASS_MAP[0xf380] = func() proto.Message { return &UploadObjectInitReqV2{} }
}

func init_class_id() {
	CLASS_ID_MAP["DownloadShardReq"] = 0x1757
	CLASS_ID_MAP["DownloadShardResp"] = 0x7a56
	CLASS_ID_MAP["ErrorMessage"] = 0x5913
	CLASS_ID_MAP["SpotCheckStatus"] = 0xa583
	CLASS_ID_MAP["SpotCheckTask"] = 0x26d0
	CLASS_ID_MAP["SpotCheckTaskList"] = 0x903a
	CLASS_ID_MAP["StatusRepReq"] = 0xc9a9
	CLASS_ID_MAP["StatusRepResp"] = 0xfa09
	CLASS_ID_MAP["TaskList"] = 0x2cb0
	CLASS_ID_MAP["TaskDispatchList"] = 0x8b4d
	CLASS_ID_MAP["MultiTaskOpResultRes"] = 0x93e4
	CLASS_ID_MAP["TaskOpResultList"] = 0x1b31
	CLASS_ID_MAP["UploadShard2CResp"] = 0x1978
	CLASS_ID_MAP["UploadShardReq"] = 0xcb05
	CLASS_ID_MAP["VoidResp"] = 0xe64f
	CLASS_ID_MAP["AuthReq"] = 0x47fb
	CLASS_ID_MAP["GetFileAuthReq"] = 0x1c45
	CLASS_ID_MAP["GetFileAuthResp"] = 0x77dd
	CLASS_ID_MAP["UploadBlockAuthReq"] = 0x6b4e
	CLASS_ID_MAP["DownloadBlockDBResp"] = 0xe231
	CLASS_ID_MAP["DownloadObjectInitResp"] = 0x4cf2
	CLASS_ID_MAP["DownloadBlockInitResp3"] = 0x35e6
	CLASS_ID_MAP["DownloadBlockInitResp2"] = 0xf527
	CLASS_ID_MAP["DownloadBlockInitResp"] = 0x267e
	CLASS_ID_MAP["PreAllocNodeResp"] = 0x78eb
	CLASS_ID_MAP["RegUserRespV2"] = 0x68d2
	CLASS_ID_MAP["UploadBlockDupResp"] = 0x1d98
	CLASS_ID_MAP["UploadBlockEndResp"] = 0x35bb
	CLASS_ID_MAP["UploadBlockInitResp"] = 0x5893
	CLASS_ID_MAP["UploadObjectInitResp"] = 0x014c
	CLASS_ID_MAP["GetNodeCapacityReq"] = 0xc487
	CLASS_ID_MAP["GetNodeCapacityResp"] = 0xe684
	CLASS_ID_MAP["CopyObjectResp"] = 0x67fc
	CLASS_ID_MAP["GetBucketResp"] = 0x43f2
	CLASS_ID_MAP["GetObjectResp"] = 0x85a7
	CLASS_ID_MAP["ListBucketResp"] = 0xc090
	CLASS_ID_MAP["ListObjectResp"] = 0x06c5
	CLASS_ID_MAP["ListObjectRespV2"] = 0x276d
	CLASS_ID_MAP["StringMap"] = 0xe26d
	CLASS_ID_MAP["CopyObjectReqV2"] = 0xd09e
	CLASS_ID_MAP["CreateBucketReqV2"] = 0xdbc2
	CLASS_ID_MAP["DeleteBucketReqV2"] = 0xd6f3
	CLASS_ID_MAP["DeleteFileReqV2"] = 0x4076
	CLASS_ID_MAP["GetBucketReqV2"] = 0x3288
	CLASS_ID_MAP["GetObjectReqV2"] = 0x0d8e
	CLASS_ID_MAP["ListBucketReqV2"] = 0xfd39
	CLASS_ID_MAP["ListObjectReqV2"] = 0xc23f
	CLASS_ID_MAP["UpdateBucketReqV2"] = 0xde6c
	CLASS_ID_MAP["UploadFileReqV2"] = 0x48bf
	CLASS_ID_MAP["ActiveCacheV2"] = 0x775e
	CLASS_ID_MAP["DownloadBlockInitReqV2"] = 0xe66e
	CLASS_ID_MAP["DownloadFileReqV2"] = 0xbef5
	CLASS_ID_MAP["DownloadObjectInitReqV2"] = 0x75c5
	CLASS_ID_MAP["PreAllocNodeReqV2"] = 0x1d20
	CLASS_ID_MAP["RegUserReqV3"] = 0x3868
	CLASS_ID_MAP["UploadBlockDBReqV2"] = 0x9517
	CLASS_ID_MAP["UploadBlockDupReqV2"] = 0x657e
	CLASS_ID_MAP["UploadBlockEndReqV2"] = 0xbc17
	CLASS_ID_MAP["UploadBlockEndReqV3"] = 0x7cd6
	CLASS_ID_MAP["CheckBlockDupReq"] = 0x56cb
	CLASS_ID_MAP["UploadBlockInitReqV2"] = 0xe299
	CLASS_ID_MAP["UploadObjectEndReqV2"] = 0xa52b
	CLASS_ID_MAP["UploadObjectInitReqV2"] = 0xf380
}
