package s3

import "time"

var (
	DefaultSkewLimit         = 15 * time.Minute
	DefaultMetadataSizeLimit = 2000
	KeySizeLimit             = 1024

	DefaultUploadPartSize = 5 * 1000 * 1000

	MaxUploadsLimit       int64 = 1000
	DefaultMaxUploads     int64 = 1000
	MaxUploadPartsLimit   int64 = 1000
	DefaultMaxUploadParts int64 = 1000

	MaxBucketKeys        int64 = 1000
	DefaultMaxBucketKeys int64 = 1000

	MaxBucketVersionKeys        int64 = 1000
	DefaultMaxBucketVersionKeys int64 = 1000

	MaxUploadPartNumber int64 = 10000
)
