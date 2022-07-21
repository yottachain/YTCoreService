package s3

import (
	"io"

	"github.com/aws/aws-sdk-go/aws/awserr"
)

const (
	DefaultBucketVersionKeys = 1000
)

type Object struct {
	Name     string
	Metadata map[string]string
	Size     int64
	Contents io.ReadCloser
	Hash     []byte
	Range    *ObjectRange

	VersionID      VersionID
	IsDeleteMarker bool
}

type ObjectList struct {
	CommonPrefixes []CommonPrefix
	Contents       []*Content
	IsTruncated    bool
	NextMarker     string
	prefixes       map[string]bool
}

func NewObjectList() *ObjectList {
	return &ObjectList{}
}

func (b *ObjectList) Add(item *Content) {
	b.Contents = append(b.Contents, item)
}

func (b *ObjectList) AddPrefix(prefix string) {
	if b.prefixes == nil {
		b.prefixes = map[string]bool{}
	} else if b.prefixes[prefix] {
		return
	}
	b.prefixes[prefix] = true
	b.CommonPrefixes = append(b.CommonPrefixes, CommonPrefix{Prefix: prefix})
}

type ObjectDeleteResult struct {
	IsDeleteMarker bool

	VersionID VersionID
}

type ListBucketVersionsPage struct {
	KeyMarker    string
	HasKeyMarker bool

	VersionIDMarker    VersionID
	HasVersionIDMarker bool
	MaxKeys            int64
}

type ListBucketPage struct {
	Marker    string
	HasMarker bool
	MaxKeys   int64
}

func (p ListBucketPage) IsEmpty() bool {
	return p == ListBucketPage{}
}

type PutObjectResult struct {
	VersionID VersionID
}

type Backend interface {
	ListBuckets(accesskey string) ([]BucketInfo, error)

	ListBucket(accesskey string, name string, prefix *Prefix, page ListBucketPage) (*ObjectList, error)

	CreateBucket(accesskey string, name string) error

	BucketExists(accesskey string, name string) (exists bool, err error)

	DeleteBucket(accesskey string, name string) error

	GetObject(accesskey string, bucketName, objectName string, rangeRequest *ObjectRangeRequest) (*Object, error)

	HeadObject(accesskey string, bucketName, objectName string) (*Object, error)

	DeleteObject(accesskey string, bucketName, objectName string) (ObjectDeleteResult, error)

	PutObject(accesskey string, bucketName, key string, meta map[string]string, input io.Reader, size int64) (PutObjectResult, error)

	DeleteMulti(accesskey string, bucketName string, objects ...string) (MultiDeleteResult, error)

	MultipartUpload(accesskey, bucketName, objectName string, partsPath []string, meta map[string]string) (PutObjectResult, []byte, error)
}

type VersionedBackend interface {
	VersioningConfiguration(accesskey string, bucket string) (VersioningConfiguration, error)

	SetVersioningConfiguration(accesskey string, bucket string, v VersioningConfiguration) error

	GetObjectVersion(accesskey string,
		bucketName, objectName string,
		versionID VersionID,
		rangeRequest *ObjectRangeRequest) (*Object, error)

	HeadObjectVersion(accesskey string, bucketName, objectName string, versionID VersionID) (*Object, error)

	DeleteObjectVersion(accesskey string, bucketName, objectName string, versionID VersionID) (ObjectDeleteResult, error)
	ListBucketVersions(accesskey string, bucketName string, prefix *Prefix, page *ListBucketVersionsPage) (*ListBucketVersionsResult, error)
}

func MergeMetadata(accesskey string, db Backend, bucketName string, objectName string, meta map[string]string) error {
	existingObj, err := db.GetObject(accesskey, bucketName, objectName, nil)
	if err != nil {
		if awsErr, ok := err.(awserr.Error); ok && awsErr.Code() != string(ErrNoSuchKey) {
			return err
		}
	}
	if existingObj != nil {
		for k, v := range existingObj.Metadata {
			if _, ok := meta[k]; !ok {
				meta[k] = v
			}
		}
	}
	return nil
}
