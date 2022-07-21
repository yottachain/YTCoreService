package s3

import (
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"io"
	"math/big"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/ryszard/goskiplist/skiplist"
	"github.com/sirupsen/logrus"
	"github.com/yottachain/YTCoreService/env"
)

var add1 = new(big.Int).SetInt64(1)

type bucketUploads struct {
	uploads map[UploadID]*multipartUpload

	objectIndex *skiplist.SkipList
}

func newBucketUploads() *bucketUploads {
	return &bucketUploads{
		uploads:     map[UploadID]*multipartUpload{},
		objectIndex: skiplist.NewStringMap(),
	}
}

func (bu *bucketUploads) add(mpu *multipartUpload) {
	bu.uploads[mpu.ID] = mpu

	uploads, ok := bu.objectIndex.Get(mpu.Object)
	if !ok {
		uploads = []*multipartUpload{mpu}
	} else {
		uploads = append(uploads.([]*multipartUpload), mpu)
	}
	bu.objectIndex.Set(mpu.Object, uploads)
}

func (bu *bucketUploads) remove(uploadID UploadID) {
	upload := bu.uploads[uploadID]
	delete(bu.uploads, uploadID)

	var uploads []*multipartUpload
	{
		upv, ok := bu.objectIndex.Get(upload.Object)
		if !ok || upv == nil {
			return
		}
		uploads = upv.([]*multipartUpload)
	}
	var found = -1
	var v *multipartUpload
	for found, v = range uploads {
		if v.ID == uploadID {
			break
		}
	}

	if found >= 0 {
		uploads = append(uploads[:found], uploads[found+1:]...)
	}

	if len(uploads) == 0 {
		bu.objectIndex.Delete(upload.Object)
	} else {
		bu.objectIndex.Set(upload.Object, uploads)
	}
}

type uploader struct {
	uploadID *big.Int

	buckets map[string]*bucketUploads
	mu      sync.Mutex
}

func newUploader() *uploader {
	return &uploader{
		buckets:  make(map[string]*bucketUploads),
		uploadID: new(big.Int),
	}
}

func (u *uploader) Begin(bucket, object string, meta map[string]string, initiated time.Time) *multipartUpload {
	u.mu.Lock()
	defer u.mu.Unlock()
	u.uploadID.Add(u.uploadID, add1)
	mpu := &multipartUpload{
		ID:        UploadID(u.uploadID.String()),
		Bucket:    bucket,
		Object:    object,
		Meta:      meta,
		Initiated: initiated,
	}
	bucketUploads := u.buckets[bucket]
	if bucketUploads == nil {
		u.buckets[bucket] = newBucketUploads()
		bucketUploads = u.buckets[bucket]
	}
	bucketUploads.add(mpu)
	return mpu
}

func (u *uploader) ListParts(bucket, object string, uploadID UploadID, marker int, limit int64) (*ListMultipartUploadPartsResult, error) {
	u.mu.Lock()
	defer u.mu.Unlock()
	mpu, err := u.getUnlocked(bucket, object, uploadID)
	if err != nil {
		return nil, err
	}
	var result = ListMultipartUploadPartsResult{
		Bucket:           bucket,
		Key:              object,
		UploadID:         uploadID,
		MaxParts:         limit,
		PartNumberMarker: marker,
		StorageClass:     "STANDARD",
	}
	var cnt int64
	for partNumber, part := range mpu.parts[marker:] {
		if part == nil {
			continue
		}
		if cnt >= limit {
			result.IsTruncated = true
			result.NextPartNumberMarker = partNumber
			break
		}
		result.Parts = append(result.Parts, ListMultipartUploadPartItem{
			ETag:         part.ETag,
			Size:         part.Length,
			PartNumber:   partNumber,
			LastModified: part.LastModified,
		})
		cnt++
	}

	return &result, nil
}

func (u *uploader) List(bucket string, marker *UploadListMarker, prefix Prefix, limit int64) (*ListMultipartUploadsResult, error) {
	u.mu.Lock()
	defer u.mu.Unlock()

	bucketUploads, ok := u.buckets[bucket]
	if !ok {
		return nil, ErrNoSuchUpload
	}
	var result = ListMultipartUploadsResult{
		Bucket:     bucket,
		Delimiter:  prefix.Delimiter,
		Prefix:     prefix.Prefix,
		MaxUploads: limit,
	}
	var firstFound = true
	var iter = NewIterator(bucketUploads.objectIndex.Iterator())
	if marker != nil {
		iter.Seek(marker.Object)
		firstFound = marker.UploadID == ""
		result.UploadIDMarker = marker.UploadID
		result.KeyMarker = marker.Object
	}
	var truncated bool
	var cnt int64
	var seenPrefixes = map[string]bool{}
	var match PrefixMatch

	for iter.Next() {
		object := iter.Key().(string)
		uploads := iter.Value().([]*multipartUpload)

	retry:
		matched := prefix.Match(object, &match)
		if !matched {
			continue
		}

		if !firstFound {
			for idx, mpu := range uploads {
				if mpu.ID == marker.UploadID {
					firstFound = true
					uploads = uploads[idx:]
					goto retry
				}
			}

		} else {
			if match.CommonPrefix {
				if !seenPrefixes[match.MatchedPart] {
					result.CommonPrefixes = append(result.CommonPrefixes, match.AsCommonPrefix())
					seenPrefixes[match.MatchedPart] = true
				}

			} else {
				for idx, upload := range uploads {
					result.Uploads = append(result.Uploads, ListMultipartUploadItem{
						StorageClass: "STANDARD",
						Key:          object,
						UploadID:     upload.ID,
						Initiated:    ContentTime{Time: upload.Initiated},
					})

					cnt++
					if cnt >= limit {
						if idx != len(uploads)-1 {
							truncated = true
							result.NextUploadIDMarker = uploads[idx+1].ID
							result.NextKeyMarker = object
						}
						goto done
					}
				}
			}
		}
	}

done:
	if !truncated {
		for iter.Next() {
			object := iter.Key().(string)
			if matched := prefix.Match(object, &match); matched && !match.CommonPrefix {
				truncated = true
				result.NextUploadIDMarker = iter.Value().([]*multipartUpload)[0].ID
				result.NextKeyMarker = object
				break
			}
		}
	}

	result.IsTruncated = truncated

	return &result, nil
}

func (u *uploader) Complete(bucket, object string, id UploadID) (*multipartUpload, error) {
	u.mu.Lock()
	defer u.mu.Unlock()
	up, err := u.getUnlocked(bucket, object, id)
	if err != nil {
		return nil, err
	}
	u.buckets[bucket].remove(id)
	return up, nil
}

func (u *uploader) Get(bucket, object string, id UploadID) (mu *multipartUpload, err error) {
	u.mu.Lock()
	defer u.mu.Unlock()
	return u.getUnlocked(bucket, object, id)
}

func (u *uploader) getUnlocked(bucket, object string, id UploadID) (mu *multipartUpload, err error) {
	bucketUps, ok := u.buckets[bucket]
	if !ok {
		return nil, ErrNoSuchUpload
	}
	mu, ok = bucketUps.uploads[id]
	if !ok {
		return nil, ErrNoSuchUpload
	}
	if mu.Bucket != bucket || mu.Object != object {
		return nil, ErrNoSuchUpload
	}
	return mu, nil
}

type UploadListMarker struct {
	Object   string
	UploadID UploadID
}

func uploadListMarkerFromQuery(q url.Values) *UploadListMarker {
	object := q.Get("key-marker")
	if object == "" {
		return nil
	}
	return &UploadListMarker{Object: object, UploadID: UploadID(q.Get("upload-id-marker"))}
}

type multipartUploadPart struct {
	PartNumber   int
	ETag         string
	Length       int64
	LastModified ContentTime
}

type multipartUpload struct {
	ID        UploadID
	Bucket    string
	Object    string
	Meta      map[string]string
	Initiated time.Time

	parts    []*multipartUploadPart
	rootpath string
	mu       sync.Mutex
}

func (mpu *multipartUpload) AddPart(partNumber int, at time.Time, rdr io.Reader, contentLength int64) (etag string, err error) {
	if int64(partNumber) > MaxUploadPartNumber {
		return "", ErrInvalidPart
	}
	mpu.mu.Lock()
	defer mpu.mu.Unlock()
	if mpu.rootpath == "" {
		id := time.Now().Format("200601021504")
		mpu.rootpath = env.GetS3Cache() + "/" + id + string(mpu.ID)
		_, err := os.Stat(mpu.rootpath)
		if err != nil {
			if !os.IsExist(err) {
				err = os.MkdirAll(mpu.rootpath, os.ModePerm)
				if err != nil {
					return "", err
				}
			} else {
				return "", err
			}
		}
	}
	path := fmt.Sprintf("%s/%d", mpu.rootpath, partNumber)
	etag, size, err3 := writeCacheFilePart(path, rdr)
	if err3 != nil {
		logrus.Errorf("[MultipartUpload]AddPart,write big file cache error:%s\n", err3)
		return "", err3
	}
	if size != contentLength {
		return "", ErrIncompleteBody
	}
	part := multipartUploadPart{
		PartNumber:   partNumber,
		Length:       size,
		ETag:         etag,
		LastModified: NewContentTime(at),
	}
	if partNumber >= len(mpu.parts) {
		mpu.parts = append(mpu.parts, make([]*multipartUploadPart, partNumber-len(mpu.parts)+1)...)
	}
	mpu.parts[partNumber] = &part
	return etag, nil
}

func (mpu *multipartUpload) Reassemble(input *CompleteMultipartUploadRequest) ([]string, error) {
	mpu.mu.Lock()
	defer mpu.mu.Unlock()
	mpuPartsLen := len(mpu.parts)
	if len(input.Parts) > mpuPartsLen {
		return nil, ErrInvalidPart
	}
	if !input.partsAreSorted() {
		return nil, ErrInvalidPartOrder
	}
	res := make([]string, len(input.Parts))
	for _, inPart := range input.Parts {
		if inPart.PartNumber >= mpuPartsLen || mpu.parts[inPart.PartNumber] == nil {
			return nil, ErrorMessagef(ErrInvalidPart, "unexpected part number %d in complete request", inPart.PartNumber)
		}
		upPart := mpu.parts[inPart.PartNumber]
		if strings.Trim(inPart.ETag, "\"") != strings.Trim(upPart.ETag, "\"") {
			return nil, ErrorMessagef(ErrInvalidPart, "unexpected part etag for number %d in complete request", inPart.PartNumber)
		}
		res[inPart.PartNumber] = fmt.Sprintf("%s/%d", mpu.rootpath, inPart.PartNumber)
	}
	return res, nil
}

func writeCacheFilePart(path string, input io.Reader) (string, int64, error) {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_TRUNC|os.O_CREATE, 0644)
	if err != nil {
		logrus.Errorf("[MultipartUpload]write cache err:%s,open files:%d\n", err)
		return "", 0, err
	}
	defer f.Close()
	hash := md5.New()
	readbuf := make([]byte, 8192)
	for {
		num, err := input.Read(readbuf)
		if err != nil && err != io.EOF {
			return "", 0, err
		}
		if num > 0 {
			bs := readbuf[0:num]
			f.Write(bs)
			hash.Write(bs)
		}
		if err != nil && err == io.EOF {
			break
		}
	}
	partEtag := fmt.Sprintf(`"%s"`, hex.EncodeToString(hash.Sum(nil)))
	return partEtag, int64(hash.Size()), nil
}
