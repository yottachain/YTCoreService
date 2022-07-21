package s3

import (
	"encoding/base64"
	"encoding/hex"
	"encoding/xml"
	"fmt"
	"io"
	"io/ioutil"
	"math"
	"net/http"
	"net/textproto"
	"net/url"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/sirupsen/logrus"
)

type Server struct {
	requestID uint64

	storage   Backend
	versioned VersionedBackend

	timeSource              TimeSource
	metadataSizeLimit       int
	integrityCheck          bool
	failOnUnimplementedPage bool
	autoBucket              bool
	uploader                *uploader
}

func NewS3(backend Backend) *Server {
	s3 := &Server{
		storage:           backend,
		metadataSizeLimit: DefaultMetadataSizeLimit,
		integrityCheck:    true,
		uploader:          newUploader(),
		requestID:         0,
	}
	s3.versioned, _ = backend.(VersionedBackend)
	if s3.timeSource == nil {
		s3.timeSource = DefaultTimeSource()
	}
	return s3
}

func (g *Server) nextRequestID() uint64 {
	return atomic.AddUint64(&g.requestID, 1)
}

func (g *Server) Server() http.Handler {
	return &withCORS{r: http.HandlerFunc(g.routeBase)}
}

func (g *Server) httpError(w http.ResponseWriter, r *http.Request, err error) {
	resp := EnsureErrorResponse(err, "")
	if resp.ErrorCode() == ErrInternal {
		logrus.Errorf("[S3]ERR:%s", err)
	}
	w.WriteHeader(resp.ErrorCode().Status())
	if r.Method != http.MethodHead {
		if err := g.xmlEncoder(w).Encode(resp); err != nil {
			logrus.Errorf("[S3]ERR:%s", err)
			return
		}
	}
}

func (g *Server) listBuckets(w http.ResponseWriter, r *http.Request) error {
	logrus.Debugln("[S3]LIST BUCKETS")
	accesskey, autherr := GetAccessKey(r)
	if autherr != nil {
		return autherr
	}
	buckets, err := g.storage.ListBuckets(accesskey)
	if err != nil {
		return err
	}
	s := &Storage{
		Xmlns:   "http://s3.amazonaws.com/doc/2006-03-01/",
		Buckets: buckets,
		Owner: &UserInfo{
			ID:          "fe7272ea58be830e56fe1663b10fafef",
			DisplayName: "YtS3Server",
		},
	}

	return g.xmlEncoder(w).Encode(s)
}

func (g *Server) listBucket(bucketName string, w http.ResponseWriter, r *http.Request) error {
	logrus.Debugln("[S3]LIST BUCKET")
	accesskey, autherr := GetAccessKey(r)
	if autherr != nil {
		return autherr
	}
	if err := g.ensureBucketExists(accesskey, bucketName); err != nil {
		return err
	}
	q := r.URL.Query()
	prefix := prefixFromQuery(q)
	page, err := listBucketPageFromQuery(q)
	if err != nil {
		return err
	}
	isVersion2 := q.Get("list-type") == "2"
	logrus.Infof("[S3]LIST BUCKET:%s,prefix:%s,page:%+v", bucketName, prefix, page)
	objects, err := g.storage.ListBucket(accesskey, bucketName, &prefix, page)
	if err != nil {
		if err == ErrInternalPageNotImplemented && !g.failOnUnimplementedPage {
			objects, err = g.storage.ListBucket(accesskey, bucketName, &prefix, ListBucketPage{})
			if err != nil {
				return err
			}
		} else if err == ErrInternalPageNotImplemented && g.failOnUnimplementedPage {
			return ErrNotImplemented
		} else {
			return err
		}
	}
	base := ListBucketResultBase{
		Xmlns:          "http://s3.amazonaws.com/doc/2006-03-01/",
		Name:           bucketName,
		CommonPrefixes: objects.CommonPrefixes,
		Contents:       objects.Contents,
		IsTruncated:    objects.IsTruncated,
		Delimiter:      prefix.Delimiter,
		Prefix:         prefix.Prefix,
		MaxKeys:        page.MaxKeys,
	}
	if !isVersion2 {
		var result = &ListBucketResult{
			ListBucketResultBase: base,
			Marker:               page.Marker,
		}
		if base.Delimiter != "" {
			result.NextMarker = objects.NextMarker
		}
		return g.xmlEncoder(w).Encode(result)
	} else {
		var result = &ListBucketResultV2{
			ListBucketResultBase: base,
			KeyCount:             int64(len(objects.CommonPrefixes) + len(objects.Contents)),
			StartAfter:           q.Get("start-after"),
			ContinuationToken:    q.Get("continuation-token"),
		}
		if objects.NextMarker != "" {
			result.NextContinuationToken = base64.URLEncoding.EncodeToString([]byte(objects.NextMarker))
		}
		if _, ok := q["fetch-owner"]; !ok {
			for _, v := range result.Contents {
				v.Owner = nil
			}
		}
		return g.xmlEncoder(w).Encode(result)
	}
}

func (g *Server) getBucketLocation(bucketName string, w http.ResponseWriter, r *http.Request) error {
	logrus.Debugln("[S3]GET BUCKET LOCATION")
	accesskey, autherr := GetAccessKey(r)
	if autherr != nil {
		return autherr
	}
	if err := g.ensureBucketExists(accesskey, bucketName); err != nil {
		return err
	}
	result := GetBucketLocation{
		Xmlns:              "http://s3.amazonaws.com/doc/2006-03-01/",
		LocationConstraint: "",
	}
	return g.xmlEncoder(w).Encode(result)
}

func (g *Server) listBucketVersions(bucketName string, w http.ResponseWriter, r *http.Request) error {
	if g.versioned == nil {
		return ErrNotImplemented
	}
	accesskey, autherr := GetAccessKey(r)
	if autherr != nil {
		return autherr
	}
	if err := g.ensureBucketExists(accesskey, bucketName); err != nil {
		return err
	}
	q := r.URL.Query()
	prefix := prefixFromQuery(q)
	page, err := listBucketVersionsPageFromQuery(q)
	if err != nil {
		return err
	}
	if page.HasVersionIDMarker {
		if page.VersionIDMarker == "" {
			return ErrorInvalidArgument("version-id-marker", "", "A version-id marker cannot be empty.")
		} else if !page.HasKeyMarker {
			return ErrorInvalidArgument("version-id-marker", "", "A version-id marker cannot be specified without a key marker.")
		}

	} else if page.HasKeyMarker && page.KeyMarker == "" {
		page = ListBucketVersionsPage{}
	}
	bucket, err := g.versioned.ListBucketVersions(accesskey, bucketName, &prefix, &page)
	if err != nil {
		return err
	}
	for _, ver := range bucket.Versions {
		if ver.GetVersionID() == "" {
			ver.setVersionID("null")
		}
	}
	return g.xmlEncoder(w).Encode(bucket)
}

func (g *Server) createBucket(bucket string, w http.ResponseWriter, r *http.Request) error {
	logrus.Infof("[S3]CREATE BUCKET:%s", bucket)
	accesskey, autherr := GetAccessKey(r)
	if autherr != nil {
		return autherr
	}
	if err := ValidateBucketName(bucket); err != nil {
		return err
	}
	if err := g.storage.CreateBucket(accesskey, bucket); err != nil {
		return err
	}
	w.Header().Set("Location", "/"+bucket)
	w.Write([]byte{})
	return nil
}

func (g *Server) deleteBucket(bucket string, w http.ResponseWriter, r *http.Request) error {
	logrus.Infof("[S3]DELETE BUCKET:%s", bucket)
	accesskey, autherr := GetAccessKey(r)
	if autherr != nil {
		return autherr
	}
	if err := g.ensureBucketExists(accesskey, bucket); err != nil {
		return err
	}
	if err := g.storage.DeleteBucket(accesskey, bucket); err != nil {
		return err
	}
	w.WriteHeader(http.StatusNoContent)
	return nil
}

func (g *Server) headBucket(bucket string, w http.ResponseWriter, r *http.Request) error {
	logrus.Debugf("[S3]HEAD BUCKET:%s", bucket)
	accesskey, autherr := GetAccessKey(r)
	if autherr != nil {
		return autherr
	}
	if err := g.ensureBucketExists(accesskey, bucket); err != nil {
		return err
	}
	w.Write([]byte{})
	return nil
}

func (g *Server) getObject(bucket, object string, versionID VersionID, w http.ResponseWriter, r *http.Request) error {
	logrus.Debugf("[S3]GET OBJECT:/%s/%s", bucket, object)
	accesskey, autherr := GetAccessKey(r)
	if autherr != nil {
		return autherr
	}
	if err := g.ensureBucketExists(accesskey, bucket); err != nil {
		return err
	}
	rnge, err := parseRangeHeader(r.Header.Get("Range"))
	if err != nil {
		return err
	}
	var obj *Object
	{
		if versionID == "" {
			obj, err = g.storage.GetObject(accesskey, bucket, object, rnge)
			if err != nil {
				return err
			}
		} else {
			if g.versioned == nil {
				return ErrNotImplemented
			}
			obj, err = g.versioned.GetObjectVersion(accesskey, bucket, object, versionID, rnge)
			if err != nil {
				return err
			}
		}
	}
	if obj == nil {
		logrus.Errorf("[S3]Unexpected nil object for key:/%s/%s", bucket, object)
		return ErrInternal
	}
	defer obj.Contents.Close()

	if err := g.writeGetOrHeadObjectResponse(obj, w, r); err != nil {
		return err
	}
	obj.Range.writeHeader(obj.Size, w)
	if _, err := io.Copy(w, obj.Contents); err != nil {
		return err
	}
	return nil
}

func (g *Server) writeGetOrHeadObjectResponse(obj *Object, w http.ResponseWriter, r *http.Request) error {
	if obj.IsDeleteMarker {
		w.Header().Set("x-amz-version-id", string(obj.VersionID))
		w.Header().Set("x-amz-delete-marker", "true")
		return KeyNotFound(obj.Name)
	}
	for mk, mv := range obj.Metadata {
		w.Header().Set(mk, mv)
	}
	if obj.VersionID != "" {
		w.Header().Set("x-amz-version-id", string(obj.VersionID))
	}
	etag := `"` + hex.EncodeToString(obj.Hash) + `"`
	w.Header().Set("ETag", etag)
	if r.Header.Get("If-None-Match") == etag {
		return ErrNotModified
	}
	w.Header().Set("Accept-Ranges", "bytes")
	return nil
}

func (g *Server) headObject(bucket, object string, versionID VersionID,
	w http.ResponseWriter, r *http.Request) error {
	logrus.Infof("[S3]HEAD OBJECT:/%s/%s", bucket, object)
	accesskey, autherr := GetAccessKey(r)
	if autherr != nil {
		return autherr
	}
	if err := g.ensureBucketExists(accesskey, bucket); err != nil {
		return err
	}
	obj, err := g.storage.HeadObject(accesskey, bucket, object)
	if err != nil {
		return err
	}
	if obj == nil {
		logrus.Errorf("[S3]Unexpected nil object for key:/%s/%s", bucket, object)
		return ErrInternal
	}
	defer obj.Contents.Close()

	if err := g.writeGetOrHeadObjectResponse(obj, w, r); err != nil {
		return err
	}
	w.Header().Set("Content-Length", fmt.Sprintf("%d", obj.Size))
	return nil
}

func (g *Server) createObjectBrowserUpload(bucket string, w http.ResponseWriter, r *http.Request) error {
	logrus.Debugln("[S3]CREATE OBJECT THROUGH BROWSER UPLOAD")
	accesskey, autherr := GetAccessKey(r)
	if autherr != nil {
		return autherr
	}
	if err := g.ensureBucketExists(accesskey, bucket); err != nil {
		return err
	}
	const _24MB = (1 << 20) * 24
	if err := r.ParseMultipartForm(_24MB); nil != err {
		return ErrMalformedPOSTRequest
	}
	keyValues := r.MultipartForm.Value["key"]
	if len(keyValues) != 1 {
		return ErrIncorrectNumberOfFilesInPostRequest
	}
	key := keyValues[0]
	logrus.Infof("[S3]CREATED OBJECT:/%s/%s", bucket, key)
	fileValues := r.MultipartForm.File["file"]
	if len(fileValues) != 1 {
		return ErrIncorrectNumberOfFilesInPostRequest
	}
	fileHeader := fileValues[0]

	infile, err := fileHeader.Open()
	if err != nil {
		return err
	}
	defer infile.Close()
	meta, err := metadataHeaders(r.MultipartForm.Value, g.timeSource.Now(), g.metadataSizeLimit)
	if err != nil {
		return err
	}
	if len(key) > KeySizeLimit {
		return ResourceError(ErrKeyTooLong, key)
	}
	rdr, err := newHashingReader(infile, "")
	if err != nil {
		return err
	}
	result, err := g.storage.PutObject(accesskey, bucket, key, meta, rdr, fileHeader.Size)
	if err != nil {
		return err
	}
	if result.VersionID != "" {
		w.Header().Set("x-amz-version-id", string(result.VersionID))
	}
	w.Header().Set("ETag", `"`+hex.EncodeToString(rdr.Sum(nil))+`"`)
	return nil
}

func (g *Server) createObject(bucket, object string, w http.ResponseWriter, r *http.Request) (err error) {
	logrus.Infof("[S3]CREATED OBJECT:/%s/%s", bucket, object)
	accesskey, autherr := GetAccessKey(r)
	if autherr != nil {
		return autherr
	}
	if err := g.ensureBucketExists(accesskey, bucket); err != nil {
		return err
	}
	meta, err := metadataHeaders(r.Header, g.timeSource.Now(), g.metadataSizeLimit)
	if err != nil {
		return err
	}
	if _, ok := meta["X-Amz-Copy-Source"]; ok {
		return g.copyObject(bucket, object, meta, w, r)
	}
	contentLength := r.Header.Get("Content-Length")
	if contentLength == "" {
		return ErrMissingContentLength
	}
	size, err := strconv.ParseInt(contentLength, 10, 64)
	if err != nil || size < 0 {
		w.WriteHeader(http.StatusBadRequest)
		return nil
	}
	if len(object) > KeySizeLimit {
		return ResourceError(ErrKeyTooLong, object)
	}
	var md5Base64 string
	if g.integrityCheck {
		md5Base64 = r.Header.Get("Content-MD5")
		if _, ok := r.Header[textproto.CanonicalMIMEHeaderKey("Content-MD5")]; ok && md5Base64 == "" {
			return ErrInvalidDigest
		}
	}
	var reader io.Reader
	if sha, ok := meta["X-Amz-Content-Sha256"]; ok && sha == "STREAMING-AWS4-HMAC-SHA256-PAYLOAD" {
		reader = newChunkedReader(r.Body)
		size, err = strconv.ParseInt(meta["X-Amz-Decoded-Content-Length"], 10, 64)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return nil
		}
	} else {
		reader = r.Body
	}
	rdr, err := newHashingReader(reader, md5Base64)
	defer r.Body.Close()
	if err != nil {
		return err
	}
	result, err := g.storage.PutObject(accesskey, bucket, object, meta, rdr, size)
	if err != nil {
		return err
	}
	if result.VersionID != "" {
		logrus.Infof("[S3]CREATED VERSION:/%s/%s/%s", bucket, object, result.VersionID)
		w.Header().Set("x-amz-version-id", string(result.VersionID))
	}
	w.Header().Set("ETag", `"`+hex.EncodeToString(rdr.Sum(nil))+`"`)

	return nil
}

func (g *Server) copyObject(bucket, object string, meta map[string]string, w http.ResponseWriter, r *http.Request) (err error) {
	accesskey, autherr := GetAccessKey(r)
	if autherr != nil {
		return autherr
	}
	if err := g.ensureBucketExists(accesskey, bucket); err != nil {
		return err
	}
	source := meta["X-Amz-Copy-Source"]
	logrus.Infof("[S3]COPY %s TO /%s/%s", source, bucket, object)
	if len(object) > KeySizeLimit {
		return ResourceError(ErrKeyTooLong, object)
	}
	parts := strings.SplitN(strings.TrimPrefix(source, "/"), "/", 2)
	srcBucket := parts[0]
	srcKey := strings.SplitN(parts[1], "?", 2)[0]

	srcKey, err = url.QueryUnescape(srcKey)
	if err != nil {
		return err
	}
	srcObj, err := g.storage.GetObject(accesskey, srcBucket, srcKey, nil)
	if err != nil {
		return err
	}
	if srcObj == nil {
		logrus.Errorf("[S3]Unexpected nil object for key:/%s/%s", bucket, object)
		return ErrInternal
	}
	defer srcObj.Contents.Close()
	for k, v := range srcObj.Metadata {
		if _, found := meta[k]; !found && k != "X-Amz-Acl" {
			meta[k] = v
		}
	}
	result, err := g.storage.PutObject(accesskey, bucket, object, meta, srcObj.Contents, srcObj.Size)
	if err != nil {
		return err
	}
	if srcObj.VersionID != "" {
		w.Header().Set("x-amz-copy-source-version-id", string(srcObj.VersionID))
	}
	if result.VersionID != "" {
		logrus.Infof("[S3]CREATED VERSION:%s/%s/%s", bucket, object, result.VersionID)
		w.Header().Set("x-amz-version-id", string(result.VersionID))
	}
	return g.xmlEncoder(w).Encode(CopyObjectResult{
		ETag:         `"` + hex.EncodeToString(srcObj.Hash) + `"`,
		LastModified: NewContentTime(g.timeSource.Now()),
	})
}

func (g *Server) deleteObject(bucket, object string, w http.ResponseWriter, r *http.Request) error {
	logrus.Infof("[S3]DELETED:/%s/%s", bucket, object)
	accesskey, autherr := GetAccessKey(r)
	if autherr != nil {
		return autherr
	}
	if err := g.ensureBucketExists(accesskey, bucket); err != nil {
		return err
	}
	result, err := g.storage.DeleteObject(accesskey, bucket, object)
	if err != nil {
		return err
	}
	if result.IsDeleteMarker {
		w.Header().Set("x-amz-delete-marker", "true")
	} else {
		w.Header().Set("x-amz-delete-marker", "false")
	}
	if result.VersionID != "" {
		w.Header().Set("x-amz-version-id", string(result.VersionID))
	}
	w.WriteHeader(http.StatusNoContent)
	return nil
}

func (g *Server) deleteObjectVersion(bucket, object string, version VersionID, w http.ResponseWriter, r *http.Request) error {
	if g.versioned == nil {
		return ErrNotImplemented
	}
	logrus.Infof("[S3]DELETED VERSION:/%s/%s/%s", bucket, object, version)
	accesskey, autherr := GetAccessKey(r)
	if autherr != nil {
		return autherr
	}
	if err := g.ensureBucketExists(accesskey, bucket); err != nil {
		return err
	}
	result, err := g.versioned.DeleteObjectVersion(accesskey, bucket, object, version)
	if err != nil {
		return err
	}

	if result.IsDeleteMarker {
		w.Header().Set("x-amz-delete-marker", "true")
	} else {
		w.Header().Set("x-amz-delete-marker", "false")
	}

	if result.VersionID != "" {
		w.Header().Set("x-amz-version-id", string(result.VersionID))
	}
	w.WriteHeader(http.StatusNoContent)
	return nil
}

func (g *Server) deleteMulti(bucket string, w http.ResponseWriter, r *http.Request) error {
	logrus.Infof("[S3]Delete multi:%s", bucket)
	accesskey, autherr := GetAccessKey(r)
	if autherr != nil {
		return autherr
	}
	if err := g.ensureBucketExists(accesskey, bucket); err != nil {
		return err
	}
	var in DeleteRequest
	defer r.Body.Close()
	dc := xml.NewDecoder(r.Body)
	if err := dc.Decode(&in); err != nil {
		return ErrorMessage(ErrMalformedXML, err.Error())
	}
	keys := make([]string, len(in.Objects))
	for i, o := range in.Objects {
		keys[i] = o.Key
	}
	out, err := g.storage.DeleteMulti(accesskey, bucket, keys...)
	if err != nil {
		return err
	}
	if in.Quiet {
		out.Deleted = nil
	}
	return g.xmlEncoder(w).Encode(out)
}

func (g *Server) initiateMultipartUpload(bucket, object string, w http.ResponseWriter, r *http.Request) error {
	logrus.Infof("[S3]Initiate multipart upload:/%s/%s", bucket, object)
	meta, err := metadataHeaders(r.Header, g.timeSource.Now(), g.metadataSizeLimit)
	if err != nil {
		return err
	}
	accesskey, autherr := GetAccessKey(r)
	if autherr != nil {
		return autherr
	}
	if err := g.ensureBucketExists(accesskey, bucket); err != nil {
		return err
	}
	upload := g.uploader.Begin(bucket, object, meta, g.timeSource.Now())
	out := InitiateMultipartUpload{
		UploadID: upload.ID,
		Bucket:   bucket,
		Key:      object,
	}
	return g.xmlEncoder(w).Encode(out)
}

func (g *Server) putMultipartUploadPart(bucket, object string, uploadID UploadID, w http.ResponseWriter, r *http.Request) error {
	logrus.Infof("[S3]Put multipart upload:/%s/%s/%s", bucket, object, uploadID)
	partNumber, err := strconv.ParseInt(r.URL.Query().Get("partNumber"), 10, 0)
	if err != nil || partNumber <= 0 || partNumber > MaxUploadPartNumber {
		return ErrInvalidPart
	}
	size, err := strconv.ParseInt(r.Header.Get("Content-Length"), 10, 64)
	if err != nil || size <= 0 {
		return ErrMissingContentLength
	}
	upload, err := g.uploader.Get(bucket, object, uploadID)
	if err != nil {
		return err
	}
	defer r.Body.Close()
	var rdr io.Reader = r.Body

	if g.integrityCheck {
		md5Base64 := r.Header.Get("Content-MD5")
		if _, ok := r.Header[textproto.CanonicalMIMEHeaderKey("Content-MD5")]; ok && md5Base64 == "" {
			return ErrInvalidDigest
		}
		if md5Base64 != "" {
			var err error
			rdr, err = newHashingReader(rdr, md5Base64)
			if err != nil {
				return err
			}
		}
	}
	etag, err := upload.AddPart(int(partNumber), g.timeSource.Now(), rdr, r.ContentLength)
	if err != nil {
		return err
	}
	w.Header().Add("ETag", etag)
	return nil
}

func (g *Server) abortMultipartUpload(bucket, object string, uploadID UploadID, w http.ResponseWriter, r *http.Request) error {
	logrus.Infof("[S3]Abort multipart upload:/%s/%s/%s", bucket, object, uploadID)
	if _, err := g.uploader.Complete(bucket, object, uploadID); err != nil {
		return err
	}
	w.WriteHeader(http.StatusNoContent)
	return nil
}

func (g *Server) completeMultipartUpload(bucket, object string, uploadID UploadID, w http.ResponseWriter, r *http.Request) error {
	logrus.Infof("[S3]Complete multipart upload:/%s/%s/%s", bucket, object, uploadID)
	accesskey, autherr := GetAccessKey(r)
	if autherr != nil {
		return autherr
	}
	var in CompleteMultipartUploadRequest
	if err := g.xmlDecodeBody(r.Body, &in); err != nil {
		return err
	}
	upload, err := g.uploader.Complete(bucket, object, uploadID)
	if err != nil {
		return err
	}
	files, err := upload.Reassemble(&in)
	if err != nil {
		return err
	}
	result, md5, err := g.storage.MultipartUpload(accesskey, bucket, object, files, upload.Meta)
	if err != nil {
		return err
	}
	etag := fmt.Sprintf("%x", md5)
	if result.VersionID != "" {
		w.Header().Set("x-amz-version-id", string(result.VersionID))
	}
	return g.xmlEncoder(w).Encode(&CompleteMultipartUploadResult{
		ETag:   etag,
		Bucket: bucket,
		Key:    object,
	})
}

func (g *Server) listMultipartUploads(bucket string, w http.ResponseWriter, r *http.Request) error {
	accesskey, autherr := GetAccessKey(r)
	if autherr != nil {
		return autherr
	}
	if err := g.ensureBucketExists(accesskey, bucket); err != nil {
		return err
	}
	query := r.URL.Query()
	prefix := prefixFromQuery(query)
	marker := uploadListMarkerFromQuery(query)
	maxUploads, err := parseClampedInt(query.Get("max-uploads"), DefaultMaxUploads, 0, MaxUploadsLimit)
	if err != nil {
		return ErrInvalidURI
	}
	if maxUploads == 0 {
		maxUploads = DefaultMaxUploads
	}
	out, err := g.uploader.List(bucket, marker, prefix, maxUploads)
	if err != nil {
		return err
	}
	return g.xmlEncoder(w).Encode(out)
}

func (g *Server) listMultipartUploadParts(bucket, object string, uploadID UploadID, w http.ResponseWriter, r *http.Request) error {
	accesskey, autherr := GetAccessKey(r)
	if autherr != nil {
		return autherr
	}
	if err := g.ensureBucketExists(accesskey, bucket); err != nil {
		return err
	}
	query := r.URL.Query()
	marker, err := parseClampedInt(query.Get("part-number-marker"), 0, 0, math.MaxInt64)
	if err != nil {
		return ErrInvalidURI
	}
	maxParts, err := parseClampedInt(query.Get("max-parts"), DefaultMaxUploadParts, 0, MaxUploadPartsLimit)
	if err != nil {
		return ErrInvalidURI
	}
	out, err := g.uploader.ListParts(bucket, object, uploadID, int(marker), maxParts)
	if err != nil {
		return err
	}
	return g.xmlEncoder(w).Encode(out)
}

func (g *Server) getBucketVersioning(bucket string, w http.ResponseWriter, r *http.Request) error {
	accesskey, autherr := GetAccessKey(r)
	if autherr != nil {
		return autherr
	}
	if err := g.ensureBucketExists(accesskey, bucket); err != nil {
		return err
	}
	var config VersioningConfiguration
	if g.versioned != nil {
		var err error
		config, err = g.versioned.VersioningConfiguration(accesskey, bucket)
		if err != nil {
			return err
		}
	}
	return g.xmlEncoder(w).Encode(config)
}

func (g *Server) putBucketVersioning(bucket string, w http.ResponseWriter, r *http.Request) error {
	accesskey, autherr := GetAccessKey(r)
	if autherr != nil {
		return autherr
	}
	if err := g.ensureBucketExists(accesskey, bucket); err != nil {
		return err
	}
	var in VersioningConfiguration
	if err := g.xmlDecodeBody(r.Body, &in); err != nil {
		return err
	}
	if g.versioned == nil {
		if in.MFADelete == MFADeleteEnabled || in.Status == VersioningEnabled {
			return ErrNotImplemented
		} else {
			return nil
		}
	}
	logrus.Infof("[S3]PUT VERSIONING:%s", in.Status)
	return g.versioned.SetVersioningConfiguration(accesskey, bucket, in)
}

func (g *Server) ensureBucketExists(accesskey string, bucket string) error {
	exists, err := g.storage.BucketExists(accesskey, bucket)
	if err != nil {
		return err
	}
	if !exists && g.autoBucket {
		if err := g.storage.CreateBucket(accesskey, bucket); err != nil {
			logrus.Errorf("[S3]Autobucket create failed:%s", err)
			return ResourceError(ErrNoSuchBucket, bucket)
		}
	} else if !exists {
		return ResourceError(ErrNoSuchBucket, bucket)
	}
	return nil
}

func (g *Server) xmlEncoder(w http.ResponseWriter) *xml.Encoder {
	w.Write([]byte(xml.Header))
	w.Header().Set("Content-Type", "application/xml")
	xe := xml.NewEncoder(w)
	xe.Indent("", "  ")
	return xe
}

func (g *Server) xmlDecodeBody(rdr io.ReadCloser, into interface{}) error {
	body, err := ioutil.ReadAll(rdr)
	defer rdr.Close()
	if err != nil {
		return err
	}
	if err := xml.Unmarshal(body, into); err != nil {
		return ErrorMessage(ErrMalformedXML, err.Error())
	}
	return nil
}

func formatHeaderTime(t time.Time) string {
	tc := t.In(time.UTC)
	return tc.Format("Mon, 02 Jan 2006 15:04:05") + " GMT"
}

func metadataSize(meta map[string]string) int {
	total := 0
	for k, v := range meta {
		total += len(k) + len(v)
	}
	return total
}

func metadataHeaders(headers map[string][]string, at time.Time, sizeLimit int) (map[string]string, error) {
	meta := make(map[string]string)
	for hk, hv := range headers {
		if strings.HasPrefix(hk, "X-Amz-") || hk == "Content-Type" || hk == "Content-Disposition" {
			meta[hk] = hv[0]
		}
	}
	meta["Last-Modified"] = formatHeaderTime(at)
	if sizeLimit > 0 && metadataSize(meta) > sizeLimit {
		return meta, ErrMetadataTooLarge
	}
	return meta, nil
}

func listBucketPageFromQuery(query url.Values) (page ListBucketPage, rerr error) {
	maxKeys, err := parseClampedInt(query.Get("max-keys"), DefaultMaxBucketKeys, 0, MaxBucketKeys)
	if err != nil {
		return page, err
	}
	page.MaxKeys = maxKeys
	if _, page.HasMarker = query["marker"]; page.HasMarker {
		page.Marker = query.Get("marker")
	} else if _, page.HasMarker = query["continuation-token"]; page.HasMarker {
		tok, err := base64.URLEncoding.DecodeString(query.Get("continuation-token"))
		if err != nil {
			return page, ErrInvalidToken
		}
		page.Marker = string(tok)
	} else if _, page.HasMarker = query["start-after"]; page.HasMarker {
		page.Marker = query.Get("start-after")
	}
	return page, nil
}

func listBucketVersionsPageFromQuery(query url.Values) (page ListBucketVersionsPage, rerr error) {
	maxKeys, err := parseClampedInt(query.Get("max-keys"), DefaultMaxBucketVersionKeys, 0, MaxBucketVersionKeys)
	if err != nil {
		return page, err
	}
	page.MaxKeys = maxKeys
	page.KeyMarker = query.Get("key-marker")
	page.VersionIDMarker = VersionID(query.Get("version-id-marker"))
	_, page.HasKeyMarker = query["key-marker"]
	_, page.HasVersionIDMarker = query["version-id-marker"]
	return page, nil
}
