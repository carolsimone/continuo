package objectstore_test

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/xml"
	"fmt"
	"io"
	"maps"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/carolsimone/continuo/pkg/objectstore/objectstoretest"
)

// minPartSize is the size S3 requires of every part of a multipart upload but
// the last.
const minPartSize = 5 << 20

// fakeS3 emulates path-style S3 for this package's tests. It serves
// GetObject, PutObject, DeleteObject, ListObjectsV2, DeleteObjects and the
// four multipart calls (create, upload part, complete, abort) over objects
// keyed "<bucket>/<key>". A listing returns keys in order with their sizes and
// modification times, at most pageSize a page (every key when pageSize is 0),
// with the page's last key as the continuation token. DeleteObjects removes
// the requested keys except those in refuse, which it reports as per-key
// errors and keeps. A multipart upload is invisible until it completes, and
// completing it follows S3's rules: at least one part, parts in ascending
// order with the ETags the upload returned, every part but the last at least
// 5 MiB.
type fakeS3 struct {
	mu       sync.Mutex
	objects  map[string]fakeObject
	uploads  map[string]*fakeUpload
	pageSize int
	refuse   map[string]bool
	// failPart, when non-zero, is the part number whose upload the server
	// answers with a 500.
	failPart int
	lists    int
	batches  [][]string
	nextID   int
}

type fakeObject struct {
	body     []byte
	modified time.Time
}

type fakeUpload struct {
	bucket, key string
	parts       map[int][]byte
}

// newFakeS3 returns a fakeS3 holding a one-byte object at each of paths
// ("<bucket>/<key>").
func newFakeS3(pageSize int, paths ...string) *fakeS3 {
	f := &fakeS3{
		objects:  map[string]fakeObject{},
		uploads:  map[string]*fakeUpload{},
		pageSize: pageSize,
		refuse:   map[string]bool{},
	}
	for _, p := range paths {
		f.objects[p] = fakeObject{body: []byte("x"), modified: time.Now()}
	}
	return f
}

func (f *fakeS3) handle(w http.ResponseWriter, r *http.Request) {
	bucket, key, _ := strings.Cut(strings.TrimPrefix(r.URL.Path, "/"), "/")
	q := r.URL.Query()
	switch {
	case r.Method == http.MethodGet && q.Get("list-type") == "2":
		f.listObjects(w, bucket, q.Get("prefix"), q.Get("continuation-token"))
	case r.Method == http.MethodPost && q.Has("delete"):
		f.deleteObjects(w, r, bucket)
	case r.Method == http.MethodPost && q.Has("uploads"):
		f.createUpload(w, bucket, key)
	case r.Method == http.MethodPost && q.Has("uploadId"):
		f.completeUpload(w, r, bucket, key, q.Get("uploadId"))
	case r.Method == http.MethodPut && q.Has("uploadId"):
		f.uploadPart(w, r, q.Get("uploadId"), q.Get("partNumber"))
	case r.Method == http.MethodDelete && q.Has("uploadId"):
		f.abortUpload(w, r, q.Get("uploadId"))
	case r.Method == http.MethodDelete:
		f.deleteObject(w, bucket+"/"+key)
	case r.Method == http.MethodGet:
		f.getObject(w, r, bucket+"/"+key)
	case r.Method == http.MethodPut:
		f.putObject(w, r, bucket+"/"+key)
	default:
		http.Error(w, "unexpected request "+r.Method+" "+r.URL.String(), http.StatusBadRequest)
	}
}

func (f *fakeS3) getObject(w http.ResponseWriter, r *http.Request, path string) {
	f.mu.Lock()
	obj, ok := f.objects[path]
	f.mu.Unlock()
	if !ok {
		objectstoretest.NoSuchKey(w, r)
		return
	}
	objectstoretest.Object(obj.body)(w, r)
}

func (f *fakeS3) putObject(w http.ResponseWriter, r *http.Request, path string) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	f.mu.Lock()
	f.objects[path] = fakeObject{body: body, modified: time.Now()}
	f.mu.Unlock()
	w.WriteHeader(http.StatusOK)
}

func (f *fakeS3) deleteObject(w http.ResponseWriter, path string) {
	f.mu.Lock()
	delete(f.objects, path)
	f.mu.Unlock()
	w.WriteHeader(http.StatusNoContent)
}

func (f *fakeS3) listObjects(w http.ResponseWriter, bucket, prefix, token string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.lists++
	var page []string
	for _, p := range slices.Sorted(maps.Keys(f.objects)) {
		b, k, _ := strings.Cut(p, "/")
		if b == bucket && strings.HasPrefix(k, prefix) && k > token {
			page = append(page, k)
		}
	}
	truncated := f.pageSize > 0 && len(page) > f.pageSize
	if truncated {
		page = page[:f.pageSize]
	}
	var out strings.Builder
	fmt.Fprintf(&out, `<?xml version="1.0" encoding="UTF-8"?><ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Name>%s</Name><Prefix>%s</Prefix><KeyCount>%d</KeyCount><MaxKeys>1000</MaxKeys><IsTruncated>%t</IsTruncated>`, bucket, prefix, len(page), truncated)
	for _, k := range page {
		obj := f.objects[bucket+"/"+k]
		fmt.Fprintf(&out, `<Contents><Key>%s</Key><Size>%d</Size><LastModified>%s</LastModified></Contents>`,
			k, len(obj.body), obj.modified.UTC().Format("2006-01-02T15:04:05.000Z"))
	}
	if truncated {
		fmt.Fprintf(&out, `<NextContinuationToken>%s</NextContinuationToken>`, page[len(page)-1])
	}
	out.WriteString(`</ListBucketResult>`)
	w.Header().Set("Content-Type", "application/xml")
	_, _ = w.Write([]byte(out.String()))
}

func (f *fakeS3) deleteObjects(w http.ResponseWriter, r *http.Request, bucket string) {
	var req struct {
		Objects []struct {
			Key string `xml:"Key"`
		} `xml:"Object"`
	}
	body, err := io.ReadAll(r.Body)
	if err == nil {
		err = xml.Unmarshal(body, &req)
	}
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	batch := make([]string, 0, len(req.Objects))
	var failures strings.Builder
	for _, o := range req.Objects {
		batch = append(batch, o.Key)
		if f.refuse[bucket+"/"+o.Key] {
			fmt.Fprintf(&failures, `<Error><Key>%s</Key><Code>AccessDenied</Code><Message>Access Denied</Message></Error>`, o.Key)
			continue
		}
		delete(f.objects, bucket+"/"+o.Key)
	}
	f.batches = append(f.batches, batch)
	w.Header().Set("Content-Type", "application/xml")
	_, _ = w.Write([]byte(`<?xml version="1.0" encoding="UTF-8"?><DeleteResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">` + failures.String() + `</DeleteResult>`))
}

func (f *fakeS3) createUpload(w http.ResponseWriter, bucket, key string) {
	f.mu.Lock()
	f.nextID++
	id := "upload-" + strconv.Itoa(f.nextID)
	f.uploads[id] = &fakeUpload{bucket: bucket, key: key, parts: map[int][]byte{}}
	f.mu.Unlock()
	writeXML(w, initiateMultipartUploadResult{Bucket: bucket, Key: key, UploadID: id})
}

// initiateMultipartUploadResult is the body S3 answers CreateMultipartUpload
// with.
type initiateMultipartUploadResult struct {
	XMLName  xml.Name `xml:"http://s3.amazonaws.com/doc/2006-03-01/ InitiateMultipartUploadResult"`
	Bucket   string   `xml:"Bucket"`
	Key      string   `xml:"Key"`
	UploadID string   `xml:"UploadId"`
}

// completeMultipartUploadResult is the body S3 answers
// CompleteMultipartUpload with.
type completeMultipartUploadResult struct {
	XMLName xml.Name `xml:"http://s3.amazonaws.com/doc/2006-03-01/ CompleteMultipartUploadResult"`
	Bucket  string   `xml:"Bucket"`
	Key     string   `xml:"Key"`
	ETag    string   `xml:"ETag"`
}

// writeXML answers with v marshalled as an XML document, so the bucket and
// key a request named are escaped in it.
func writeXML(w http.ResponseWriter, v any) {
	body, err := xml.Marshal(v)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/xml")
	_, _ = w.Write(append([]byte(xml.Header), body...))
}

func (f *fakeS3) uploadPart(w http.ResponseWriter, r *http.Request, id, partNumber string) {
	number, err := strconv.Atoi(partNumber)
	if err != nil || number < 1 || number > 10000 {
		objectstoretest.ErrorStatus(http.StatusBadRequest, "InvalidArgument")(w, r)
		return
	}
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	up, ok := f.uploads[id]
	if !ok {
		objectstoretest.ErrorStatus(http.StatusNotFound, "NoSuchUpload")(w, r)
		return
	}
	if number == f.failPart {
		objectstoretest.ErrorStatus(http.StatusInternalServerError, "InternalError")(w, r)
		return
	}
	up.parts[number] = body
	w.Header().Set("ETag", `"`+etag(body)+`"`)
	w.WriteHeader(http.StatusOK)
}

func (f *fakeS3) completeUpload(w http.ResponseWriter, r *http.Request, bucket, key, id string) {
	var req struct {
		Parts []struct {
			PartNumber int    `xml:"PartNumber"`
			ETag       string `xml:"ETag"`
		} `xml:"Part"`
	}
	body, err := io.ReadAll(r.Body)
	if err == nil {
		err = xml.Unmarshal(body, &req)
	}
	if err != nil || len(req.Parts) == 0 {
		objectstoretest.ErrorStatus(http.StatusBadRequest, "MalformedXML")(w, r)
		return
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	up, ok := f.uploads[id]
	if !ok || up.bucket != bucket || up.key != key {
		objectstoretest.ErrorStatus(http.StatusNotFound, "NoSuchUpload")(w, r)
		return
	}
	var assembled []byte
	last := 0
	for i, p := range req.Parts {
		stored, ok := up.parts[p.PartNumber]
		switch {
		case p.PartNumber <= last:
			objectstoretest.ErrorStatus(http.StatusBadRequest, "InvalidPartOrder")(w, r)
			return
		case !ok || strings.Trim(p.ETag, `"`) != etag(stored):
			objectstoretest.ErrorStatus(http.StatusBadRequest, "InvalidPart")(w, r)
			return
		case i < len(req.Parts)-1 && len(stored) < minPartSize:
			objectstoretest.ErrorStatus(http.StatusBadRequest, "EntityTooSmall")(w, r)
			return
		}
		last = p.PartNumber
		assembled = append(assembled, stored...)
	}
	f.objects[bucket+"/"+key] = fakeObject{body: assembled, modified: time.Now()}
	delete(f.uploads, id)
	writeXML(w, completeMultipartUploadResult{Bucket: bucket, Key: key, ETag: `"` + etag(assembled) + `"`})
}

func (f *fakeS3) abortUpload(w http.ResponseWriter, r *http.Request, id string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, ok := f.uploads[id]; !ok {
		objectstoretest.ErrorStatus(http.StatusNotFound, "NoSuchUpload")(w, r)
		return
	}
	delete(f.uploads, id)
	w.WriteHeader(http.StatusNoContent)
}

// etag is the opaque tag the fake returns for a part or an object: a digest of
// its content.
func etag(body []byte) string {
	sum := sha256.Sum256(body)
	return hex.EncodeToString(sum[:16])
}

// state returns the stored paths in order, the key batches DeleteObjects
// received, and the number of listings served.
func (f *fakeS3) state() (paths []string, batches [][]string, lists int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Sorted(maps.Keys(f.objects)), slices.Clone(f.batches), f.lists
}

// openUploads returns how many multipart uploads were created and neither
// completed nor aborted.
func (f *fakeS3) openUploads() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.uploads)
}

// dropUploads forgets every open multipart upload, as a store does when an
// operator or a lifecycle rule aborts them.
func (f *fakeS3) dropUploads() {
	f.mu.Lock()
	defer f.mu.Unlock()
	clear(f.uploads)
}

// setFailPart makes the server answer the upload of part number with a 500.
func (f *fakeS3) setFailPart(number int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.failPart = number
}
