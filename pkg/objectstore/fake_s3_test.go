package objectstore_test

import (
	"encoding/xml"
	"fmt"
	"io"
	"maps"
	"net/http"
	"slices"
	"strings"
	"sync"

	"github.com/carolsimone/continuo/pkg/objectstore/objectstoretest"
)

// fakeS3 emulates path-style S3 for this package's tests. It serves
// GetObject, PutObject, ListObjectsV2 and DeleteObjects over objects keyed
// "<bucket>/<key>". A listing returns keys in order, at most pageSize a page
// (every key when pageSize is 0), with the page's last key as the
// continuation token. DeleteObjects removes the requested keys except those
// in refuse, which it reports as per-key errors and keeps.
type fakeS3 struct {
	mu       sync.Mutex
	objects  map[string][]byte
	pageSize int
	refuse   map[string]bool
	lists    int
	batches  [][]string
}

// newFakeS3 returns a fakeS3 holding a one-byte object at each of paths
// ("<bucket>/<key>").
func newFakeS3(pageSize int, paths ...string) *fakeS3 {
	f := &fakeS3{objects: map[string][]byte{}, pageSize: pageSize, refuse: map[string]bool{}}
	for _, p := range paths {
		f.objects[p] = []byte("x")
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
	body, ok := f.objects[path]
	f.mu.Unlock()
	if !ok {
		objectstoretest.NoSuchKey(w, r)
		return
	}
	objectstoretest.Object(body)(w, r)
}

func (f *fakeS3) putObject(w http.ResponseWriter, r *http.Request, path string) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	f.mu.Lock()
	f.objects[path] = body
	f.mu.Unlock()
	w.WriteHeader(http.StatusOK)
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
		fmt.Fprintf(&out, `<Contents><Key>%s</Key><Size>1</Size></Contents>`, k)
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

// state returns the stored paths in order, the key batches DeleteObjects
// received, and the number of listings served.
func (f *fakeS3) state() (paths []string, batches [][]string, lists int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Sorted(maps.Keys(f.objects)), slices.Clone(f.batches), f.lists
}
