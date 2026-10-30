package objectstore_test

import (
	"context"
	"encoding/xml"
	"errors"
	"io"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/carolsimone/continuo/pkg/objectstore"
	"github.com/carolsimone/continuo/pkg/objectstore/objectstoretest"
	pkgredis "github.com/carolsimone/continuo/pkg/redis"
)

const (
	opCreate   = "CreateMultipartUpload"
	opPart     = "UploadPart"
	opComplete = "CompleteMultipartUpload"
	opAbort    = "AbortMultipartUpload"
	opPut      = "PutObject"
)

// operation names the S3 operation a recorded request is.
func operation(r objectstoretest.Request) string {
	switch {
	case r.Method == http.MethodPost && r.Query.Has("uploads"):
		return opCreate
	case r.Method == http.MethodPut && r.Query.Has("partNumber"):
		return opPart
	case r.Method == http.MethodPost && r.Query.Has("uploadId"):
		return opComplete
	case r.Method == http.MethodDelete && r.Query.Has("uploadId"):
		return opAbort
	case r.Method == http.MethodDelete:
		return "DeleteObject"
	case r.Method == http.MethodPut:
		return opPut
	case r.Method == http.MethodGet && r.Query.Get("list-type") == "2":
		return "ListObjectsV2"
	case r.Method == http.MethodGet:
		return "GetObject"
	}
	return r.Method
}

// operations returns the operations srv received, in order.
func operations(srv *objectstoretest.Server) []string {
	var ops []string
	for _, r := range srv.Requests() {
		ops = append(ops, operation(r))
	}
	return ops
}

// requestsOf returns the requests of srv that are the operation op.
func requestsOf(srv *objectstoretest.Server, op string) []objectstoretest.Request {
	var out []objectstoretest.Request
	for _, r := range srv.Requests() {
		if operation(r) == op {
			out = append(out, r)
		}
	}
	return out
}

// completedPartNumbers returns the part numbers a CompleteMultipartUpload
// request lists, in the order it lists them.
func completedPartNumbers(t *testing.T, r objectstoretest.Request) []int {
	t.Helper()
	var req struct {
		Parts []struct {
			PartNumber int `xml:"PartNumber"`
		} `xml:"Part"`
	}
	require.NoError(t, xml.Unmarshal(r.Body, &req))
	var numbers []int
	for _, p := range req.Parts {
		numbers = append(numbers, p.PartNumber)
	}
	return numbers
}

// checksumHeaders returns the additional-checksum headers of r.
func checksumHeaders(r objectstoretest.Request) []string {
	var names []string
	for name := range r.Header {
		if strings.HasPrefix(strings.ToLower(name), "x-amz-checksum-") || strings.EqualFold(name, "x-amz-sdk-checksum-algorithm") {
			names = append(names, name)
		}
	}
	return names
}

func httpStatus(t *testing.T, err error) int {
	t.Helper()
	var status interface{ HTTPStatusCode() int }
	require.True(t, errors.As(err, &status), "the SDK's response error stays in the chain: %v", err)
	return status.HTTPStatusCode()
}

// The writer creates the upload when it is opened, sends each part as soon as
// 8 MiB are buffered, and completes the upload on Close: it never holds more
// than one part.
func TestClient_OpenWriterNeverHoldsMoreThanOnePart(t *testing.T) {
	fake := newFakeS3(0)
	srv := objectstoretest.NewServer(t, fake.handle)
	c := newTestClient(t, srv.URL)
	ref := objectstore.Ref{Key: "exports/graph.ndjson"}
	body := content(40 * mib)

	w, err := c.OpenWriter(context.Background(), ref, "application/x-ndjson")
	require.NoError(t, err)
	require.Equal(t, []string{opCreate}, operations(srv), "the upload is created before the first byte is written")
	create := srv.Requests()[0]
	assert.Equal(t, "/continuo/exports/graph.ndjson", create.Path)
	assert.Equal(t, "application/x-ndjson", create.ContentType)

	for written := 0; written < len(body); written += mib {
		n, err := w.Write(body[written : written+mib])
		require.NoError(t, err)
		require.Equal(t, mib, n)
		assert.Len(t, requestsOf(srv, opPart), (written+mib)/(8*mib),
			"a part goes out once 8 MiB are buffered, and not before (%d MiB written)", (written+mib)/mib)
	}
	require.NoError(t, w.Close())

	parts := requestsOf(srv, opPart)
	require.Len(t, parts, 5)
	for i, p := range parts {
		assert.LessOrEqual(t, len(p.Body), 8*mib, "part %d", i+1)
		assert.Equal(t, strconv.Itoa(i+1), p.Query.Get("partNumber"))
		assert.Equal(t, parts[0].Query.Get("uploadId"), p.Query.Get("uploadId"))
	}
	assert.Equal(t, []string{opCreate, opPart, opPart, opPart, opPart, opPart, opComplete}, operations(srv))
	assert.Equal(t, []int{1, 2, 3, 4, 5}, completedPartNumbers(t, requestsOf(srv, opComplete)[0]))
	assert.Zero(t, fake.openUploads())

	requestsBefore := len(srv.Requests())
	require.NoError(t, w.Abort())
	assert.Len(t, srv.Requests(), requestsBefore, "Abort after Close sends no AbortMultipartUpload")
	assert.NotContains(t, operations(srv), opAbort)

	stored, err := c.Get(context.Background(), ref, 64*mib)
	require.NoError(t, err)
	assert.True(t, slices.Equal(body, stored))
}

// The SDK's default configuration, which a client without static keys uses,
// adds a CRC32 checksum to every request that can carry one. The upload is
// created without a checksum algorithm, so its parts must carry none.
func TestClient_OpenWriterSendsPartsWithoutAChecksumUnderTheDefaultConfiguration(t *testing.T) {
	objectstoretest.UseChainCredentials(t)
	fake := newFakeS3(0)
	srv := objectstoretest.NewServer(t, fake.handle)
	c, err := objectstore.New(context.Background(), objectstore.Config{EndpointURL: srv.URL, Region: "us-east-1", Bucket: "continuo"})
	require.NoError(t, err)
	body := content(9 * mib)

	w, err := c.OpenWriter(context.Background(), objectstore.Ref{Key: "k"}, "text/plain")
	require.NoError(t, err)
	writeAll(t, w, body, mib)
	require.NoError(t, w.Close())

	parts := requestsOf(srv, opPart)
	require.Len(t, parts, 2)
	for _, p := range parts {
		assert.Empty(t, checksumHeaders(p), "part %s", p.Query.Get("partNumber"))
	}
	assert.True(t, slices.Equal(body, readStream(t, c, objectstore.Ref{Key: "k"})))
}

// The last part is whatever is left, however short. An object that is a whole
// number of parts ends with its last full part, never an empty extra one.
func TestClient_OpenWriterUploadsTheRemainderAsTheLastPart(t *testing.T) {
	for _, tc := range []struct {
		name  string
		total int
		parts []int
	}{
		{"one byte", 1, []int{1}},
		{"just under a part", 8*mib - 1, []int{8*mib - 1}},
		{"exactly one part", 8 * mib, []int{8 * mib}},
		{"one byte over a part", 8*mib + 1, []int{8 * mib, 1}},
		{"exactly two parts", 16 * mib, []int{8 * mib, 8 * mib}},
		{"two parts and a remainder", 17 * mib, []int{8 * mib, 8 * mib, mib}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fake := newFakeS3(0)
			srv := objectstoretest.NewServer(t, fake.handle)
			c := newTestClient(t, srv.URL)
			ref := objectstore.Ref{Key: "k"}
			body := content(tc.total)

			w, err := c.OpenWriter(context.Background(), ref, "text/plain")
			require.NoError(t, err)
			writeAll(t, w, body, 3*mib+7) // chunks that straddle part boundaries
			require.NoError(t, w.Close())

			var sizes []int
			for _, p := range requestsOf(srv, opPart) {
				sizes = append(sizes, len(p.Body))
			}
			assert.Equal(t, tc.parts, sizes)
			assert.True(t, slices.Equal(body, readStream(t, c, ref)))
		})
	}
}

// S3 completes a multipart upload only from at least one part, so an object of
// no bytes is stored with one PutObject and the upload opened for it is
// aborted.
func TestClient_OpenWriterStoresAZeroByteObjectWithOnePut(t *testing.T) {
	fake := newFakeS3(0)
	srv := objectstoretest.NewServer(t, fake.handle)
	c := newTestClient(t, srv.URL)

	w, err := c.OpenWriter(context.Background(), objectstore.Ref{Key: "exports/empty"}, "application/x-ndjson")
	require.NoError(t, err)
	require.NoError(t, w.Close())

	assert.Equal(t, []string{opCreate, opAbort, opPut}, operations(srv))
	put := requestsOf(srv, opPut)[0]
	assert.Equal(t, "/continuo/exports/empty", put.Path)
	assert.Equal(t, "application/x-ndjson", put.ContentType)
	assert.Empty(t, put.Body)
	assert.Zero(t, fake.openUploads())
	paths, _, _ := fake.state()
	assert.Equal(t, []string{"continuo/exports/empty"}, paths)

	requestsBefore := len(srv.Requests())
	require.NoError(t, w.Abort())
	assert.Len(t, srv.Requests(), requestsBefore, "Abort after Close sends nothing")
}

func TestClient_OpenWriterAbortDiscardsTheUploadedParts(t *testing.T) {
	fake := newFakeS3(0)
	srv := objectstoretest.NewServer(t, fake.handle)
	c := newTestClient(t, srv.URL)

	w, err := c.OpenWriter(context.Background(), objectstore.Ref{Bucket: "other", Key: "k"}, "text/plain")
	require.NoError(t, err)
	writeAll(t, w, content(9*mib), mib)
	require.NoError(t, w.Abort())

	assert.Equal(t, []string{opCreate, opPart, opAbort}, operations(srv))
	abort := requestsOf(srv, opAbort)[0]
	assert.Equal(t, "/other/k", abort.Path)
	assert.Equal(t, requestsOf(srv, opPart)[0].Query.Get("uploadId"), abort.Query.Get("uploadId"))
	assert.Zero(t, fake.openUploads(), "the store holds no part of the aborted upload")
	paths, _, _ := fake.state()
	assert.Empty(t, paths)

	requestsBefore := len(srv.Requests())
	require.NoError(t, w.Abort())
	assert.Len(t, srv.Requests(), requestsBefore, "a second Abort sends nothing")
}

// A store that refuses the multipart upload fails the open, before any byte is
// written.
func TestClient_OpenWriterFailsAtOpenWhenTheStoreRefusesTheUpload(t *testing.T) {
	srv := objectstoretest.NewServer(t, objectstoretest.ErrorStatus(http.StatusForbidden, "AccessDenied"))

	w, err := newTestClient(t, srv.URL).OpenWriter(context.Background(), objectstore.Ref{Key: "k"}, "text/plain")
	require.Error(t, err)
	assert.Nil(t, w)
	assert.Equal(t, http.StatusForbidden, httpStatus(t, err))
	assert.Contains(t, err.Error(), "s3://continuo/k")
	assert.Len(t, srv.Requests(), 1)
}

// A part the store refuses fails the Write that sent it, every later call
// fails with the same error, and the writer can still be aborted.
func TestClient_OpenWriterUploadErrorMidStreamSurfacesAndStaysAbortable(t *testing.T) {
	fake := newFakeS3(0)
	fake.setFailPart(1)
	srv := objectstoretest.NewServer(t, fake.handle)
	c := newTestClient(t, srv.URL)

	w, err := c.OpenWriter(context.Background(), objectstore.Ref{Key: "k"}, "text/plain")
	require.NoError(t, err)
	chunk := content(mib)
	for i := 1; i < 8; i++ {
		_, err = w.Write(chunk)
		require.NoError(t, err, "the first 7 MiB only fill the buffer")
	}
	_, err = w.Write(chunk)
	require.Error(t, err, "the write that completes the part sends it")
	assert.Equal(t, http.StatusInternalServerError, httpStatus(t, err))
	assert.Equal(t, pkgredis.ClassInfrastructure, pkgredis.Classify(err))
	assert.Contains(t, err.Error(), "part 1")

	_, again := w.Write(chunk)
	assert.Equal(t, err, again, "a failed writer keeps returning its error")
	assert.Equal(t, err, w.Close(), "a failed writer cannot complete")
	assert.NotContains(t, operations(srv), opComplete)

	require.NoError(t, w.Abort())
	assert.Equal(t, opAbort, operations(srv)[len(operations(srv))-1])
	assert.Zero(t, fake.openUploads())
	paths, _, _ := fake.state()
	assert.Empty(t, paths)
}

func TestClient_OpenWriterUploadErrorAtCloseSurfacesAndStaysAbortable(t *testing.T) {
	fake := newFakeS3(0)
	fake.setFailPart(2)
	srv := objectstoretest.NewServer(t, fake.handle)
	c := newTestClient(t, srv.URL)

	w, err := c.OpenWriter(context.Background(), objectstore.Ref{Key: "k"}, "text/plain")
	require.NoError(t, err)
	writeAll(t, w, content(9*mib), mib) // part 1 goes out; 1 MiB stays buffered

	err = w.Close()
	require.Error(t, err, "Close sends the last part")
	assert.Equal(t, http.StatusInternalServerError, httpStatus(t, err))
	assert.Contains(t, err.Error(), "part 2")
	assert.NotContains(t, operations(srv), opComplete)
	assert.Equal(t, err, w.Close(), "a failed Close is not retried")

	require.NoError(t, w.Abort())
	assert.Zero(t, fake.openUploads())
	paths, _, _ := fake.state()
	assert.Empty(t, paths, "the object was never completed")
}

// An upload the store no longer holds cannot be aborted any further: Abort is
// done, while Close reports the loss.
func TestClient_OpenWriterAbortSucceedsWhenTheStoreForgotTheUpload(t *testing.T) {
	fake := newFakeS3(0)
	srv := objectstoretest.NewServer(t, fake.handle)
	c := newTestClient(t, srv.URL)

	w, err := c.OpenWriter(context.Background(), objectstore.Ref{Key: "k"}, "text/plain")
	require.NoError(t, err)
	writeAll(t, w, content(9*mib), mib)
	fake.dropUploads()

	require.Error(t, w.Close(), "the last part belongs to an upload the store forgot")
	require.NoError(t, w.Abort())
	assert.Equal(t, opAbort, operations(srv)[len(operations(srv))-1])
}

// Every request but the cleanup honours the context the writer was opened
// with. Abort runs even once that context is cancelled, because a cancelled
// context is the usual reason to abort and the parts it left would otherwise
// stay in the store.
func TestClient_OpenWriterHonoursItsContextAndStillAbortsOnceItIsCancelled(t *testing.T) {
	fake := newFakeS3(0)
	srv := objectstoretest.NewServer(t, fake.handle)
	c := newTestClient(t, srv.URL)
	ctx, cancel := context.WithCancel(context.Background())

	w, err := c.OpenWriter(ctx, objectstore.Ref{Key: "k"}, "text/plain")
	require.NoError(t, err)
	writeAll(t, w, content(9*mib), mib)
	cancel()

	_, err = w.Write([]byte("x"))
	require.ErrorIs(t, err, context.Canceled)
	require.ErrorIs(t, w.Close(), context.Canceled)
	assert.NotContains(t, operations(srv), opComplete)

	require.NoError(t, w.Abort())
	assert.Equal(t, opAbort, operations(srv)[len(operations(srv))-1])
	assert.Zero(t, fake.openUploads())
}

// GetStream hands the body over as it arrives: it returns before the object
// is read, and the bytes sent so far are readable while the rest is still on
// the wire.
func TestClient_GetStreamReturnsBeforeTheBodyIsRead(t *testing.T) {
	release := make(chan struct{})
	srv := objectstoretest.NewServer(t, func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("first"))
		w.(http.Flusher).Flush()
		<-release
		_, _ = w.Write([]byte("-last"))
	})
	t.Cleanup(func() { // runs before the server's Close, which waits for the handler
		select {
		case <-release:
		default:
			close(release)
		}
	})

	rc, err := newTestClient(t, srv.URL).GetStream(context.Background(), objectstore.Ref{Key: "k"})
	require.NoError(t, err)
	head := make([]byte, 5)
	_, err = io.ReadFull(rc, head)
	require.NoError(t, err)
	assert.Equal(t, "first", string(head))

	close(release)
	rest, err := io.ReadAll(rc)
	require.NoError(t, err)
	assert.Equal(t, "-last", string(rest))
	require.NoError(t, rc.Close())
}

func TestClient_GetStreamServerErrorKeepsItsStatus(t *testing.T) {
	srv := objectstoretest.NewServer(t, objectstoretest.ErrorStatus(http.StatusServiceUnavailable, "ServiceUnavailable"))

	_, err := newTestClient(t, srv.URL).GetStream(context.Background(), objectstore.Ref{Key: "k"})
	require.Error(t, err)
	assert.NotErrorIs(t, err, objectstore.ErrNotFound)
	assert.Equal(t, http.StatusServiceUnavailable, httpStatus(t, err))
	assert.Equal(t, pkgredis.ClassInfrastructure, pkgredis.Classify(err))
}

func TestClient_ListFollowsContinuationTokens(t *testing.T) {
	fake := newFakeS3(2,
		"continuo/p/e", "continuo/p/a", "continuo/p/d", "continuo/p/b", "continuo/p/c", "continuo/q/a")
	srv := objectstoretest.NewServer(t, fake.handle)

	listed, err := newTestClient(t, srv.URL).List(context.Background(), "p/")
	require.NoError(t, err)

	var keys []string
	for _, o := range listed {
		keys = append(keys, o.Key)
		assert.Equal(t, int64(1), o.Size)
		assert.False(t, o.LastModified.IsZero())
	}
	assert.Equal(t, []string{"p/a", "p/b", "p/c", "p/d", "p/e"}, keys)
	_, _, lists := fake.state()
	assert.Equal(t, 3, lists, "five keys at two a page are three listings")
}

func TestClient_ListFailureKeepsItsStatus(t *testing.T) {
	srv := objectstoretest.NewServer(t, objectstoretest.ErrorStatus(http.StatusServiceUnavailable, "ServiceUnavailable"))

	_, err := newTestClient(t, srv.URL).List(context.Background(), "p/")
	require.Error(t, err)
	assert.Equal(t, pkgredis.ClassInfrastructure, pkgredis.Classify(err))
}

func TestClient_DeleteSendsOneDeleteObjectRequest(t *testing.T) {
	srv := objectstoretest.NewServer(t, objectstoretest.OK)
	c := newTestClient(t, srv.URL)
	ctx := context.Background()

	require.NoError(t, c.Delete(ctx, objectstore.Ref{Key: "a/b"}))
	require.NoError(t, c.Delete(ctx, objectstore.Ref{Bucket: "other", Key: "a/b"}))

	reqs := srv.Requests()
	require.Len(t, reqs, 2)
	assert.Equal(t, http.MethodDelete, reqs[0].Method)
	assert.Equal(t, "/continuo/a/b", reqs[0].Path, "a Ref without a bucket deletes from the default bucket")
	assert.Equal(t, "/other/a/b", reqs[1].Path)
}

func TestClient_DeleteRefusedKeepsItsStatus(t *testing.T) {
	srv := objectstoretest.NewServer(t, objectstoretest.ErrorStatus(http.StatusForbidden, "AccessDenied"))

	err := newTestClient(t, srv.URL).Delete(context.Background(), objectstore.Ref{Key: "k"})
	require.Error(t, err)
	assert.Equal(t, http.StatusForbidden, httpStatus(t, err))
	assert.Contains(t, err.Error(), "s3://continuo/k")
}
