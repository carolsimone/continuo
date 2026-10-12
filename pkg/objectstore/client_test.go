package objectstore_test

import (
	"context"
	"errors"
	"io"
	"math"
	"net/http"
	"testing"

	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/carolsimone/continuo/pkg/objectstore"
	"github.com/carolsimone/continuo/pkg/objectstore/objectstoretest"
	pkgredis "github.com/carolsimone/continuo/pkg/redis"
)

// newTestClient points a Client with static keys at endpoint, with SDK retries
// off so a 5xx answer reaches the caller on the first attempt.
func newTestClient(t *testing.T, endpoint string) *objectstore.Client {
	t.Helper()
	objectstoretest.IsolateAWSEnvironment(t)
	c, err := objectstore.NewClientWithOptions(context.Background(), objectstore.Config{
		EndpointURL:     endpoint,
		Region:          "us-east-1",
		Bucket:          "continuo",
		AccessKeyID:     "AKIASTATIC",
		SecretAccessKey: "static-secret",
	}, func(o *awss3.Options) { o.RetryMaxAttempts = 1 })
	require.NoError(t, err)
	return c
}

// A client built without static keys signs every request with the
// credentials the default chain resolves.
func TestClient_SignsWithTheDefaultChainWhenKeysAreAbsent(t *testing.T) {
	keyID := objectstoretest.UseChainCredentials(t)
	srv := objectstoretest.NewServer(t, objectstoretest.NoSuchKey)
	c, err := objectstore.New(context.Background(), objectstore.Config{EndpointURL: srv.URL, Region: "us-east-1", Bucket: "continuo"})
	require.NoError(t, err)

	_, err = c.Get(context.Background(), objectstore.Ref{Key: "k"}, 1024)
	require.ErrorIs(t, err, objectstore.ErrNotFound)
	reqs := srv.Requests()
	require.Len(t, reqs, 1)
	assert.Contains(t, reqs[0].Authorization, "Credential="+keyID+"/")
}

func TestClient_SignsWithTheStaticKeysWhenBothAreSet(t *testing.T) {
	objectstoretest.UseChainCredentials(t)
	srv := objectstoretest.NewServer(t, objectstoretest.NoSuchKey)
	c, err := objectstore.New(context.Background(), objectstore.Config{
		EndpointURL: srv.URL, Region: "us-east-1", Bucket: "continuo",
		AccessKeyID: "AKIASTATIC", SecretAccessKey: "static-secret",
	})
	require.NoError(t, err)

	_, err = c.Get(context.Background(), objectstore.Ref{Key: "k"}, 1024)
	require.ErrorIs(t, err, objectstore.ErrNotFound)
	reqs := srv.Requests()
	require.Len(t, reqs, 1)
	assert.Contains(t, reqs[0].Authorization, "Credential=AKIASTATIC/")
}

func TestGet_ReadsTheDefaultBucketOrTheNamedOne(t *testing.T) {
	srv := objectstoretest.NewServer(t, objectstoretest.Object([]byte("body")))
	c := newTestClient(t, srv.URL)
	ctx := context.Background()

	got, err := c.Get(ctx, objectstore.Ref{Key: "a/b.json"}, 1024)
	require.NoError(t, err)
	assert.Equal(t, []byte("body"), got)
	_, err = c.Get(ctx, objectstore.Ref{Bucket: "other", Key: "a/b.json"}, 1024)
	require.NoError(t, err)

	reqs := srv.Requests()
	require.Len(t, reqs, 2)
	assert.Equal(t, http.MethodGet, reqs[0].Method)
	assert.Equal(t, "/continuo/a/b.json", reqs[0].Path, "a Ref without a bucket reads the default bucket, path-style")
	assert.Equal(t, "/other/a/b.json", reqs[1].Path)
	assert.Equal(t, "continuo", c.Bucket())
}

func TestGet_MissingKeyIsNotFound(t *testing.T) {
	srv := objectstoretest.NewServer(t, objectstoretest.NoSuchKey)

	_, err := newTestClient(t, srv.URL).Get(context.Background(),
		objectstore.Ref{Key: "tenants/default/topologies/r1/topology.json.gz"}, 1024)
	require.ErrorIs(t, err, objectstore.ErrNotFound)
	assert.NotErrorIs(t, err, objectstore.ErrTooLarge)
	assert.Contains(t, err.Error(), "s3://continuo/tenants/default/topologies/r1/topology.json.gz")
}

// An S3 outage must stay an infrastructure error all the way to the consumer,
// which then pauses and retries the message instead of counting a delivery.
func TestGet_ServerErrorKeepsItsStatusForClassification(t *testing.T) {
	srv := objectstoretest.NewServer(t, objectstoretest.ErrorStatus(http.StatusServiceUnavailable, "ServiceUnavailable"))

	_, err := newTestClient(t, srv.URL).Get(context.Background(), objectstore.Ref{Key: "k"}, 1024)
	require.Error(t, err)
	assert.NotErrorIs(t, err, objectstore.ErrNotFound)
	assert.NotErrorIs(t, err, objectstore.ErrTooLarge)
	var status interface{ HTTPStatusCode() int }
	require.True(t, errors.As(err, &status), "the SDK's response error stays in the chain")
	assert.Equal(t, http.StatusServiceUnavailable, status.HTTPStatusCode())
	assert.Equal(t, pkgredis.ClassInfrastructure, pkgredis.Classify(err))
}

// The cap holds whether the object declares its length or not: a declared
// length over the cap is refused before the body is read, and an undeclared
// one is refused once the read passes the cap.
func TestGet_RefusesAnObjectOverTheCap(t *testing.T) {
	for _, tc := range []struct {
		name    string
		handler http.HandlerFunc
	}{
		{"declared length", objectstoretest.Object([]byte("0123456789"))},
		{"declared length, body never sent", objectstoretest.DeclaredLength(1 << 20)},
		{"undeclared length", func(w http.ResponseWriter, _ *http.Request) {
			_, _ = w.Write([]byte("01234"))
			w.(http.Flusher).Flush() // sends the headers without a Content-Length
			_, _ = w.Write([]byte("56789"))
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			srv := objectstoretest.NewServer(t, tc.handler)

			_, err := newTestClient(t, srv.URL).Get(context.Background(), objectstore.Ref{Key: "k"}, 4)
			require.ErrorIs(t, err, objectstore.ErrTooLarge)
			assert.NotErrorIs(t, err, objectstore.ErrNotFound)
		})
	}
}

func TestGet_AcceptsAnObjectExactlyAtTheCap(t *testing.T) {
	srv := objectstoretest.NewServer(t, objectstoretest.Object([]byte("0123")))

	got, err := newTestClient(t, srv.URL).Get(context.Background(), objectstore.Ref{Key: "k"}, 4)
	require.NoError(t, err)
	assert.Equal(t, []byte("0123"), got)
}

// A response that ends before its declared Content-Length is a transport
// failure, not a bad object: it must classify as infrastructure so the
// consumer pauses for storage recovery instead of dead-lettering the message.
func TestGet_TruncatedBodyIsInfrastructure(t *testing.T) {
	srv := objectstoretest.NewServer(t, func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Length", "100")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("only ten b"))
	})

	_, err := newTestClient(t, srv.URL).Get(context.Background(), objectstore.Ref{Key: "k"}, 1024)
	require.Error(t, err)
	assert.ErrorIs(t, err, io.ErrUnexpectedEOF)
	assert.NotErrorIs(t, err, objectstore.ErrNotFound)
	assert.NotErrorIs(t, err, objectstore.ErrTooLarge)
	assert.Equal(t, pkgredis.ClassInfrastructure, pkgredis.Classify(err))
}

func TestGet_RefusesAnUnusableLimitWithoutARequest(t *testing.T) {
	srv := objectstoretest.NewServer(t, objectstoretest.Object([]byte("body")))
	c := newTestClient(t, srv.URL)

	for _, limit := range []int64{0, -1, math.MaxInt64} {
		_, err := c.Get(context.Background(), objectstore.Ref{Key: "k"}, limit)
		assert.Error(t, err, "limit %d", limit)
	}
	assert.Empty(t, srv.Requests())
}

// A Ref built in code is held to the rule ParseRef applies to a URI: one that
// names no object is refused before any request.
func TestClient_RefusesARefThatNamesNoObjectWithoutARequest(t *testing.T) {
	srv := objectstoretest.NewServer(t, objectstoretest.OK)
	c := newTestClient(t, srv.URL)
	ctx := context.Background()

	for _, ref := range []objectstore.Ref{{}, {Bucket: "continuo"}, {Key: "/k"}} {
		_, err := c.Get(ctx, ref, 1024)
		assert.ErrorIs(t, err, objectstore.ErrInvalidRef, "get %+v", ref)
		assert.ErrorIs(t, c.Put(ctx, ref, []byte("x"), "text/plain"), objectstore.ErrInvalidRef, "put %+v", ref)
	}
	assert.Empty(t, srv.Requests())
}

func TestPut_WritesTheBodyAndContentTypeToTheDefaultBucketOrTheNamedOne(t *testing.T) {
	srv := objectstoretest.NewServer(t, objectstoretest.OK)
	c := newTestClient(t, srv.URL)
	ctx := context.Background()

	require.NoError(t, c.Put(ctx, objectstore.Ref{Key: "tenants/default/topologies/r1/topology.json.gz"},
		[]byte("gzipped topology bytes"), "application/gzip"))
	require.NoError(t, c.Put(ctx, objectstore.Ref{Bucket: "other", Key: "k"}, []byte("x"), "text/plain"))

	reqs := srv.Requests()
	require.Len(t, reqs, 2)
	assert.Equal(t, http.MethodPut, reqs[0].Method)
	assert.Equal(t, "/continuo/tenants/default/topologies/r1/topology.json.gz", reqs[0].Path)
	assert.Equal(t, "application/gzip", reqs[0].ContentType)
	assert.Equal(t, []byte("gzipped topology bytes"), reqs[0].Body, "over plain HTTP the SDK sends the object body unframed")
	assert.Equal(t, "/other/k", reqs[1].Path)
}

func TestPut_RefusedWriteKeepsItsStatus(t *testing.T) {
	srv := objectstoretest.NewServer(t, objectstoretest.ErrorStatus(http.StatusForbidden, "AccessDenied"))

	err := newTestClient(t, srv.URL).Put(context.Background(), objectstore.Ref{Key: "k"}, []byte("x"), "text/plain")
	require.Error(t, err)
	var status interface{ HTTPStatusCode() int }
	require.True(t, errors.As(err, &status))
	assert.Equal(t, http.StatusForbidden, status.HTTPStatusCode())
	assert.Contains(t, err.Error(), "s3://continuo/k")
}

func TestDeletePrefix_DeletesEveryPageInOneBatchEach(t *testing.T) {
	fake := newFakeS3(2,
		"continuo/candidate-sql/r1/a.sql", "continuo/candidate-sql/r1/b.sql",
		"continuo/candidate-sql/r1/c.sql", "continuo/candidate-sql/r2/a.sql")
	srv := objectstoretest.NewServer(t, fake.handle)

	require.NoError(t, newTestClient(t, srv.URL).DeletePrefix(context.Background(), "candidate-sql/r1/"))

	paths, batches, lists := fake.state()
	assert.Equal(t, [][]string{
		{"candidate-sql/r1/a.sql", "candidate-sql/r1/b.sql"},
		{"candidate-sql/r1/c.sql"},
	}, batches)
	assert.Equal(t, []string{"continuo/candidate-sql/r2/a.sql"}, paths, "objects outside the prefix stay")
	assert.Equal(t, 2, lists)
}

func TestDeletePrefix_NothingMatchingSendsNoDelete(t *testing.T) {
	fake := newFakeS3(0, "continuo/code-bundles/r2/bundle.json")
	srv := objectstoretest.NewServer(t, fake.handle)

	require.NoError(t, newTestClient(t, srv.URL).DeletePrefix(context.Background(), "code-bundles/r1/"))

	paths, batches, lists := fake.state()
	assert.Empty(t, batches)
	assert.Equal(t, []string{"continuo/code-bundles/r2/bundle.json"}, paths)
	assert.Equal(t, 1, lists)
}

func TestDeletePrefix_PartialFailureIsAnErrorNamingTheFirstRefusedKey(t *testing.T) {
	fake := newFakeS3(0, "continuo/p/a", "continuo/p/b", "continuo/p/c")
	fake.refuse["continuo/p/b"] = true
	fake.refuse["continuo/p/c"] = true
	srv := objectstoretest.NewServer(t, fake.handle)

	err := newTestClient(t, srv.URL).DeletePrefix(context.Background(), "p/")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "2 of 3 keys failed")
	assert.Contains(t, err.Error(), "first p/b")
	assert.Contains(t, err.Error(), "AccessDenied")
	paths, _, _ := fake.state()
	assert.Equal(t, []string{"continuo/p/b", "continuo/p/c"}, paths, "the keys the store accepted are gone")
}

func TestDeletePrefix_ListFailureKeepsItsStatus(t *testing.T) {
	srv := objectstoretest.NewServer(t, objectstoretest.ErrorStatus(http.StatusServiceUnavailable, "ServiceUnavailable"))

	err := newTestClient(t, srv.URL).DeletePrefix(context.Background(), "p/")
	require.Error(t, err)
	assert.Equal(t, pkgredis.ClassInfrastructure, pkgredis.Classify(err))
}

// An empty prefix names every object in the bucket; it is refused before any
// request is sent.
func TestDeletePrefix_RefusesAnEmptyPrefix(t *testing.T) {
	fake := newFakeS3(0, "continuo/a", "continuo/b")
	srv := objectstoretest.NewServer(t, fake.handle)

	require.Error(t, newTestClient(t, srv.URL).DeletePrefix(context.Background(), ""))
	assert.Empty(t, srv.Requests())
}
