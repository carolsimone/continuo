package s3

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/carolsimone/continuo/orchestrator/service/ports"
	"github.com/carolsimone/continuo/pkg/objectstore"
	"github.com/carolsimone/continuo/pkg/objectstore/objectstoretest"
	pkgredis "github.com/carolsimone/continuo/pkg/redis"
)

// defaultBucket is the default bucket of every store these tests build.
const defaultBucket = "continuo"

// validBundleJSON is a code-bundle document with one node, decodable by
// pkg/codebundle.Decode.
const validBundleJSON = `{"contract_version":1,"release_id":"rel-1",
 "nodes":{"analytics.orders":{"runtime":"dbt","raw_code":"select 1 as x","content_hash":"h1"}},
 "shared_code":{}}`

// statusError is a store error carrying an HTTP status, as the AWS SDK's
// response errors do.
type statusError struct{ code int }

func (e statusError) Error() string       { return fmt.Sprintf("s3 answered HTTP %d", e.code) }
func (e statusError) HTTPStatusCode() int { return e.code }

// bundleRef is where the tests store the release's bundle.
var bundleRef = objectstore.Ref{Bucket: defaultBucket, Key: "code-bundles/rel-1/bundle.json"}

func TestCodeBundleReader_FetchesAndDecodesTheBundle(t *testing.T) {
	mem := objectstoretest.NewMemory(defaultBucket)
	ref := objectstore.Ref{Bucket: "bundles", Key: "code-bundles/rel-1/bundle.json"}
	require.NoError(t, mem.Put(context.Background(), ref, []byte(validBundleJSON), "application/json"))

	bundle, err := NewCodeBundleReader(mem).Fetch(context.Background(), "s3://bundles/code-bundles/rel-1/bundle.json")
	require.NoError(t, err)
	assert.Equal(t, "rel-1", bundle.ReleaseID)
	assert.Equal(t, []objectstoretest.Read{{Ref: ref, MaxBytes: maxBundleBytes}}, mem.Reads(),
		"the URI's bucket is read, capped at the bundle ceiling")
}

func TestCodeBundleReader_BareKeyReadsTheDefaultBucket(t *testing.T) {
	mem := objectstoretest.NewMemory(defaultBucket)
	require.NoError(t, mem.Put(context.Background(), bundleRef, []byte(validBundleJSON), "application/json"))

	_, err := NewCodeBundleReader(mem).Fetch(context.Background(), bundleRef.Key)
	require.NoError(t, err)
	assert.Equal(t, bundleRef, mem.Reads()[0].Ref)
}

// No handler passes a blank URI (both check for one first); the reader keeps
// answering not found for it without a read.
func TestCodeBundleReader_BlankURIIsNotFoundWithoutARead(t *testing.T) {
	mem := objectstoretest.NewMemory(defaultBucket)

	_, err := NewCodeBundleReader(mem).Fetch(context.Background(), "  ")
	assert.ErrorIs(t, err, ports.ErrBundleNotFound)
	assert.Empty(t, mem.Reads())
}

// A URI that names no object is a producer defect no retry repairs: the reader
// reports a malformed bundle, which the versions handler dead-letters and the
// rejections handler records without code, and reads nothing.
func TestCodeBundleReader_URIThatNamesNoObjectIsMalformedWithoutARead(t *testing.T) {
	for _, tc := range []struct{ name, uri string }{
		{"bucket without a key", "s3://continuo"},
		{"bucket with an empty key", "s3://continuo/"},
		{"another scheme", "gs://continuo/code-bundles/rel-1/bundle.json"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mem := objectstoretest.NewMemory(defaultBucket)

			_, err := NewCodeBundleReader(mem).Fetch(context.Background(), tc.uri)
			assert.ErrorIs(t, err, ports.ErrBundleMalformed)
			assert.ErrorIs(t, err, objectstore.ErrInvalidRef)
			assert.NotErrorIs(t, err, ports.ErrBundleNotFound)
			assert.Empty(t, mem.Reads())
		})
	}
}

func TestCodeBundleReader_MissingObjectIsNotFound(t *testing.T) {
	_, err := NewCodeBundleReader(objectstoretest.NewMemory(defaultBucket)).Fetch(context.Background(), bundleRef.String())
	require.ErrorIs(t, err, ports.ErrBundleNotFound)
	assert.Contains(t, err.Error(), "s3://continuo/code-bundles/rel-1/bundle.json")
}

func TestCodeBundleReader_ObjectOverTheCeilingIsTooLarge(t *testing.T) {
	mem := objectstoretest.NewMemory(defaultBucket)
	mem.FailGet(bundleRef, fmt.Errorf("%w: %s is over the limit", objectstore.ErrTooLarge, bundleRef))

	_, err := NewCodeBundleReader(mem).Fetch(context.Background(), bundleRef.String())
	assert.ErrorIs(t, err, ports.ErrBundleTooLarge)
	assert.NotErrorIs(t, err, ports.ErrBundleNotFound)
}

func TestCodeBundleReader_UndecodableObjectIsMalformed(t *testing.T) {
	mem := objectstoretest.NewMemory(defaultBucket)
	require.NoError(t, mem.Put(context.Background(), bundleRef, []byte("not json"), "application/json"))

	_, err := NewCodeBundleReader(mem).Fetch(context.Background(), bundleRef.String())
	assert.ErrorIs(t, err, ports.ErrBundleMalformed)
}

// A failure that is neither a missing nor an oversized object keeps its cause
// and wraps none of the port's sentinels, so an S3 outage reaches the
// consumer's classifier as infrastructure.
func TestCodeBundleReader_OtherFailuresKeepTheirCause(t *testing.T) {
	mem := objectstoretest.NewMemory(defaultBucket)
	mem.FailGet(bundleRef, statusError{code: http.StatusServiceUnavailable})

	_, err := NewCodeBundleReader(mem).Fetch(context.Background(), bundleRef.String())
	require.Error(t, err)
	assert.NotErrorIs(t, err, ports.ErrBundleNotFound)
	assert.NotErrorIs(t, err, ports.ErrBundleTooLarge)
	assert.NotErrorIs(t, err, ports.ErrBundleMalformed)
	var status interface{ HTTPStatusCode() int }
	require.True(t, errors.As(err, &status))
	assert.Equal(t, http.StatusServiceUnavailable, status.HTTPStatusCode())
	assert.Equal(t, pkgredis.ClassInfrastructure, pkgredis.Classify(err))
}
