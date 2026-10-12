package s3

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/carolsimone/continuo/agent-remediation/service/ports"
	"github.com/carolsimone/continuo/pkg/objectstore"
	"github.com/carolsimone/continuo/pkg/objectstore/objectstoretest"
)

// testBucket is the default bucket of every store these tests build.
const testBucket = "test-bucket"

// validBundleJSON is a code-bundle document containing exactly one node,
// analytics.orders, decodable by pkg/codebundle.Decode.
const validBundleJSON = `{"contract_version":1,"release_id":"rel-1",
 "nodes":{"analytics.orders":{"runtime":"dbt","raw_code":"select 1 as x","content_hash":"h1"}},
 "shared_code":{}}`

const testBundleURI = "s3://test-bucket/code-bundles/rel-1/bundle.json"

// bundleRef is the object testBundleURI names.
var bundleRef = objectstore.Ref{Bucket: testBucket, Key: "code-bundles/rel-1/bundle.json"}

// statusError is a store error carrying an HTTP status, as the AWS SDK's
// response errors do.
type statusError struct{ code int }

func (e statusError) Error() string       { return fmt.Sprintf("s3 answered HTTP %d", e.code) }
func (e statusError) HTTPStatusCode() int { return e.code }

// newBundleStore returns an in-memory store holding body at bundleRef.
func newBundleStore(t *testing.T, body string) *objectstoretest.Memory {
	t.Helper()
	mem := objectstoretest.NewMemory(testBucket)
	require.NoError(t, mem.Put(context.Background(), bundleRef, []byte(body), "application/json"))
	return mem
}

func TestNodeSource_ReturnsNodeEntry(t *testing.T) {
	mem := newBundleStore(t, validBundleJSON)

	src, err := NewCandidateSourceReader(mem).NodeSource(context.Background(), testBundleURI, "analytics.orders", "rel-1")
	require.NoError(t, err)
	require.Equal(t, ports.CandidateSource{RawCode: "select 1 as x", Runtime: "dbt"}, src)
	assert.Equal(t, []objectstoretest.Read{{Ref: bundleRef, MaxBytes: maxBundleBytes}}, mem.Reads(),
		"NodeSource reads the bundle, capped at the bundle ceiling")
}

// A URI that names no object is a permanent miss, refused before any read:
// the caller takes its repo-read fallback.
func TestNodeSource_URIThatNamesNoObject_ErrNotFound(t *testing.T) {
	for _, tc := range []struct{ name, uri string }{
		{"empty", ""},
		{"bucket without a key", "s3://test-bucket"},
		{"another scheme", "https://minio:9000/test-bucket/code-bundles/rel-1/bundle.json"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mem := newBundleStore(t, validBundleJSON)

			_, err := NewCandidateSourceReader(mem).NodeSource(context.Background(), tc.uri, "analytics.orders", "rel-1")
			require.ErrorIs(t, err, ports.ErrNotFound)
			assert.Empty(t, mem.Reads(), "a URI that names no object short-circuits before any read")
		})
	}
}

func TestNodeSource_NodeAbsent_ErrNotFound(t *testing.T) {
	mem := newBundleStore(t, validBundleJSON)

	_, err := NewCandidateSourceReader(mem).NodeSource(context.Background(), testBundleURI, "analytics.missing", "rel-1")
	require.ErrorIs(t, err, ports.ErrNotFound)
}

// TestNodeSource_ReleaseMismatch_ErrNotFound proves a bundle that decodes
// cleanly and names the requested node, but under a different release_id than
// the trigger's own, is treated as a permanent miss rather than returning
// that node's source — the bundle URI is derived from the release, but a
// stale or misrouted object could still resolve to another release's
// document naming the same unique_id.
func TestNodeSource_ReleaseMismatch_ErrNotFound(t *testing.T) {
	mem := newBundleStore(t, validBundleJSON)

	_, err := NewCandidateSourceReader(mem).NodeSource(context.Background(), testBundleURI, "analytics.orders", "rel-2")
	require.ErrorIs(t, err, ports.ErrNotFound)
}

func TestNodeSource_MalformedBundle_ErrNotFound(t *testing.T) {
	mem := newBundleStore(t, "not valid json")

	_, err := NewCandidateSourceReader(mem).NodeSource(context.Background(), testBundleURI, "analytics.orders", "rel-1")
	require.ErrorIs(t, err, ports.ErrNotFound)
}

func TestNodeSource_MissingObject_ErrNotFound(t *testing.T) {
	mem := objectstoretest.NewMemory(testBucket)

	_, err := NewCandidateSourceReader(mem).NodeSource(context.Background(), testBundleURI, "analytics.orders", "rel-1")
	require.ErrorIs(t, err, ports.ErrNotFound)
}

// TestNodeSource_OversizedBundle_ErrNotFound proves the in-memory size ceiling
// is a permanent condition too: a bundle over maxBundleBytes can never be
// read in full, so the caller falls back to a repo read rather than retrying
// a fetch that will never succeed.
func TestNodeSource_OversizedBundle_ErrNotFound(t *testing.T) {
	mem := objectstoretest.NewMemory(testBucket)
	mem.FailGet(bundleRef, fmt.Errorf("%w: %s is over the limit", objectstore.ErrTooLarge, bundleRef))

	_, err := NewCandidateSourceReader(mem).NodeSource(context.Background(), testBundleURI, "analytics.orders", "rel-1")
	require.ErrorIs(t, err, ports.ErrNotFound)
}

// TestNodeSource_TransientFetchError_NotFlattened proves a failure other than
// a missing or oversized object is kept distinct from the permanent
// conditions above: it must not be classified as ErrNotFound, so the caller
// does not fall back to a repo read and the trigger redelivers instead.
func TestNodeSource_TransientFetchError_NotFlattened(t *testing.T) {
	mem := objectstoretest.NewMemory(testBucket)
	mem.FailGet(bundleRef, statusError{code: http.StatusServiceUnavailable})

	_, err := NewCandidateSourceReader(mem).NodeSource(context.Background(), testBundleURI, "analytics.orders", "rel-1")
	require.Error(t, err)
	require.False(t, errors.Is(err, ports.ErrNotFound),
		"a transient fetch error must not be flattened into ErrNotFound so the trigger redelivers")
	var status interface{ HTTPStatusCode() int }
	require.True(t, errors.As(err, &status))
	assert.Equal(t, http.StatusServiceUnavailable, status.HTTPStatusCode())
}
