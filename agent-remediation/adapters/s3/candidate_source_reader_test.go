package s3

import (
	"context"
	"encoding/json"
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

// A dbt node's owner is the `meta.owner` its config carries in the bundle. An
// owner that is not a string, or a config with no meta, gives no owner, and
// the node is then absent from the owners.
func TestCandidateSourceReader_ReadsTheDbtOwner(t *testing.T) {
	for _, tc := range []struct {
		name, config string
		want         map[string]string
	}{
		{"meta.owner", `{"materialized":"table","meta":{"owner":"finance"}}`, map[string]string{"analytics.orders": "finance"}},
		{"no meta", `{"materialized":"table"}`, map[string]string{}},
		{"an owner that is not a string", `{"meta":{"owner":["a","b"]}}`, map[string]string{}},
		{"a meta that is not a mapping", `{"meta":"finance"}`, map[string]string{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			body := `{"contract_version":1,"release_id":"rel-1","nodes":{"analytics.orders":{"runtime":"dbt",
			  "raw_code":"select 1 as x","content_hash":"h1","config":` + tc.config + `}},"shared_code":{}}`
			owners, err := NewCandidateSourceReader(newBundleStore(t, body)).NodeOwners(context.Background(), testBundleURI, "rel-1", []string{"analytics.orders"})
			require.NoError(t, err)
			assert.Equal(t, tc.want, owners)
		})
	}
}

// A python node's owner is the `owner` key of its raw_code, the contract entry
// topology-controller writes from the node's ODCS team.name, never its config
// meta. A raw_code that is not a JSON object, or has no string owner, gives no
// owner.
func TestCandidateSourceReader_ReadsThePythonOwner(t *testing.T) {
	for _, tc := range []struct {
		name, rawCode string
		want          map[string]string
	}{
		{"owner", `{"kind": "python-node", "owner": "e2e", "table": "kpis"}`, map[string]string{"analytics.kpis": "e2e"}},
		{"no owner", `{"kind": "python-node"}`, map[string]string{}},
		{"an owner that is not a string", `{"owner": 7}`, map[string]string{}},
		{"not JSON", `not json`, map[string]string{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			raw, err := json.Marshal(tc.rawCode)
			require.NoError(t, err)
			body := `{"contract_version":1,"release_id":"rel-1","nodes":{"analytics.kpis":{"runtime":"python",
			  "raw_code":` + string(raw) + `,"content_hash":"h1","config":{"meta":{"owner":"not-this-one"}}}},"shared_code":{}}`
			owners, err := NewCandidateSourceReader(newBundleStore(t, body)).NodeOwners(context.Background(), testBundleURI, "rel-1", []string{"analytics.kpis"})
			require.NoError(t, err)
			assert.Equal(t, tc.want, owners)
		})
	}
}

// The owners of many nodes come from one read of the bundle, capped at the
// bundle ceiling; a node the bundle does not hold is absent.
func TestNodeOwners_ReadsTheBundleOnceForEveryNode(t *testing.T) {
	body := `{"contract_version":1,"release_id":"rel-1","nodes":{
	  "analytics.orders":{"runtime":"dbt","raw_code":"select 1","content_hash":"h1","config":{"meta":{"owner":"finance"}}},
	  "analytics.kpis":{"runtime":"python","raw_code":"{\"owner\": \"e2e\"}","content_hash":"h2"}},"shared_code":{}}`
	mem := newBundleStore(t, body)

	owners, err := NewCandidateSourceReader(mem).NodeOwners(context.Background(), testBundleURI, "rel-1",
		[]string{"analytics.orders", "analytics.kpis", "analytics.absent"})
	require.NoError(t, err)
	assert.Equal(t, map[string]string{"analytics.orders": "finance", "analytics.kpis": "e2e"}, owners)
	assert.Equal(t, []objectstoretest.Read{{Ref: bundleRef, MaxBytes: maxBundleBytes}}, mem.Reads(), "one read for every node")
}

// The bundle is read as NodeSource reads it: a missing bundle, a malformed one
// or one of another release is ErrNotFound, and a transient fetch error keeps
// its cause.
func TestNodeOwners_ReadsTheBundleAsNodeSourceDoes(t *testing.T) {
	t.Run("missing", func(t *testing.T) {
		_, err := NewCandidateSourceReader(objectstoretest.NewMemory(testBucket)).NodeOwners(context.Background(), testBundleURI, "rel-1", []string{"analytics.orders"})
		require.ErrorIs(t, err, ports.ErrNotFound)
	})
	t.Run("malformed", func(t *testing.T) {
		_, err := NewCandidateSourceReader(newBundleStore(t, `{"contract_version":1,`)).NodeOwners(context.Background(), testBundleURI, "rel-1", []string{"analytics.orders"})
		require.ErrorIs(t, err, ports.ErrNotFound)
	})
	t.Run("another release", func(t *testing.T) {
		_, err := NewCandidateSourceReader(newBundleStore(t, validBundleJSON)).NodeOwners(context.Background(), testBundleURI, "rel-2", []string{"analytics.orders"})
		require.ErrorIs(t, err, ports.ErrNotFound)
	})
	t.Run("transient", func(t *testing.T) {
		mem := objectstoretest.NewMemory(testBucket)
		mem.FailGet(bundleRef, statusError{code: http.StatusServiceUnavailable})
		_, err := NewCandidateSourceReader(mem).NodeOwners(context.Background(), testBundleURI, "rel-1", []string{"analytics.orders"})
		require.Error(t, err)
		assert.False(t, errors.Is(err, ports.ErrNotFound))
		var status interface{ HTTPStatusCode() int }
		require.True(t, errors.As(err, &status))
		assert.Equal(t, http.StatusServiceUnavailable, status.HTTPStatusCode())
	})
}
