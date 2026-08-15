package s3

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/carolsimone/continuo/remediation-agent/service/ports"
)

// validBundleJSON is a code-bundle document containing exactly one node,
// analytics.orders, decodable by pkg/codebundle.Decode.
const validBundleJSON = `{"contract_version":1,"release_id":"rel-1",
 "nodes":{"analytics.orders":{"runtime":"dbt","raw_code":"select 1 as x","content_hash":"h1"}},
 "shared_code":{}}`

const testBundleURI = "s3://test-bucket/code-bundles/rel-1/bundle.json"

// candidateSourceServer starts an httptest server that plays the given S3
// GetObject response on every request, recording whether it was ever hit so
// a test can prove NodeSource short-circuited without touching S3.
func candidateSourceServer(status int, body string) (*httptest.Server, *atomic.Bool) {
	var called atomic.Bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called.Store(true)
		w.WriteHeader(status)
		_, _ = w.Write([]byte(body))
	}))
	return srv, &called
}

func newTestReader(endpointURL string) *CandidateSourceReader {
	return NewCandidateSourceReader(endpointURL, "test-bucket", "us-east-1", "AKIAEXAMPLE", "secret")
}

func TestNodeSource_ReturnsNodeEntry(t *testing.T) {
	srv, called := candidateSourceServer(http.StatusOK, validBundleJSON)
	defer srv.Close()

	r := newTestReader(srv.URL)
	src, err := r.NodeSource(context.Background(), testBundleURI, "analytics.orders", "rel-1")
	require.NoError(t, err)
	require.Equal(t, ports.CandidateSource{RawCode: "select 1 as x", Runtime: "dbt"}, src)
	require.True(t, called.Load(), "NodeSource must fetch the bundle from S3")
}

func TestNodeSource_EmptyURI_ErrNotFound(t *testing.T) {
	srv, called := candidateSourceServer(http.StatusOK, validBundleJSON)
	defer srv.Close()

	r := newTestReader(srv.URL)
	_, err := r.NodeSource(context.Background(), "", "analytics.orders", "rel-1")
	require.ErrorIs(t, err, ports.ErrNotFound)
	require.False(t, called.Load(), "an empty bundle URI must short-circuit before any S3 call")
}

func TestNodeSource_NodeAbsent_ErrNotFound(t *testing.T) {
	srv, _ := candidateSourceServer(http.StatusOK, validBundleJSON)
	defer srv.Close()

	r := newTestReader(srv.URL)
	_, err := r.NodeSource(context.Background(), testBundleURI, "analytics.missing", "rel-1")
	require.ErrorIs(t, err, ports.ErrNotFound)
}

// TestNodeSource_ReleaseMismatch_ErrNotFound proves a bundle that decodes
// cleanly and names the requested node, but under a different release_id than
// the trigger's own, is treated as a permanent miss rather than returning
// that node's source — the bundle URI is derived from the release, but a
// stale or misrouted object could still resolve to another release's
// document naming the same unique_id.
func TestNodeSource_ReleaseMismatch_ErrNotFound(t *testing.T) {
	srv, _ := candidateSourceServer(http.StatusOK, validBundleJSON)
	defer srv.Close()

	r := newTestReader(srv.URL)
	_, err := r.NodeSource(context.Background(), testBundleURI, "analytics.orders", "rel-2")
	require.ErrorIs(t, err, ports.ErrNotFound)
}

func TestNodeSource_MalformedBundle_ErrNotFound(t *testing.T) {
	srv, _ := candidateSourceServer(http.StatusOK, "not valid json")
	defer srv.Close()

	r := newTestReader(srv.URL)
	_, err := r.NodeSource(context.Background(), testBundleURI, "analytics.orders", "rel-1")
	require.ErrorIs(t, err, ports.ErrNotFound)
}

func TestNodeSource_MissingObject_ErrNotFound(t *testing.T) {
	srv, _ := candidateSourceServer(http.StatusNotFound,
		`<Error><Code>NoSuchKey</Code><Message>The specified key does not exist.</Message></Error>`)
	defer srv.Close()

	r := newTestReader(srv.URL)
	_, err := r.NodeSource(context.Background(), testBundleURI, "analytics.orders", "rel-1")
	require.ErrorIs(t, err, ports.ErrNotFound)
}

// TestNodeSource_OversizedBundle_ErrNotFound proves the in-memory size ceiling
// is a permanent condition too: a bundle over maxBundleBytes can never be
// read in full, so the caller falls back to a repo read rather than retrying
// a fetch that will never succeed.
func TestNodeSource_OversizedBundle_ErrNotFound(t *testing.T) {
	oversized := bytes.Repeat([]byte("a"), int(maxBundleBytes)+1)
	srv, _ := candidateSourceServer(http.StatusOK, string(oversized))
	defer srv.Close()

	r := newTestReader(srv.URL)
	_, err := r.NodeSource(context.Background(), testBundleURI, "analytics.orders", "rel-1")
	require.ErrorIs(t, err, ports.ErrNotFound)
}

// TestNodeSource_TransientFetchError_NotFlattened proves a non-NoSuchKey S3
// failure is kept distinct from the permanent conditions above: it must not
// be classified as ErrNotFound, so the caller does not fall back to a repo
// read and the trigger redelivers instead.
func TestNodeSource_TransientFetchError_NotFlattened(t *testing.T) {
	srv, _ := candidateSourceServer(http.StatusForbidden,
		`<Error><Code>AccessDenied</Code><Message>Access Denied</Message></Error>`)
	defer srv.Close()

	r := newTestReader(srv.URL)
	_, err := r.NodeSource(context.Background(), testBundleURI, "analytics.orders", "rel-1")
	require.Error(t, err)
	require.False(t, errors.Is(err, ports.ErrNotFound),
		"a transient fetch error must not be flattened into ErrNotFound so the trigger redelivers")
}
