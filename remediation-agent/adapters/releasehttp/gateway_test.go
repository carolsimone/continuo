package releasehttp

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/carolsimone/continuo/remediation-agent/service/ports"
)

// fakeEvidence is a minimal in-memory ports.EvidenceReader for tests: it maps
// a URI to canned content, or reports ports.ErrNotFound for anything else.
type fakeEvidence struct {
	byURI map[string]string
}

var _ ports.EvidenceReader = (*fakeEvidence)(nil)

func (f *fakeEvidence) Fetch(_ context.Context, uri string) (string, error) {
	if body, ok := f.byURI[uri]; ok {
		return body, nil
	}
	return "", ports.ErrNotFound
}

func releaseServer(t *testing.T, wantPath string, status int, body string) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, wantPath, r.URL.Path)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_, _ = w.Write([]byte(body))
	}))
}

func TestSubmit_AcceptedPostsExactShadowBody(t *testing.T) {
	var gotBody map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, http.MethodPost, r.Method)
		require.Equal(t, "/releases", r.URL.Path)
		require.NoError(t, json.NewDecoder(r.Body).Decode(&gotBody))
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write([]byte(`{"release_id":"shadow-1","status":"received"}`))
	}))
	defer srv.Close()

	g := NewGateway(srv.URL, &fakeEvidence{}, srv.Client())
	err := g.Submit(context.Background(), ports.ShadowSubmission{
		ReleaseID: "shadow-1",
		Service:   "svc-py",
		ImageTag:  "docker.io/x/svc-py:abc",
		Repo:      "acme/dbt-repo",
		CommitSHA: "deadbeef",
	})
	require.NoError(t, err)
	require.Equal(t, "shadow-1", gotBody["release_id"])
	require.Equal(t, "svc-py", gotBody["service"])
	require.Equal(t, "docker.io/x/svc-py:abc", gotBody["image_tag"])
	require.Equal(t, false, gotBody["bootstrap"])
	require.Equal(t, "acme/dbt-repo", gotBody["repo"])
	require.Equal(t, "deadbeef", gotBody["commit_sha"])
	require.Equal(t, "python", gotBody["kind"])
	require.Equal(t, true, gotBody["shadow"])
}

func TestSubmit_NonAcceptedIsError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "invalid JSON: boom", http.StatusBadRequest)
	}))
	defer srv.Close()

	g := NewGateway(srv.URL, &fakeEvidence{}, srv.Client())
	err := g.Submit(context.Background(), ports.ShadowSubmission{ReleaseID: "shadow-1", Service: "svc-py", ImageTag: "t", Repo: "acme/r", CommitSHA: "sha"})
	require.Error(t, err)
}

// TestVerdict_ValidatingIsNonTerminal covers case (a): a release still moving
// through the pipeline is neither validated nor rejected.
func TestVerdict_ValidatingIsNonTerminal(t *testing.T) {
	srv := releaseServer(t, "/releases/shadow-1", http.StatusOK,
		`{"release_id":"shadow-1","status":"validating","per_node_results":[]}`)
	defer srv.Close()

	g := NewGateway(srv.URL, &fakeEvidence{}, srv.Client())
	v, err := g.Verdict(context.Background(), "shadow-1")
	require.NoError(t, err)
	require.False(t, v.Terminal, "a validating release must not be reported terminal")
	require.False(t, v.Validated)
	require.Empty(t, v.NodeErrors)
}

// TestVerdict_Validated covers case (b): status validated means terminal and
// validated, with no node errors.
func TestVerdict_Validated(t *testing.T) {
	srv := releaseServer(t, "/releases/shadow-1", http.StatusOK,
		`{"release_id":"shadow-1","status":"validated","per_node_results":[]}`)
	defer srv.Close()

	g := NewGateway(srv.URL, &fakeEvidence{}, srv.Client())
	v, err := g.Verdict(context.Background(), "shadow-1")
	require.NoError(t, err)
	require.True(t, v.Terminal)
	require.True(t, v.Validated)
	require.Empty(t, v.NodeErrors)
}

// TestVerdict_RejectedExtractsNodeErrorsFromEvidence covers case (c): a
// rejected release with one failed validation node resolves its run_results_uri
// through the EvidenceReader to the sentinel JSON's message.
func TestVerdict_RejectedExtractsNodeErrorsFromEvidence(t *testing.T) {
	body := `{
		"release_id": "shadow-1",
		"status": "rejected",
		"reject_reason": "validation_failed",
		"reject_detail": "1 node failed",
		"per_node_results": [
			{"stage": "validation", "node_id": "analytics.py_daily_kpis", "status": "failed", "run_results_uri": "s3://bucket/run-results/analytics.py_daily_kpis.json"},
			{"stage": "validation", "node_id": "analytics.py_other", "status": "ok"}
		]
	}`
	srv := releaseServer(t, "/releases/shadow-1", http.StatusOK, body)
	defer srv.Close()

	evidence := &fakeEvidence{byURI: map[string]string{
		"s3://bucket/run-results/analytics.py_daily_kpis.json": `{"status":"error","message":"column \"day\" does not exist"}`,
	}}
	g := NewGateway(srv.URL, evidence, srv.Client())
	v, err := g.Verdict(context.Background(), "shadow-1")
	require.NoError(t, err)
	require.True(t, v.Terminal)
	require.False(t, v.Validated)
	require.Len(t, v.NodeErrors, 1, "the passing node must not appear in NodeErrors")
	require.Contains(t, v.NodeErrors["analytics.py_daily_kpis"], `column "day" does not exist`)
}

// TestVerdict_RejectedFallsBackToReleaseRejectDetail covers the fallback path:
// when the evidence read yields no message (missing/unresolvable
// run_results_uri), the node's error text falls back to the release's
// reject_reason and reject_detail.
func TestVerdict_RejectedFallsBackToReleaseRejectDetail(t *testing.T) {
	body := `{
		"release_id": "shadow-1",
		"status": "rejected",
		"reject_reason": "validation_failed",
		"reject_detail": "1 node failed",
		"per_node_results": [
			{"stage": "validation", "node_id": "analytics.py_daily_kpis", "status": "failed", "run_results_uri": ""}
		]
	}`
	srv := releaseServer(t, "/releases/shadow-1", http.StatusOK, body)
	defer srv.Close()

	g := NewGateway(srv.URL, &fakeEvidence{}, srv.Client())
	v, err := g.Verdict(context.Background(), "shadow-1")
	require.NoError(t, err)
	require.Contains(t, v.NodeErrors["analytics.py_daily_kpis"], "validation_failed")
	require.Contains(t, v.NodeErrors["analytics.py_daily_kpis"], "1 node failed")
}

// TestImageTag covers case (d): ImageTag reads image_tags[service] from the
// release the id names, erroring for a service absent from that map.
func TestImageTag(t *testing.T) {
	srv := releaseServer(t, "/releases/rel-original", http.StatusOK,
		`{"release_id":"rel-original","status":"promoted","image_tags":{"svc-py":"docker.io/x/svc-py:abc"}}`)
	defer srv.Close()

	g := NewGateway(srv.URL, &fakeEvidence{}, srv.Client())
	tag, err := g.ImageTag(context.Background(), "rel-original", "svc-py")
	require.NoError(t, err)
	require.Equal(t, "docker.io/x/svc-py:abc", tag)

	_, err = g.ImageTag(context.Background(), "rel-original", "svc-unknown")
	require.Error(t, err)
}
