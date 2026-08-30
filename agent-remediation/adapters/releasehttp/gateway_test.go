package releasehttp

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/carolsimone/continuo/agent-remediation/service/ports"
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
		Kind:      ports.ShadowKindPython,
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
	require.NotContains(t, gotBody, "source_overlay_uri", "a python submission must omit source_overlay_uri entirely")
}

// TestSubmit_DbtCarriesSourceOverlayURI covers the dbt shadow case: the
// compile leg lays a source overlay tarball over the project, and its S3 URI
// travels in the POST body alongside kind:"dbt".
func TestSubmit_DbtCarriesSourceOverlayURI(t *testing.T) {
	var gotBody map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.NoError(t, json.NewDecoder(r.Body).Decode(&gotBody))
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write([]byte(`{"release_id":"shadow-1","status":"received"}`))
	}))
	defer srv.Close()

	g := NewGateway(srv.URL, &fakeEvidence{}, srv.Client())
	err := g.Submit(context.Background(), ports.ShadowSubmission{
		ReleaseID:        "shadow-1",
		Service:          "svc-dbt",
		ImageTag:         "docker.io/x/svc-dbt:abc",
		Repo:             "acme/dbt-repo",
		CommitSHA:        "deadbeef",
		Kind:             ports.ShadowKindDbt,
		SourceOverlayURI: "s3://b/svc/shadow-1/source-overlay.tar.gz",
	})
	require.NoError(t, err)
	require.Equal(t, "dbt", gotBody["kind"])
	require.Equal(t, "s3://b/svc/shadow-1/source-overlay.tar.gz", gotBody["source_overlay_uri"])
}

// TestSubmit_EmptyKindIsError pins that Submit refuses to silently default an
// unset Kind to "python" — an empty Kind is a caller bug, and the request
// must fail before any HTTP call is made.
func TestSubmit_EmptyKindIsError(t *testing.T) {
	called := false
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
		w.WriteHeader(http.StatusAccepted)
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
	require.Error(t, err)
	require.False(t, called, "an empty Kind must fail before any HTTP call is made")
}

func TestSubmit_NonAcceptedIsError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "invalid JSON: boom", http.StatusBadRequest)
	}))
	defer srv.Close()

	g := NewGateway(srv.URL, &fakeEvidence{}, srv.Client())
	err := g.Submit(context.Background(), ports.ShadowSubmission{ReleaseID: "shadow-1", Service: "svc-py", ImageTag: "t", Repo: "acme/r", CommitSHA: "sha", Kind: ports.ShadowKindPython})
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

// TestVerdict_RejectedReadsSentinelJSONSurroundedByOutput pins that the object
// at run_results_uri is not required to be pure JSON. k8s-controller uploads
// the raw text captured between the validation pod's sentinel markers, which
// carries whatever the runner wrote around the structured record: a log
// preamble before it — possibly with braces of its own — and further output
// after it. A strict decode of the whole body fails on all of that and drops
// the node to the release-level fallback, which for a validation rejection is
// the bare word "validation_failed" with no detail, leaving the next fix
// attempt with no real error to learn from.
func TestVerdict_RejectedReadsSentinelJSONSurroundedByOutput(t *testing.T) {
	body := `{
		"release_id": "shadow-1",
		"status": "rejected",
		"reject_reason": "validation_failed",
		"reject_detail": "",
		"per_node_results": [
			{"stage": "validation", "node_id": "analytics.py_daily_kpis", "status": "failed", "run_results_uri": "s3://bucket/run-results/analytics.py_daily_kpis.json"}
		]
	}`
	srv := releaseServer(t, "/releases/shadow-1", http.StatusOK, body)
	defer srv.Close()

	// A brace-bearing log line first, then the structured record, then more
	// output. Only the middle object carries a status.
	noisy := "WARNING: unable to reach the metrics sink {\"attempt\": 1}\n" +
		`{"status":"error","message":"column \"revenue_total\" does not exist"}` + "\n" +
		"INFO: uploading artifacts\n"

	evidence := &fakeEvidence{byURI: map[string]string{
		"s3://bucket/run-results/analytics.py_daily_kpis.json": noisy,
	}}
	g := NewGateway(srv.URL, evidence, srv.Client())
	v, err := g.Verdict(context.Background(), "shadow-1")
	require.NoError(t, err)
	require.Equal(t, `column "revenue_total" does not exist`, v.NodeErrors["analytics.py_daily_kpis"],
		"the node's own error must survive a log preamble and trailing output")
}

// TestVerdict_RejectedFallsBackWhenNoSentinelJSON pins the other half of the
// lenient scan: a body holding no status-bearing JSON object at all is still a
// miss, so the node falls back to the release-level reject text rather than
// reporting a message scraped out of unrelated output.
func TestVerdict_RejectedFallsBackWhenNoSentinelJSON(t *testing.T) {
	body := `{
		"release_id": "shadow-1",
		"status": "rejected",
		"reject_reason": "validation_failed",
		"reject_detail": "1 node failed",
		"per_node_results": [
			{"stage": "validation", "node_id": "analytics.py_daily_kpis", "status": "failed", "run_results_uri": "s3://bucket/run-results/analytics.py_daily_kpis.json"}
		]
	}`
	srv := releaseServer(t, "/releases/shadow-1", http.StatusOK, body)
	defer srv.Close()

	evidence := &fakeEvidence{byURI: map[string]string{
		"s3://bucket/run-results/analytics.py_daily_kpis.json": "traceback: the runner died before writing a result {\"attempt\": 1}\n",
	}}
	g := NewGateway(srv.URL, evidence, srv.Client())
	v, err := g.Verdict(context.Background(), "shadow-1")
	require.NoError(t, err)
	require.Equal(t, "validation_failed — 1 node failed", v.NodeErrors["analytics.py_daily_kpis"])
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

// TestVerdict_ReportsWhenTheReleaseLeftTheQueue pins the moment the caller
// measures a verification budget from. A shadow release joins the same global
// FIFO queue as every other release and sits in "received" until its turn, so
// the wait a timeout is meant to bound starts when the pipeline actually picked
// it up — the first transition past "received" — not when it was submitted.
func TestVerdict_ReportsWhenTheReleaseLeftTheQueue(t *testing.T) {
	srv := releaseServer(t, "/releases/shadow-1", http.StatusOK,
		`{"release_id":"shadow-1","status":"validating","per_node_results":[],
		  "transitions":[{"to":"received","at":"2026-08-19T10:00:00Z"},
		                 {"to":"parsing","at":"2026-08-19T11:30:00Z"},
		                 {"to":"validating","at":"2026-08-19T11:32:00Z"}]}`)
	defer srv.Close()

	g := NewGateway(srv.URL, &fakeEvidence{}, srv.Client())
	v, err := g.Verdict(context.Background(), "shadow-1")
	require.NoError(t, err)
	require.Equal(t, time.Date(2026, 8, 19, 11, 30, 0, 0, time.UTC), v.ActivatedAt.UTC(),
		"activation is the first transition past 'received', not the last one and not the submission")
}

// TestVerdict_AQueuedReleaseHasNoActivationMoment is the other half: a release
// still waiting its turn reports no activation at all, so a caller can tell
// "this has been running too long" apart from "this has not started".
func TestVerdict_AQueuedReleaseHasNoActivationMoment(t *testing.T) {
	srv := releaseServer(t, "/releases/shadow-1", http.StatusOK,
		`{"release_id":"shadow-1","status":"received","per_node_results":[],
		  "transitions":[{"to":"received","at":"2026-08-19T10:00:00Z"}]}`)
	defer srv.Close()

	g := NewGateway(srv.URL, &fakeEvidence{}, srv.Client())
	v, err := g.Verdict(context.Background(), "shadow-1")
	require.NoError(t, err)
	require.False(t, v.Terminal)
	require.True(t, v.ActivatedAt.IsZero(), "a release still in the queue has not started being verified")
}
