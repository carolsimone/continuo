package releasehttp

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/carolsimone/continuo/agent-remediation/domain/proposal"
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

func TestSubmit_PostsTheVerificationBody(t *testing.T) {
	var got map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, http.MethodPost, r.Method)
		require.Equal(t, "/verification-runs", r.URL.Path)
		require.NoError(t, json.NewDecoder(r.Body).Decode(&got))
		w.WriteHeader(http.StatusAccepted)
	}))
	defer srv.Close()
	g := NewGateway(srv.URL, &fakeEvidence{}, nil)
	require.NoError(t, g.Submit(context.Background(), ports.VerificationRequest{
		RunID: "verify-rel-1-core-a1", Service: "core", ImageTag: "img:1", Kind: ports.VerificationKindDbt,
		VerifiesReleaseID: "rel-1", Attempt: 1, SourceOverlayURI: "s3://b/core/verify-rel-1-core-a1/source-overlay.tar.gz",
	}))
	assert.Equal(t, map[string]any{
		"run_id": "verify-rel-1-core-a1", "service": "core", "image_tag": "img:1", "kind": "dbt",
		"verifies_release_id": "rel-1", "attempt": float64(1),
		"source_overlay_uri": "s3://b/core/verify-rel-1-core-a1/source-overlay.tar.gz",
	}, got)
}

func TestSubmit_PythonOmitsTheOverlay(t *testing.T) {
	var got map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.NoError(t, json.NewDecoder(r.Body).Decode(&got))
		w.WriteHeader(http.StatusAccepted)
	}))
	defer srv.Close()
	g := NewGateway(srv.URL, &fakeEvidence{}, nil)
	require.NoError(t, g.Submit(context.Background(), ports.VerificationRequest{RunID: "v", Service: "py", ImageTag: "i", Kind: ports.VerificationKindPython, VerifiesReleaseID: "rel-1", Attempt: 2}))
	_, has := got["source_overlay_uri"]
	assert.False(t, has)
}

// TestSubmit_RequiresKind pins that Submit refuses to silently default an
// unset Kind — an empty Kind is a caller bug, and the request must fail
// before any HTTP call is made.
func TestSubmit_RequiresKind(t *testing.T) {
	g := NewGateway("http://unused", &fakeEvidence{}, nil)
	err := g.Submit(context.Background(), ports.VerificationRequest{RunID: "v", Service: "s", ImageTag: "i", VerifiesReleaseID: "rel-1", Attempt: 1})
	require.ErrorContains(t, err, "kind is required")
}

func TestSubmit_NonAcceptedStatusIsAnError(t *testing.T) {
	srv := releaseServer(t, "/verification-runs", http.StatusConflict, `run id already names a candidate`)
	defer srv.Close()
	g := NewGateway(srv.URL, &fakeEvidence{}, nil)
	err := g.Submit(context.Background(), ports.VerificationRequest{RunID: "v", Service: "s", ImageTag: "i", Kind: "dbt", VerifiesReleaseID: "rel-1", Attempt: 1})
	require.ErrorContains(t, err, "status 409")
}

func TestStatus_PhaseFromRunStatus(t *testing.T) {
	for status, want := range map[string]proposal.Phase{
		"received": proposal.PhaseQueued, "compiling": proposal.PhaseRunning, "parsing": proposal.PhaseRunning,
		"seed_building": proposal.PhaseRunning, "validating": proposal.PhaseRunning,
		"passed": proposal.PhasePassed, "failed": proposal.PhaseFailed,
	} {
		t.Run(status, func(t *testing.T) {
			srv := releaseServer(t, "/verification-runs/verify-x", http.StatusOK,
				`{"run_id":"verify-x","status":"`+status+`","activated_at":"","per_node_results":[]}`)
			defer srv.Close()
			g := NewGateway(srv.URL, &fakeEvidence{}, nil)
			st, err := g.Status(context.Background(), "verify-x")
			require.NoError(t, err)
			assert.Equal(t, want, st.Phase)
		})
	}
}

func TestStatus_ActivatedAtIsReadFromTheResponse(t *testing.T) {
	srv := releaseServer(t, "/verification-runs/verify-x", http.StatusOK,
		`{"run_id":"verify-x","status":"compiling","activated_at":"2026-09-02T10:00:00Z","per_node_results":[]}`)
	defer srv.Close()
	g := NewGateway(srv.URL, &fakeEvidence{}, nil)
	st, err := g.Status(context.Background(), "verify-x")
	require.NoError(t, err)
	assert.Equal(t, time.Date(2026, 9, 2, 10, 0, 0, 0, time.UTC), st.ActivatedAt)
}

func TestStatus_FailedCarriesNodeErrorsFromRunResults(t *testing.T) {
	ev := &fakeEvidence{byURI: map[string]string{"s3://r/orders.json": `{"status":"error","message":"column x does not exist"}`}}
	srv := releaseServer(t, "/verification-runs/verify-x", http.StatusOK,
		`{"run_id":"verify-x","status":"failed","fail_reason":"validation_failed","fail_detail":"",
		  "per_node_results":[{"stage":"validation","node_id":"model.core.orders","status":"failed","run_results_uri":"s3://r/orders.json"},
		                      {"stage":"validation","node_id":"model.core.ok","status":"ok"}]}`)
	defer srv.Close()
	g := NewGateway(srv.URL, ev, nil)
	st, err := g.Status(context.Background(), "verify-x")
	require.NoError(t, err)
	assert.Equal(t, proposal.PhaseFailed, st.Phase)
	assert.Equal(t, map[string]string{"model.core.orders": "column x does not exist"}, st.NodeErrors)
}

func TestStatus_FailedFallsBackToFailReasonAndDetail(t *testing.T) {
	srv := releaseServer(t, "/verification-runs/verify-x", http.StatusOK,
		`{"run_id":"verify-x","status":"failed","fail_reason":"seed_build_failed","fail_detail":"csv is malformed",
		  "per_node_results":[{"stage":"validation","node_id":"model.core.orders","status":"failed"}]}`)
	defer srv.Close()
	g := NewGateway(srv.URL, &fakeEvidence{}, nil)
	st, err := g.Status(context.Background(), "verify-x")
	require.NoError(t, err)
	assert.Equal(t, "seed_build_failed — csv is malformed", st.NodeErrors["model.core.orders"])
}

// TestStatus_FailedReadsSentinelJSONSurroundedByOutput pins that the object at
// run_results_uri is not required to be pure JSON. k8s-controller uploads the
// raw text captured between the validation pod's sentinel markers, which
// carries whatever the runner wrote around the structured record: a log
// preamble before it — possibly with braces of its own — and further output
// after it. A strict decode of the whole body fails on all of that and drops
// the node to the run-level fallback, which for a validation failure is the
// bare word "validation_failed" with no detail, leaving the next fix attempt
// with no real error to learn from.
func TestStatus_FailedReadsSentinelJSONSurroundedByOutput(t *testing.T) {
	srv := releaseServer(t, "/verification-runs/verify-x", http.StatusOK,
		`{"run_id":"verify-x","status":"failed","fail_reason":"validation_failed","fail_detail":"",
		  "per_node_results":[{"stage":"validation","node_id":"model.core.orders","status":"failed","run_results_uri":"s3://bucket/run-results/orders.json"}]}`)
	defer srv.Close()

	// A brace-bearing log line first, then the structured record, then more
	// output. Only the middle object carries a status.
	noisy := "WARNING: unable to reach the metrics sink {\"attempt\": 1}\n" +
		`{"status":"error","message":"column \"revenue_total\" does not exist"}` + "\n" +
		"INFO: uploading artifacts\n"

	ev := &fakeEvidence{byURI: map[string]string{
		"s3://bucket/run-results/orders.json": noisy,
	}}
	g := NewGateway(srv.URL, ev, nil)
	st, err := g.Status(context.Background(), "verify-x")
	require.NoError(t, err)
	assert.Equal(t, `column "revenue_total" does not exist`, st.NodeErrors["model.core.orders"],
		"the node's own error must survive a log preamble and trailing output")
}

// TestStatus_FailedFallsBackWhenNoSentinelJSON pins the other half of the
// lenient scan: a body holding no status-bearing JSON object at all is still
// a miss, so the node falls back to the run-level fail text rather than
// reporting a message scraped out of unrelated output.
func TestStatus_FailedFallsBackWhenNoSentinelJSON(t *testing.T) {
	srv := releaseServer(t, "/verification-runs/verify-x", http.StatusOK,
		`{"run_id":"verify-x","status":"failed","fail_reason":"validation_failed","fail_detail":"1 node failed",
		  "per_node_results":[{"stage":"validation","node_id":"model.core.orders","status":"failed","run_results_uri":"s3://bucket/run-results/orders.json"}]}`)
	defer srv.Close()

	ev := &fakeEvidence{byURI: map[string]string{
		"s3://bucket/run-results/orders.json": "traceback: the runner died before writing a result {\"attempt\": 1}\n",
	}}
	g := NewGateway(srv.URL, ev, nil)
	st, err := g.Status(context.Background(), "verify-x")
	require.NoError(t, err)
	assert.Equal(t, "validation_failed — 1 node failed", st.NodeErrors["model.core.orders"])
}

func TestImageTag_ReadsTheReleaseNotTheRun(t *testing.T) {
	srv := releaseServer(t, "/releases/rel-1", http.StatusOK, `{"release_id":"rel-1","image_tags":{"core":"img:9"}}`)
	defer srv.Close()
	g := NewGateway(srv.URL, &fakeEvidence{}, nil)
	tag, err := g.ImageTag(context.Background(), "rel-1", "core")
	require.NoError(t, err)
	assert.Equal(t, "img:9", tag)
}
