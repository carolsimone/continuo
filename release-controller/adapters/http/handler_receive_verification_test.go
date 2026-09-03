package http

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/carolsimone/continuo/release-controller/domain/pipeline"
	"github.com/carolsimone/continuo/release-controller/domain/release"
	"github.com/carolsimone/continuo/release-controller/service/handlers"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func validVerificationInput() handlers.ReceiveVerificationInput {
	return handlers.ReceiveVerificationInput{
		RunID: "verify-rel-1-core-a1", Service: "core", ImageTag: "img:1", Kind: "dbt",
		VerifiesReleaseID: "rel-1", Attempt: 1, SourceOverlayURI: "s3://b/core/verify-rel-1-core-a1/source-overlay.tar.gz",
	}
}

func postVerificationRun(srv *Server, in handlers.ReceiveVerificationInput) *httptest.ResponseRecorder {
	body, _ := json.Marshal(in)
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/verification-runs", bytes.NewReader(body))
	srv.Routes().ServeHTTP(rec, req)
	return rec
}

func TestHandleReceiveVerification_Accepted(t *testing.T) {
	now := time.Unix(100, 0).UTC()
	deps, releases := newRetryRemediationDeps(now)

	rec := postVerificationRun(newTestServer(deps), validVerificationInput())

	require.Equal(t, http.StatusAccepted, rec.Code)
	require.Equal(t, "application/json", rec.Header().Get("Content-Type"))
	var body map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
	assert.Equal(t, "verify-rel-1-core-a1", body["run_id"])

	stored := releases.releases["verify-rel-1-core-a1"]
	require.NotNil(t, stored)
	assert.Equal(t, pipeline.KindVerification, stored.Kind())
}

func TestHandleReceiveVerification_ValidationIs400(t *testing.T) {
	now := time.Unix(100, 0).UTC()
	deps, _ := newRetryRemediationDeps(now)
	for name, mutate := range map[string]func(*handlers.ReceiveVerificationInput){
		"missing run_id":         func(in *handlers.ReceiveVerificationInput) { in.RunID = "" },
		"bad kind":               func(in *handlers.ReceiveVerificationInput) { in.Kind = "yaml" },
		"attempt below one":      func(in *handlers.ReceiveVerificationInput) { in.Attempt = 0 },
		"overlay on python kind": func(in *handlers.ReceiveVerificationInput) { in.Kind = "python" },
	} {
		t.Run(name, func(t *testing.T) {
			in := validVerificationInput()
			mutate(&in)
			rec := postVerificationRun(newTestServer(deps), in)
			require.Equal(t, http.StatusBadRequest, rec.Code)
		})
	}
}

func TestHandleReceiveVerification_CandidateIDConflictIs409(t *testing.T) {
	now := time.Unix(100, 0).UTC()
	deps, releases := newRetryRemediationDeps(now)
	releases.releases["rel-9"] = pipeline.NewCandidate("rel-9", "core", "img", false, "org/r", "sha", release.ManifestKindDbt, now)

	in := validVerificationInput()
	in.RunID = "rel-9"
	rec := postVerificationRun(newTestServer(deps), in)

	require.Equal(t, http.StatusConflict, rec.Code)
}

func TestHandleReceiveCandidate_VerificationIDConflictIs409(t *testing.T) {
	now := time.Unix(100, 0).UTC()
	deps, releases := newRetryRemediationDeps(now)
	releases.releases["run-9"] = pipeline.NewVerification("run-9", "core", "img", "rel-0", 1, "", release.ManifestKindDbt, now)

	body, _ := json.Marshal(handlers.ReceiveCandidateInput{
		Service: "core", ReleaseID: "run-9", ImageTag: "img", Repo: "org/r", CommitSHA: "sha",
	})
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/releases", bytes.NewReader(body))
	newTestServer(deps).Routes().ServeHTTP(rec, req)

	require.Equal(t, http.StatusConflict, rec.Code)
}

// TestHandleReceiveCandidate_RefusesVerificationFieldsWith400 verifies that
// POST /releases answers 400 for a body carrying any of the three
// fix-verification-only fields, rather than persisting a misrouted run.
func TestHandleReceiveCandidate_RefusesVerificationFieldsWith400(t *testing.T) {
	now := time.Unix(100, 0).UTC()
	base := handlers.ReceiveCandidateInput{Service: "core", ReleaseID: "rel-1", ImageTag: "img", Repo: "org/r", CommitSHA: "sha"}
	for name, mutate := range map[string]func(*handlers.ReceiveCandidateInput){
		"shadow":              func(in *handlers.ReceiveCandidateInput) { in.Shadow = true },
		"source_overlay_uri":  func(in *handlers.ReceiveCandidateInput) { in.SourceOverlayURI = "s3://x" },
		"verifies_release_id": func(in *handlers.ReceiveCandidateInput) { in.VerifiesReleaseID = "rel-0" },
	} {
		t.Run(name, func(t *testing.T) {
			deps, _ := newRetryRemediationDeps(now)
			in := base
			mutate(&in)
			body, _ := json.Marshal(in)
			rec := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodPost, "/releases", bytes.NewReader(body))
			newTestServer(deps).Routes().ServeHTTP(rec, req)
			require.Equal(t, http.StatusBadRequest, rec.Code)
			assert.Contains(t, rec.Body.String(), "POST /verification-runs")
		})
	}
}
