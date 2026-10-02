package http

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/carolsimone/continuo/release-controller/domain/pipeline"
	"github.com/carolsimone/continuo/release-controller/domain/release"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func validVerificationInput() ReceiveVerificationRequest {
	return ReceiveVerificationRequest{
		RunID: "verify-rel-1-core-a1", Service: "core", ImageTag: "img:1", Kind: "dbt",
		VerifiesReleaseID: "rel-1", Attempt: 1, SourceOverlayURI: "s3://b/core/verify-rel-1-core-a1/source-overlay.tar.gz",
	}
}

func postVerificationRun(srv *Server, in ReceiveVerificationRequest) *httptest.ResponseRecorder {
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
	for name, mutate := range map[string]func(*ReceiveVerificationRequest){
		"missing run_id":         func(in *ReceiveVerificationRequest) { in.RunID = "" },
		"bad kind":               func(in *ReceiveVerificationRequest) { in.Kind = "yaml" },
		"attempt below one":      func(in *ReceiveVerificationRequest) { in.Attempt = 0 },
		"overlay on python kind": func(in *ReceiveVerificationRequest) { in.Kind = "python" },
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

	body, _ := json.Marshal(ReceiveCandidateRequest{
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
	base := ReceiveCandidateRequest{Service: "core", ReleaseID: "rel-1", ImageTag: "img", Repo: "org/r", CommitSHA: "sha"}
	for name, mutate := range map[string]func(*ReceiveCandidateRequest){
		"shadow":              func(in *ReceiveCandidateRequest) { in.Shadow = true },
		"source_overlay_uri":  func(in *ReceiveCandidateRequest) { in.SourceOverlayURI = "s3://x" },
		"verifies_release_id": func(in *ReceiveCandidateRequest) { in.VerifiesReleaseID = "rel-0" },
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

// POST /verification-runs reads the wire field names below; a mistyped tag on
// the request DTO would drop a field silently, so the stored run is checked
// against a raw JSON body.
func TestHandleReceiveVerification_StoresEveryWireFieldAsSent(t *testing.T) {
	deps, releases := newRetryRemediationDeps(time.Unix(100, 0).UTC())
	raw := `{"run_id":"verify-rel-7-core-a3","service":"core","image_tag":"img:7","kind":"dbt",` +
		`"verifies_release_id":"rel-7","attempt":3,"source_overlay_uri":"s3://b/core/overlay.tar.gz"}`
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/verification-runs", strings.NewReader(raw))
	newTestServer(deps).Routes().ServeHTTP(rec, req)

	require.Equal(t, http.StatusAccepted, rec.Code)
	stored := releases.releases["verify-rel-7-core-a3"]
	require.NotNil(t, stored)
	assert.Equal(t, pipeline.KindVerification, stored.Kind())
	assert.Equal(t, "rel-7", stored.VerifiesReleaseID())
	assert.Equal(t, 3, stored.Attempt())
	assert.Equal(t, "s3://b/core/overlay.tar.gz", stored.SourceOverlayURI())
	assert.Equal(t, "img:7", stored.ImageTags()["core"])
	assert.Equal(t, release.ManifestKindDbt, stored.ManifestKind())
}
