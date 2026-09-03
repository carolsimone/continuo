package http

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/carolsimone/continuo/release-controller/domain/pipeline"
	"github.com/carolsimone/continuo/release-controller/domain/release"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandleGetVerificationRun_ReceivedShapeHasEmptyActivatedAt(t *testing.T) {
	now := time.Unix(100, 0).UTC()
	deps, releases := newRetryRemediationDeps(now)
	releases.releases["verify-1"] = pipeline.NewVerification("verify-1", "core", "img:1", "rel-1", 2, "s3://b/o.tar.gz", release.ManifestKindDbt, now)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/verification-runs/verify-1", nil)
	newTestServer(deps).Routes().ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	var body map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
	assert.Equal(t, "verify-1", body["run_id"])
	assert.Equal(t, "received", body["status"])
	assert.Equal(t, "core", body["changed_service"])
	assert.Equal(t, "rel-1", body["verifies_release_id"])
	assert.EqualValues(t, 2, body["attempt"])
	assert.Equal(t, "dbt", body["manifest_kind"])
	assert.NotEmpty(t, body["created_at"])
	assert.Equal(t, "", body["activated_at"], "a run still in received has no activated_at")
	assert.Equal(t, "", body["finished_at"], "a non-terminal run has no finished_at")
	for _, key := range []string{"transitions", "validation_node_ids", "failing_nodes", "fail_reason", "fail_detail", "per_node_results", "image_tags"} {
		_, ok := body[key]
		assert.True(t, ok, "response must carry %q", key)
	}
}

func TestHandleGetVerificationRun_CandidateIDAnswers404(t *testing.T) {
	now := time.Unix(100, 0).UTC()
	deps, releases := newRetryRemediationDeps(now)
	releases.releases["rel-1"] = pipeline.NewCandidate("rel-1", "core", "img", false, "org/r", "sha", release.ManifestKindDbt, now)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/verification-runs/rel-1", nil)
	newTestServer(deps).Routes().ServeHTTP(rec, req)

	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestHandleGetVerificationRun_UnknownIDAnswers404(t *testing.T) {
	now := time.Unix(100, 0).UTC()
	deps, _ := newRetryRemediationDeps(now)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/verification-runs/nope", nil)
	newTestServer(deps).Routes().ServeHTTP(rec, req)

	assert.Equal(t, http.StatusNotFound, rec.Code)
}
