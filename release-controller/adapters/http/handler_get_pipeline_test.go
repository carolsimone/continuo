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

func TestHandleGetPipeline_NothingActiveIsNull(t *testing.T) {
	now := time.Unix(100, 0).UTC()
	deps, _ := newRetryRemediationDeps(now)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/pipeline", nil)
	newTestServer(deps).Routes().ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	var body map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
	active, ok := body["active"]
	require.True(t, ok, "response must carry an active key")
	assert.Nil(t, active)
}

func TestHandleGetPipeline_ActiveCandidate(t *testing.T) {
	now := time.Unix(100, 0).UTC()
	deps, releases := newRetryRemediationDeps(now)
	c := pipeline.NewCandidate("rel-1", "core", "img", false, "org/r", "sha", release.ManifestKindDbt, now)
	require.NoError(t, c.TransitionToCompiling(now.Add(time.Minute)))
	releases.releases["rel-1"] = c

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/pipeline", nil)
	newTestServer(deps).Routes().ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	var body map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
	active, ok := body["active"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "rel-1", active["run_id"])
	assert.Equal(t, "candidate", active["run_kind"])
	assert.Equal(t, "compiling", active["status"])
	assert.Equal(t, "core", active["service"])
	_, hasVerifies := active["verifies_release_id"]
	assert.False(t, hasVerifies, "a candidate's active summary carries no verifies_release_id")
}

func TestHandleGetPipeline_ActiveVerificationCarriesAttemptAndVerifies(t *testing.T) {
	now := time.Unix(100, 0).UTC()
	deps, releases := newRetryRemediationDeps(now)
	v := pipeline.NewVerification("verify-1", "core", "img", "rel-1", 2, "", release.ManifestKindDbt, now)
	require.NoError(t, v.TransitionToCompiling(now.Add(time.Minute)))
	releases.releases["verify-1"] = v

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/pipeline", nil)
	newTestServer(deps).Routes().ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	var body map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
	active, ok := body["active"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "verify-1", active["run_id"])
	assert.Equal(t, "verification", active["run_kind"])
	assert.Equal(t, "rel-1", active["verifies_release_id"])
	assert.EqualValues(t, 2, active["attempt"])
}
