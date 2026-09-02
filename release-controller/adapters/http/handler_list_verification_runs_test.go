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

func TestHandleListVerificationRuns_RequiresVerifiesParam(t *testing.T) {
	now := time.Unix(100, 0).UTC()
	deps, _ := newRetryRemediationDeps(now)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/verification-runs", nil)
	newTestServer(deps).Routes().ServeHTTP(rec, req)

	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandleListVerificationRuns_ReturnsOnlyRunsForTheGivenRelease(t *testing.T) {
	now := time.Unix(100, 0).UTC()
	deps, releases := newRetryRemediationDeps(now)
	releases.releases["verify-rel-1-a1"] = pipeline.NewVerification("verify-rel-1-a1", "core", "img", "rel-1", 1, "", release.ManifestKindDbt, now)
	releases.releases["verify-rel-1-a2"] = pipeline.NewVerification("verify-rel-1-a2", "core", "img", "rel-1", 2, "", release.ManifestKindDbt, now.Add(time.Minute))
	releases.releases["verify-rel-2-a1"] = pipeline.NewVerification("verify-rel-2-a1", "core", "img", "rel-2", 1, "", release.ManifestKindDbt, now)
	releases.releases["rel-1"] = pipeline.NewCandidate("rel-1", "core", "img", false, "org/r", "sha", release.ManifestKindDbt, now)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/verification-runs?verifies=rel-1", nil)
	newTestServer(deps).Routes().ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	var resp struct {
		Runs []map[string]any `json:"runs"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	require.Len(t, resp.Runs, 2)
	ids := []string{resp.Runs[0]["run_id"].(string), resp.Runs[1]["run_id"].(string)}
	assert.ElementsMatch(t, []string{"verify-rel-1-a1", "verify-rel-1-a2"}, ids)
}
