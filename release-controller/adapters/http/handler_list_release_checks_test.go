package http

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/carolsimone/continuo/release-controller/domain/release"
	"github.com/carolsimone/continuo/release-controller/domain/releasecheck"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandleListReleaseChecks_RequiresVerifiesParam(t *testing.T) {
	now := time.Unix(100, 0).UTC()
	deps, _ := newRetryRemediationDeps(now)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/release-checks", nil)
	newTestServer(deps).Routes().ServeHTTP(rec, req)

	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandleListReleaseChecks_ReturnsOnlyTheChecksOfTheGivenRelease(t *testing.T) {
	now := time.Unix(100, 0).UTC()
	deps, releases := newRetryRemediationDeps(now)
	releases.releases["verify-rel-1-a1"] = releasecheck.NewFix("verify-rel-1-a1", "core", "img", "rel-1", 1, "", release.ManifestKindDbt, now)
	releases.releases["verify-rel-1-a2"] = releasecheck.NewFix("verify-rel-1-a2", "core", "img", "rel-1", 2, "", release.ManifestKindDbt, now.Add(time.Minute))
	releases.releases["verify-rel-2-a1"] = releasecheck.NewFix("verify-rel-2-a1", "core", "img", "rel-2", 1, "", release.ManifestKindDbt, now)
	releases.releases["rel-1"] = releasecheck.NewCandidate("rel-1", "core", "img", false, false, "org/r", "sha", release.ManifestKindDbt, now)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/release-checks?verifies=rel-1", nil)
	newTestServer(deps).Routes().ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	var resp struct {
		Checks []map[string]any `json:"checks"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	require.Len(t, resp.Checks, 2)
	ids := []string{resp.Checks[0]["check_id"].(string), resp.Checks[1]["check_id"].(string)}
	assert.ElementsMatch(t, []string{"verify-rel-1-a1", "verify-rel-1-a2"}, ids)
}
