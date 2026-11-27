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
	releases.releases["verify-rel-1-r1-t1"] = releasecheck.NewFix("verify-rel-1-r1-t1", "core", "img", "rel-1", 1, 1, "", release.ManifestKindDbt, now)
	releases.releases["verify-rel-1-r2-t1"] = releasecheck.NewFix("verify-rel-1-r2-t1", "core", "img", "rel-1", 2, 1, "", release.ManifestKindDbt, now.Add(time.Minute))
	releases.releases["verify-rel-2-r1-t1"] = releasecheck.NewFix("verify-rel-2-r1-t1", "core", "img", "rel-2", 1, 1, "", release.ManifestKindDbt, now)
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
	byID := map[string]map[string]any{}
	for _, c := range resp.Checks {
		byID[c["check_id"].(string)] = c
	}
	require.Contains(t, byID, "verify-rel-1-r1-t1")
	require.Contains(t, byID, "verify-rel-1-r2-t1")
	assert.EqualValues(t, 1, byID["verify-rel-1-r1-t1"]["round"])
	assert.EqualValues(t, 2, byID["verify-rel-1-r2-t1"]["round"])
	assert.EqualValues(t, 1, byID["verify-rel-1-r2-t1"]["try"], "the try number restarts in each round")
}
