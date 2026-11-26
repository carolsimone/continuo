package http

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/carolsimone/continuo/pkg/domain/model"
	"github.com/carolsimone/continuo/release-controller/domain/release"
	"github.com/carolsimone/continuo/release-controller/domain/releasecheck"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandleGetReleaseCheck_ReceivedShapeHasEmptyActivatedAt(t *testing.T) {
	now := time.Unix(100, 0).UTC()
	deps, releases := newRetryRemediationDeps(now)
	releases.releases["verify-1"] = releasecheck.NewFix("verify-1", "core", "img:1", "rel-1", 2, "s3://b/o.tar.gz", release.ManifestKindDbt, now)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/release-checks/verify-1", nil)
	newTestServer(deps).Routes().ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	var body map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
	assert.Equal(t, "verify-1", body["check_id"])
	assert.Equal(t, "received", body["status"])
	assert.Equal(t, "core", body["changed_project"])
	assert.Equal(t, "rel-1", body["verifies_release_id"])
	assert.EqualValues(t, 2, body["attempt"])
	assert.Equal(t, "dbt", body["manifest_kind"])
	assert.NotEmpty(t, body["created_at"])
	assert.Equal(t, "", body["activated_at"], "a check still in received has no activated_at")
	assert.Equal(t, "", body["finished_at"], "a non-terminal check has no finished_at")
	for _, key := range []string{"transitions", "validation_node_ids", "failing_nodes", "fail_reason", "fail_detail", "per_node_results", "image_tags"} {
		_, ok := body[key]
		assert.True(t, ok, "response must carry %q", key)
	}
}

func TestHandleGetReleaseCheck_CandidateIDAnswers404(t *testing.T) {
	now := time.Unix(100, 0).UTC()
	deps, releases := newRetryRemediationDeps(now)
	releases.releases["rel-1"] = releasecheck.NewCandidate("rel-1", "core", "img", false, false, "org/r", "sha", release.ManifestKindDbt, now)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/release-checks/rel-1", nil)
	newTestServer(deps).Routes().ServeHTTP(rec, req)

	assert.Equal(t, http.StatusNotFound, rec.Code)
	assert.Contains(t, rec.Body.String(), "GET /releases/{id}", "the 404 says where a candidate release's check is read")
}

func TestHandleGetReleaseCheck_UnknownIDAnswers404(t *testing.T) {
	now := time.Unix(100, 0).UTC()
	deps, _ := newRetryRemediationDeps(now)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/release-checks/nope", nil)
	newTestServer(deps).Routes().ServeHTTP(rec, req)

	assert.Equal(t, http.StatusNotFound, rec.Code)
}

// GET /release-checks/{id} returns the parse timings a check recorded, as a release does:
// how long the parse took, how long its contracts took, and how many contracts the contract
// bundle holds by family. A rejected parse has the durations and no count.
func TestGetReleaseCheck_ReturnsTheParseTimings(t *testing.T) {
	parseMs, contractsMs := int64(4180), int64(1260)
	cases := map[string]struct {
		timings release.ParseTimings
		want    map[string]any
	}{
		"a parsed check": {
			timings: release.ParseTimings{ParseMs: &parseMs, ContractsMs: &contractsMs, ContractsByFamily: map[model.ContractFamily]int{"dbt": 3, "python": 2}},
			want:    map[string]any{"parse_ms": 4180.0, "contracts_ms": 1260.0, "contracts_by_family": map[string]any{"dbt": 3.0, "python": 2.0}},
		},
		"a rejected parse": {
			timings: release.ParseTimings{ParseMs: &parseMs, ContractsMs: &contractsMs},
			want:    map[string]any{"parse_ms": 4180.0, "contracts_ms": 1260.0},
		},
		"a check with no contracts": {
			timings: release.ParseTimings{ParseMs: &parseMs, ContractsMs: &contractsMs, ContractsByFamily: map[model.ContractFamily]int{}},
			want:    map[string]any{"parse_ms": 4180.0, "contracts_ms": 1260.0, "contracts_by_family": map[string]any{}},
		},
		"a check the parse has not reached": {
			timings: release.NewParseTimings(0, 0, nil),
			want:    map[string]any{},
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			deps, releases := newRetryRemediationDeps(time.Unix(1, 0).UTC())
			releases.releases["verify-1"] = releasecheck.Rehydrate(releasecheck.RehydrateInput{
				ID: "verify-1", Kind: releasecheck.KindFix, Status: releasecheck.StatusValidating, ParseTimings: tc.timings,
			})

			rec := httptest.NewRecorder()
			newTestServer(deps).Routes().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/release-checks/verify-1", nil))
			require.Equal(t, http.StatusOK, rec.Code)
			var decoded map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &decoded))

			for _, key := range []string{"parse_ms", "contracts_ms", "contracts_by_family"} {
				if want, ok := tc.want[key]; ok {
					assert.Equal(t, want, decoded[key], key)
				} else {
					assert.NotContains(t, decoded, key)
				}
			}
		})
	}
}
