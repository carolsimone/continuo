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

// TestResolvedAt_PassedIsResolved verifies that resolvedAt treats
// StatusPassed as a terminal transition — the same as promoted, rejected,
// and superseded — so a verification run's list item carries a non-empty
// resolved_at once it reaches its terminal status.
func TestResolvedAt_PassedIsResolved(t *testing.T) {
	at := time.Unix(42, 0).UTC()
	r := pipeline.Rehydrate(pipeline.RehydrateInput{
		ID:     "rVAL",
		Status: pipeline.StatusPassed,
		Transitions: []pipeline.Transition{
			{To: pipeline.StatusReceived, At: time.Unix(1, 0).UTC()},
			{To: pipeline.StatusValidating, At: time.Unix(2, 0).UTC()},
			{To: pipeline.StatusPassed, At: at},
		},
	})

	got := resolvedAt(r)
	require.NotNil(t, got)
	assert.Equal(t, at, got.UTC())
}

// TestToReleaseListItem_CarriesProvenance verifies the list row exposes the
// release's source repo and commit SHA, so the UI can resolve the commit author
// without a follow-up per-release detail fetch.
func TestToReleaseListItem_CarriesProvenance(t *testing.T) {
	r := pipeline.Rehydrate(pipeline.RehydrateInput{
		ID:        "rel-1",
		Status:    pipeline.StatusPromoted,
		Repo:      "acme/dbt",
		CommitSHA: "abc123",
	})

	item := toReleaseListItem(r)

	assert.Equal(t, "acme/dbt", item.Repo)
	assert.Equal(t, "abc123", item.CommitSHA)
}

// TestHandleListReleases_OmitsVerificationRuns verifies that GET /releases
// scopes its listing to candidates: a verification run occupying the same
// run collection must not appear in the release history, since it is read
// through GET /verification-runs instead.
func TestHandleListReleases_OmitsVerificationRuns(t *testing.T) {
	now := time.Unix(100, 0).UTC()
	deps, releases := newRetryRemediationDeps(now)
	releases.releases["rel-1"] = pipeline.NewCandidate("rel-1", "finance", "tag", false, "owner/repo", "abc123", release.ManifestKindDbt, now)
	releases.releases["verify-1"] = pipeline.NewVerification("verify-1", "finance", "tag", "rel-0", 1, "", release.ManifestKindDbt, now)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/releases", nil)
	newTestServer(deps).Routes().ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	var resp struct {
		Releases []map[string]any `json:"releases"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	require.Len(t, resp.Releases, 1)
	assert.Equal(t, "rel-1", resp.Releases[0]["release_id"])
}
