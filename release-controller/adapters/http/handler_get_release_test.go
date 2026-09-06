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

// TestGetReleaseResponse_PerNodeResultsIncludeStage verifies that each entry in
// per_node_results exposes its stage (and file_path when present) so the UI can
// group results into Compilation / Seed / Validation sections.
func TestGetReleaseResponse_PerNodeResultsIncludeStage(t *testing.T) {
	rel := pipeline.Rehydrate(pipeline.RehydrateInput{
		ID:     "rSTAGE",
		Status: pipeline.StatusRejected,
		PerNodeResults: []pipeline.NodeValidationResult{
			{Stage: "compile", NodeID: "model.svc.node_a", Status: "failed", FilePath: "models/node_a.sql"},
			{Stage: "validation", NodeID: "model.svc.node_b", Status: "ok"},
		},
	})

	body, err := json.Marshal(getReleaseResponse(rel))
	require.NoError(t, err)

	var decoded map[string]any
	require.NoError(t, json.Unmarshal(body, &decoded))

	raw, ok := decoded["per_node_results"]
	require.True(t, ok, "per_node_results must be present in response")
	entries, ok := raw.([]any)
	require.True(t, ok, "per_node_results must be a JSON array")
	require.Len(t, entries, 2)

	first := entries[0].(map[string]any)
	assert.Equal(t, "compile", first["stage"], "first entry must carry stage=compile")
	assert.Equal(t, "models/node_a.sql", first["file_path"], "compile entry must carry file_path")

	second := entries[1].(map[string]any)
	assert.Equal(t, "validation", second["stage"], "second entry must carry stage=validation")
	_, hasFilePath := second["file_path"]
	assert.False(t, hasFilePath, "validation entry with no file_path must omit the field (omitempty)")
}

func TestGetReleaseResponse_IncludesProvenance(t *testing.T) {
	rel := pipeline.Rehydrate(pipeline.RehydrateInput{
		ID:        "rPROV",
		Status:    pipeline.StatusRejected,
		Repo:      "acme/demo",
		CommitSHA: "deadbeefcafe1234",
	})

	body, err := json.Marshal(getReleaseResponse(rel))
	require.NoError(t, err)

	var decoded map[string]any
	require.NoError(t, json.Unmarshal(body, &decoded))
	assert.Equal(t, "acme/demo", decoded["repo"])
	assert.Equal(t, "deadbeefcafe1234", decoded["commit_sha"])
	// Guard the full response shape so an accidental field drop in the extracted
	// helper is caught: 11 pre-existing keys plus repo + commit_sha +
	// remediation_round. There is no shadow key: a candidate response never
	// carries one, since GET /releases/{id} only ever returns a candidate.
	assert.Len(t, decoded, 15)
	assert.Equal(t, "rPROV", decoded["release_id"])
	_, hasShadow := decoded["shadow"]
	assert.False(t, hasShadow, "a candidate response must not carry a shadow key")
}

// TestHandleGetRelease_VerificationIDAnswers404 verifies that GET
// /releases/{id} treats a verification run's id as unknown: a verification
// is not a release and is read through GET /verification-runs instead.
func TestHandleGetRelease_VerificationIDAnswers404(t *testing.T) {
	now := time.Unix(1, 0).UTC()
	v := pipeline.NewVerification("verify-1", "finance", "tag", "rel-orig", 1, "", release.ManifestKindDbt, now)
	deps, releases := newRetryRemediationDeps(now)
	releases.releases["verify-1"] = v

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/releases/verify-1", nil)
	newTestServer(deps).Routes().ServeHTTP(rec, req)

	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestGetReleaseResponse_IncludesRejectDetail(t *testing.T) {
	r := pipeline.NewCandidate("rel-1", "finance", "tag", false, "owner/repo", "abc123",
		release.ManifestKindDbt, time.Unix(1, 0).UTC())
	require.NoError(t, r.TransitionToParsing(time.Unix(2, 0).UTC()))
	require.NoError(t, r.Fail("duplicate_table",
		"analytics.orders is produced by finance (models/orders.sql) and marketing (models/orders.sql)",
		[]string{"analytics.orders"}, time.Unix(3, 0).UTC()))

	body := getReleaseResponse(r)

	assert.Equal(t, "duplicate_table", body["reject_reason"])
	assert.Equal(t,
		"analytics.orders is produced by finance (models/orders.sql) and marketing (models/orders.sql)",
		body["reject_detail"])
}

// TestGetReleaseResponse_IncludesManifestKind verifies the response names how
// the release's artifact is parsed, so the UI can preview the stages that
// apply to its kind (a python release has no compile leg).
func TestGetReleaseResponse_IncludesManifestKind(t *testing.T) {
	rel := pipeline.Rehydrate(pipeline.RehydrateInput{
		ID:           "rKIND",
		Status:       pipeline.StatusReceived,
		ManifestKind: release.ManifestKindPython,
	})

	body, err := json.Marshal(getReleaseResponse(rel))
	require.NoError(t, err)

	var decoded map[string]any
	require.NoError(t, json.Unmarshal(body, &decoded))
	assert.Equal(t, "python", decoded["manifest_kind"])
}
