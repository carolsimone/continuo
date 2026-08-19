package http

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/carolsimone/continuo/release-controller/domain/release"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestGetReleaseResponse_PerNodeResultsIncludeStage verifies that each entry in
// per_node_results exposes its stage (and file_path when present) so the UI can
// group results into Compilation / Seed / Validation sections.
func TestGetReleaseResponse_PerNodeResultsIncludeStage(t *testing.T) {
	rel := release.Rehydrate(release.RehydrateInput{
		ID:     "rSTAGE",
		Status: release.StatusRejected,
		PerNodeResults: []release.NodeValidationResult{
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
	rel := release.Rehydrate(release.RehydrateInput{
		ID:        "rPROV",
		Status:    release.StatusRejected,
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
	// helper is caught: 11 pre-existing keys plus repo + commit_sha + shadow.
	assert.Len(t, decoded, 14)
	assert.Equal(t, "rPROV", decoded["release_id"])
}

// TestGetReleaseResponse_IncludesShadow verifies that the shadow flag is
// exposed by GET /releases/{id} so a caller can distinguish a fix-verification
// release from a normal one.
func TestGetReleaseResponse_IncludesShadow(t *testing.T) {
	shadowRel := release.New("rel-shadow", "finance", "tag", false, true, "owner/repo", "abc123",
		release.ManifestKindDbt, time.Unix(1, 0).UTC())
	assert.Equal(t, true, getReleaseResponse(shadowRel)["shadow"])

	plainRel := release.New("rel-plain", "finance", "tag", false, false, "owner/repo", "abc123",
		release.ManifestKindDbt, time.Unix(1, 0).UTC())
	assert.Equal(t, false, getReleaseResponse(plainRel)["shadow"])
}

func TestGetReleaseResponse_IncludesRejectDetail(t *testing.T) {
	r := release.New("rel-1", "finance", "tag", false, false, "owner/repo", "abc123",
		release.ManifestKindDbt, time.Unix(1, 0).UTC())
	require.NoError(t, r.TransitionToParsing(time.Unix(2, 0).UTC()))
	require.NoError(t, r.TransitionToRejected("duplicate_table",
		"analytics.orders is produced by finance (models/orders.sql) and marketing (models/orders.sql)",
		[]string{"analytics.orders"}, time.Unix(3, 0).UTC()))

	body := getReleaseResponse(r)

	assert.Equal(t, "duplicate_table", body["reject_reason"])
	assert.Equal(t,
		"analytics.orders is produced by finance (models/orders.sql) and marketing (models/orders.sql)",
		body["reject_detail"])
}
