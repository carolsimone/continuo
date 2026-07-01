package http

import (
	"encoding/json"
	"testing"

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
	// helper is caught: 10 pre-existing keys plus repo + commit_sha.
	assert.Len(t, decoded, 12)
	assert.Equal(t, "rPROV", decoded["release_id"])
}
