package http

import (
	"encoding/json"
	"testing"

	"github.com/carolsimone/continuo/release-controller/domain/release"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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
