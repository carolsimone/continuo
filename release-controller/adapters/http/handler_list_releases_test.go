package http

import (
	"testing"
	"time"

	"github.com/carolsimone/continuo/release-controller/domain/pipeline"
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
