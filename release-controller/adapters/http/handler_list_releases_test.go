package http

import (
	"testing"
	"time"

	"github.com/carolsimone/continuo/release-controller/domain/release"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestResolvedAt_ValidatedIsResolved verifies that resolvedAt treats
// StatusValidated as a terminal transition — the same as promoted, rejected,
// and superseded — so a shadow release's list item carries a non-empty
// resolved_at once it reaches its terminal status.
func TestResolvedAt_ValidatedIsResolved(t *testing.T) {
	at := time.Unix(42, 0).UTC()
	r := release.Rehydrate(release.RehydrateInput{
		ID:     "rVAL",
		Status: release.StatusValidated,
		Transitions: []release.Transition{
			{To: release.StatusReceived, At: time.Unix(1, 0).UTC()},
			{To: release.StatusValidating, At: time.Unix(2, 0).UTC()},
			{To: release.StatusValidated, At: at},
		},
	})

	got := resolvedAt(r)
	require.NotNil(t, got)
	assert.Equal(t, at, got.UTC())
}
