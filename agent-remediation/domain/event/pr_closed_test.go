package event

import (
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

// TestPRClosedEventID_Deterministic verifies the id is stable for the same
// (release, attempt, service) and distinct across attempts and from PROpened ids.
func TestPRClosedEventID_Deterministic(t *testing.T) {
	a := PRClosedEventID("rel-1", 1, "")
	b := PRClosedEventID("rel-1", 1, "")
	require.Equal(t, a, b, "same inputs must produce the same event id")
	require.NotEqual(t, a, PRClosedEventID("rel-1", 2, ""))
	require.NotEqual(t, a, PROpenedEventID("rel-1", 1, ""),
		"pr_closed and pr_opened ids must not collide for the same PR")
}

// TestPRClosedEventID_LegacyServiceMatchesPreChangeValue pins the legacy ""
// service to the exact id the original two-argument PRClosedEventID(releaseID,
// attempt) signature produced, before the per-service split added the service
// argument.
func TestPRClosedEventID_LegacyServiceMatchesPreChangeValue(t *testing.T) {
	legacy := uuid.NewSHA1(prClosedNamespace, []byte("rel-1"+"|"+itoa(1)))
	require.Equal(t, legacy, PRClosedEventID("rel-1", 1, ""),
		"service \"\" must reproduce the pre-change id byte-for-byte")
}

// TestPRClosedEventID_PerServiceDistinct verifies two owning-service PRs of the
// same (release, attempt) get distinct ids.
func TestPRClosedEventID_PerServiceDistinct(t *testing.T) {
	require.NotEqual(t, PRClosedEventID("rel-1", 1, "core"), PRClosedEventID("rel-1", 1, "finance"))
	require.NotEqual(t, PRClosedEventID("rel-1", 1, ""), PRClosedEventID("rel-1", 1, "core"))
}

