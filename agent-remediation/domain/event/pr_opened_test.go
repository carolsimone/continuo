package event

import (
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestPROpenedEventIDVariesByAttempt(t *testing.T) {
	a1 := PROpenedEventID("r1", 1, "")
	a1b := PROpenedEventID("r1", 1, "")
	a2 := PROpenedEventID("r1", 2, "")
	require.Equal(t, a1, a1b, "same (release,attempt,service) must be stable")
	require.NotEqual(t, a1, a2, "different attempt must differ")
}

func TestPROpenedEventIDDiffersAcrossInputs(t *testing.T) {
	id1 := PROpenedEventID("r-1", 1, "")
	id2 := PROpenedEventID("r-2", 1, "")
	id3 := PROpenedEventID("r-1", 2, "")
	require.NotEqual(t, id1, id2, "different releaseID must differ")
	require.NotEqual(t, id1, id3, "different attempt must differ")
}

// TestPROpenedEventID_LegacyServiceMatchesPreChangeValue pins the legacy ""
// service to the exact id the original two-argument PROpenedEventID(releaseID,
// attempt) signature produced, so a pr_opened fact emitted for a
// whole-proposal (unsplit) PR keeps deduping to the same downstream event id
// across the signature change that added the per-service split's service
// argument.
func TestPROpenedEventID_LegacyServiceMatchesPreChangeValue(t *testing.T) {
	// The pre-change body: NewSHA1(namespace, releaseID+"|"+itoa(attempt)).
	legacy := uuid.NewSHA1(prOpenedNamespace, []byte("r"+"|"+itoa(1)))
	require.Equal(t, legacy, PROpenedEventID("r", 1, ""),
		"service \"\" must reproduce the pre-change id byte-for-byte")
}

// TestPROpenedEventID_PerServiceDistinct verifies two owning-service PRs of the
// same (release, attempt) get distinct ids, so each per-service PR dedups on
// its own key rather than colliding.
func TestPROpenedEventID_PerServiceDistinct(t *testing.T) {
	require.NotEqual(t, PROpenedEventID("r", 1, "core"), PROpenedEventID("r", 1, "finance"),
		"two services of the same attempt must produce distinct ids")
	require.NotEqual(t, PROpenedEventID("r", 1, ""), PROpenedEventID("r", 1, "core"),
		"the legacy \"\" id must not collide with a per-service id")
}

