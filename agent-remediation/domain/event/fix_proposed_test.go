package event

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestRemediationEventID_KeyedOnReleaseRoundAndTry verifies the id is a
// function of (releaseID, round, try) alone: stable for the same try, and
// distinct for another try of the round or the same try number of another
// round.
func TestRemediationEventID_KeyedOnReleaseRoundAndTry(t *testing.T) {
	first := RemediationEventID("r1", 1, 1)
	require.Equal(t, first, RemediationEventID("r1", 1, 1), "the same try must mint the same id")
	require.NotEqual(t, first, RemediationEventID("r1", 1, 2), "another try of the round must differ")
	require.NotEqual(t, first, RemediationEventID("r1", 2, 1), "try 1 of another round must differ")
	require.NotEqual(t, first, RemediationEventID("r2", 1, 1), "another release must differ")
}
