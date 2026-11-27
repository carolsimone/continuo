package event

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestPROpenedEventID_KeyedOnReleaseRoundTryAndProject verifies the id is
// stable for the same (release, round, try, project) and distinct when any of
// them changes, so each per-project PR dedups on its own key.
func TestPROpenedEventID_KeyedOnReleaseRoundTryAndProject(t *testing.T) {
	first := PROpenedEventID("r1", 1, 1, "")
	require.Equal(t, first, PROpenedEventID("r1", 1, 1, ""), "the same PR must mint the same id")
	require.NotEqual(t, first, PROpenedEventID("r2", 1, 1, ""), "another release must differ")
	require.NotEqual(t, first, PROpenedEventID("r1", 2, 1, ""), "try 1 of another round must differ")
	require.NotEqual(t, first, PROpenedEventID("r1", 1, 2, ""), "another try must differ")
	require.NotEqual(t, PROpenedEventID("r1", 1, 1, "core"), PROpenedEventID("r1", 1, 1, "finance"),
		"two projects of the same try must produce distinct ids")
	require.NotEqual(t, first, PROpenedEventID("r1", 1, 1, "core"),
		"the whole-fix \"\" id must not collide with a per-project id")
}
