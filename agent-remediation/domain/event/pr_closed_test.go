package event

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestPRClosedEventID_KeyedOnReleaseRoundTryAndProject verifies the id is
// stable for the same (release, round, try, project), distinct when any of
// them changes, and never the PROpened id of the same PR.
func TestPRClosedEventID_KeyedOnReleaseRoundTryAndProject(t *testing.T) {
	first := PRClosedEventID("rel-1", 1, 1, "")
	require.Equal(t, first, PRClosedEventID("rel-1", 1, 1, ""), "same inputs must produce the same event id")
	require.NotEqual(t, first, PRClosedEventID("rel-1", 1, 2, ""), "another try must differ")
	require.NotEqual(t, first, PRClosedEventID("rel-1", 2, 1, ""), "try 1 of another round must differ")
	require.NotEqual(t, PRClosedEventID("rel-1", 1, 1, "core"), PRClosedEventID("rel-1", 1, 1, "finance"))
	require.NotEqual(t, first, PRClosedEventID("rel-1", 1, 1, "core"))
	require.NotEqual(t, first, PROpenedEventID("rel-1", 1, 1, ""),
		"pr_closed and pr_opened ids must not collide for the same PR")
}
