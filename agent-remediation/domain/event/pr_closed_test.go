package event

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestPRClosedEventID_Deterministic verifies the id is stable for the same
// (release, attempt) and distinct across attempts and from PROpened ids.
func TestPRClosedEventID_Deterministic(t *testing.T) {
	a := PRClosedEventID("rel-1", 1)
	b := PRClosedEventID("rel-1", 1)
	require.Equal(t, a, b, "same inputs must produce the same event id")
	require.NotEqual(t, a, PRClosedEventID("rel-1", 2))
	require.NotEqual(t, a, PROpenedEventID("rel-1", 1),
		"pr_closed and pr_opened ids must not collide for the same PR")
}

// TestPRClosed_JSONShape verifies the wire keys downstream consumers correlate on.
func TestPRClosed_JSONShape(t *testing.T) {
	body, err := json.Marshal(PRClosed{
		ProposalID: "p1", ReleaseID: "rel-1", NodeID: "model.p.orders",
		ResolvedNodeIDs: []string{"model.p.orders"},
		PrURL:           "http://gh/pull/7", PrNumber: 7,
		Outcome: "merged", ClosedAt: "2026-07-03T00:00:00Z",
	})
	require.NoError(t, err)
	var m map[string]any
	require.NoError(t, json.Unmarshal(body, &m))
	for _, k := range []string{"proposal_id", "release_id", "node_id", "resolved_node_ids", "pr_url", "pr_number", "outcome", "closed_at"} {
		require.Contains(t, m, k)
	}
}
