package event

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPROpenedEventIDVariesByAttempt(t *testing.T) {
	a1 := PROpenedEventID("r1", 1)
	a1b := PROpenedEventID("r1", 1)
	a2 := PROpenedEventID("r1", 2)
	require.Equal(t, a1, a1b, "same (release,attempt) must be stable")
	require.NotEqual(t, a1, a2, "different attempt must differ")
}

func TestPROpenedEventIDDiffersAcrossInputs(t *testing.T) {
	id1 := PROpenedEventID("r-1", 1)
	id2 := PROpenedEventID("r-2", 1)
	id3 := PROpenedEventID("r-1", 2)
	require.NotEqual(t, id1, id2, "different releaseID must differ")
	require.NotEqual(t, id1, id3, "different attempt must differ")
}

func TestPROpenedJSON(t *testing.T) {
	b, _ := json.Marshal(PROpened{
		ProposalID:      "p1",
		ReleaseID:       "r-1",
		NodeID:          "n",
		ResolvedNodeIDs: []string{"n"},
		PrURL:           "u",
		PrNumber:        7,
		OpenedBy:        "dev|local",
		OpenedAt:        "2026-06-24T00:00:00Z",
	})
	require.JSONEq(t, `{"proposal_id":"p1","release_id":"r-1","node_id":"n","resolved_node_ids":["n"],"pr_url":"u","pr_number":7,"opened_by":"dev|local","opened_at":"2026-06-24T00:00:00Z"}`, string(b))
}
