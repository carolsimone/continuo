package event

import (
	"encoding/json"
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
// service to the exact id the pre-Phase-4 two-arg body produced.
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
	// service and edits are additive and omitted when empty.
	require.NotContains(t, m, "service")
	require.NotContains(t, m, "edits")
}

// TestPRClosed_ServiceAndEditsPresent verifies the additive service field and
// the per-edit close payload (path, target node, amended flag, diff) are
// carried on the wire when set.
func TestPRClosed_ServiceAndEditsPresent(t *testing.T) {
	body, err := json.Marshal(PRClosed{
		ProposalID: "p1", ReleaseID: "rel-1", NodeID: "model.core.a",
		ResolvedNodeIDs: []string{"model.core.a"},
		Service:         "core",
		PrURL:           "http://gh/pull/7", PrNumber: 7,
		Outcome: "merged", ClosedAt: "2026-07-03T00:00:00Z",
		Edits: []ClosedEdit{
			{Path: "services/core/models/a.sql", TargetNodeID: "model.core.a", Amended: true, Diff: "@@ -1 +1 @@"},
		},
	})
	require.NoError(t, err)
	var payload PRClosed
	require.NoError(t, json.Unmarshal(body, &payload))
	require.Equal(t, "core", payload.Service)
	require.Len(t, payload.Edits, 1)
	require.Equal(t, "services/core/models/a.sql", payload.Edits[0].Path)
	require.Equal(t, "model.core.a", payload.Edits[0].TargetNodeID)
	require.True(t, payload.Edits[0].Amended)
	require.Equal(t, "@@ -1 +1 @@", payload.Edits[0].Diff)
}
