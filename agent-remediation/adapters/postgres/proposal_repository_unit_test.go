package postgres

import (
	"testing"

	"github.com/carolsimone/continuo/agent-remediation/domain/proposal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestFileEditsRoundTripCarryTargetNode verifies that TargetNodeID — the node
// whose source one edit changes — round-trips through the file_edits JSONB
// column alongside the existing path/content/diff fields.
func TestFileEditsRoundTripCarryTargetNode(t *testing.T) {
	raw, err := marshalFileEdits([]proposal.FileEdit{{Path: "a.sql", ContentURI: "c", DiffURI: "d", TargetNodeID: "s.u"}})
	require.NoError(t, err)
	got := editsOrLegacy(raw, "", "", "")
	require.Len(t, got, 1)
	assert.Equal(t, "s.u", got[0].TargetNodeID)
}

// TestNodeOutcomesAndVerificationsRoundTrip verifies the codecs for the two
// new batched-attempt columns: node_outcomes (one outcome per failing node)
// and verifications (one shadow release per edited service), and that an
// empty verifications array decodes to nil rather than an empty non-nil slice.
func TestNodeOutcomesAndVerificationsRoundTrip(t *testing.T) {
	outcomes := map[string]proposal.NodeOutcome{"s.a": {Status: proposal.StatusVerifying}, "s.b": {Status: proposal.StatusSkipped, Reason: "no source"}}
	raw, err := marshalNodeOutcomes(outcomes)
	require.NoError(t, err)
	assert.Equal(t, outcomes, unmarshalNodeOutcomes(raw))

	vs := []proposal.Verification{{Service: "svc", Kind: "dbt", ShadowReleaseID: "shadow-r-svc-a1"}}
	rawV, err := marshalVerifications(vs)
	require.NoError(t, err)
	assert.Equal(t, vs, unmarshalVerifications(rawV))
	assert.Nil(t, unmarshalVerifications([]byte("[]")))
}

// TestResolvedNodeIDsDefaultsToNodeIDForLegacyRows verifies that a row written
// before resolved_node_ids existed (an empty JSON array) reads back as one
// entry synthesized from the legacy node_id column, while a row that already
// carries a batched resolved_node_ids array is returned unchanged.
func TestResolvedNodeIDsDefaultsToNodeIDForLegacyRows(t *testing.T) {
	assert.Equal(t, []string{"s.n"}, resolvedOrLegacy([]byte("[]"), "s.n"))
	assert.Equal(t, []string{"s.a", "s.b"}, resolvedOrLegacy([]byte(`["s.a","s.b"]`), "s.a"))
}
