package postgres

import (
	"encoding/json"
	"testing"

	"github.com/carolsimone/continuo/agent-remediation/domain/proposal"
	"github.com/carolsimone/continuo/pkg/domain/model"
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
// and verifications (one fix-verification run per edited project), and that an
// empty verifications array decodes to nil rather than an empty non-nil slice.
func TestNodeOutcomesAndVerificationsRoundTrip(t *testing.T) {
	outcomes := map[string]proposal.NodeOutcome{"s.a": {Status: proposal.StatusVerifying}, "s.b": {Status: proposal.StatusSkipped, Reason: "no source"}}
	raw, err := marshalNodeOutcomes(outcomes)
	require.NoError(t, err)
	assert.Equal(t, outcomes, unmarshalNodeOutcomes(raw))

	vs := []proposal.Verification{{Project: "svc", Kind: "dbt", RunID: "verify-r-svc-a1", Phase: proposal.PhasePassed}}
	rawV, err := marshalVerifications(vs)
	require.NoError(t, err)
	assert.Equal(t, vs, unmarshalVerifications(rawV))
	assert.Nil(t, unmarshalVerifications([]byte("[]")))
}

// TestNodeOutcomesCodecCarriesTheCode verifies that the reason code of a node
// outcome round-trips through the node_outcomes codec under the key "code", and
// that an outcome with no code encodes without that key.
func TestNodeOutcomesCodecCarriesTheCode(t *testing.T) {
	outcomes := map[string]proposal.NodeOutcome{
		"s.a": {Status: proposal.StatusEscalated, Reason: "no type matches", Code: model.HandoverReasonNoType},
		"s.b": {Status: proposal.StatusProposed},
	}
	raw, err := marshalNodeOutcomes(outcomes)
	require.NoError(t, err)
	assert.Equal(t, outcomes, unmarshalNodeOutcomes(raw))

	var stored map[string]map[string]any
	require.NoError(t, json.Unmarshal(raw, &stored))
	assert.Equal(t, string(model.HandoverReasonNoType), stored["s.a"]["code"])
	assert.NotContains(t, stored["s.b"], "code")
}

// TestResolvedNodeIDsDefaultsToNodeIDForLegacyRows verifies that a row written
// before resolved_node_ids existed (an empty JSON array) reads back as one
// entry synthesized from the legacy node_id column, while a row that already
// carries a batched resolved_node_ids array is returned unchanged.
func TestResolvedNodeIDsDefaultsToNodeIDForLegacyRows(t *testing.T) {
	assert.Equal(t, []string{"s.n"}, resolvedOrLegacy([]byte("[]"), "s.n"))
	assert.Equal(t, []string{"s.a", "s.b"}, resolvedOrLegacy([]byte(`["s.a","s.b"]`), "s.a"))
}

// TestApproversCodecRoundTrips verifies the approvers column codec: each
// approver round-trips under the keys node_id, owner and source, a nil or empty
// list encodes as an empty array, and an empty or malformed blob decodes as nil.
func TestApproversCodecRoundTrips(t *testing.T) {
	approvers := []proposal.Approver{
		{NodeID: "s.a", Owner: "finance", Source: model.ApproverSourceBundle},
		{NodeID: "s.b", Source: model.ApproverSourceNone},
	}
	raw, err := marshalApprovers(approvers)
	require.NoError(t, err)
	assert.Equal(t, approvers, unmarshalApprovers(raw))

	var stored []map[string]string
	require.NoError(t, json.Unmarshal(raw, &stored))
	assert.Equal(t, map[string]string{"node_id": "s.a", "owner": "finance", "source": "bundle"}, stored[0])

	empty, err := marshalApprovers(nil)
	require.NoError(t, err)
	assert.JSONEq(t, `[]`, string(empty))
	assert.Nil(t, unmarshalApprovers(empty))
	assert.Nil(t, unmarshalApprovers([]byte("not json")))
}

// TestClaimRowNarrowsApproversToTheClaimNodes verifies that a claim reads the
// approvers of the nodes it resolves from the row's approvers, in the row's order.
func TestClaimRowNarrowsApproversToTheClaimNodes(t *testing.T) {
	approvers := []proposal.Approver{
		{NodeID: "s.a", Owner: "sales", Source: model.ApproverSourceBundle},
		{NodeID: "s.b", Owner: "finance", Source: model.ApproverSourceBundle},
		{NodeID: "s.c", Owner: "growth", Source: model.ApproverSourceBundle},
	}
	raw, err := marshalApprovers(approvers)
	require.NoError(t, err)
	outcomes, err := marshalNodeOutcomes(map[string]proposal.NodeOutcome{
		"s.a": {Status: proposal.StatusProposed},
		"s.b": {Status: proposal.StatusSkipped, Code: model.HandoverReasonDeclined},
		"s.c": {Status: proposal.StatusProposed},
	})
	require.NoError(t, err)
	row := claimRow{ResolvedNodeIDs: []byte(`["s.a","s.b","s.c"]`), NodeOutcomes: outcomes, Approvers: raw}

	claim := row.toClaim("", "branch", nil)

	assert.Equal(t, []string{"s.a", "s.c"}, claim.ResolvedNodeIDs)
	assert.Equal(t, []proposal.Approver{approvers[0], approvers[2]}, claim.Approvers)
}
