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
// and verifications (one fix-verification run per edited service), and that an
// empty verifications array decodes to nil rather than an empty non-nil slice.
func TestNodeOutcomesAndVerificationsRoundTrip(t *testing.T) {
	outcomes := map[string]proposal.NodeOutcome{"s.a": {Status: proposal.StatusVerifying}, "s.b": {Status: proposal.StatusSkipped, Reason: "no source"}}
	raw, err := marshalNodeOutcomes(outcomes)
	require.NoError(t, err)
	assert.Equal(t, outcomes, unmarshalNodeOutcomes(raw))

	vs := []proposal.Verification{{Service: "svc", Kind: "dbt", RunID: "verify-r-svc-a1", Phase: proposal.PhasePassed}}
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
