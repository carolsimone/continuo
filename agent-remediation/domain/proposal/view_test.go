package proposal_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/carolsimone/continuo/agent-remediation/domain/proposal"
)

// TestView_FixedNodeIDs_NamesOnlyTheNodesTheAttemptActuallyFixed covers the
// mixed batch, which is the ordinary shape once one attempt addresses a whole
// failing set: some nodes were repaired and are carried by the pull request,
// others were skipped or failed and are not. Only the repaired ones may be
// named as fixed, or the PR would be attached to a rejection it does nothing
// about.
func TestView_FixedNodeIDs_NamesOnlyTheNodesTheAttemptActuallyFixed(t *testing.T) {
	v := proposal.View{
		ResolvedNodeIDs: []string{"s.a", "s.b", "s.c"},
		NodeOutcomes: map[string]proposal.NodeOutcome{
			"s.a": {Status: proposal.StatusProposed},
			"s.b": {Status: proposal.StatusSkipped, Reason: "no source"},
			"s.c": {Status: proposal.StatusFailed, Reason: "nothing to verify"},
		},
	}

	assert.Equal(t, []string{"s.a"}, v.FixedNodeIDs())
}

// TestView_FixedNodeIDs_KeepsResolvedOrder pins that the fixed set follows the
// resolved set's own (sorted) order rather than map iteration order, so the
// events built from it are byte-identical across runs.
func TestView_FixedNodeIDs_KeepsResolvedOrder(t *testing.T) {
	v := proposal.View{
		ResolvedNodeIDs: []string{"s.a", "s.b", "s.c"},
		NodeOutcomes: map[string]proposal.NodeOutcome{
			"s.c": {Status: proposal.StatusProposed},
			"s.a": {Status: proposal.StatusProposed},
			"s.b": {Status: proposal.StatusProposed},
		},
	}

	assert.Equal(t, []string{"s.a", "s.b", "s.c"}, v.FixedNodeIDs())
}

// TestView_FixedNodeIDs_FallsBackToTheResolvedSet covers a row carrying no
// per-node outcomes at all — one written before the column existed, or one
// whose outcomes were never populated. There is nothing to filter on, so the
// whole resolved set is reported exactly as before.
func TestView_FixedNodeIDs_FallsBackToTheResolvedSet(t *testing.T) {
	v := proposal.View{ResolvedNodeIDs: []string{"s.a", "s.b"}}
	assert.Equal(t, []string{"s.a", "s.b"}, v.FixedNodeIDs())

	v.NodeOutcomes = map[string]proposal.NodeOutcome{}
	assert.Equal(t, []string{"s.a", "s.b"}, v.FixedNodeIDs())
}

// TestView_FixedNodeIDs_EmptyWhenNothingWasProposed pins that a row whose
// every node ended skipped or failed reports no fixed node, rather than
// silently falling back to the whole set: outcomes exist and none of them is a
// fix.
func TestView_FixedNodeIDs_EmptyWhenNothingWasProposed(t *testing.T) {
	v := proposal.View{
		ResolvedNodeIDs: []string{"s.a", "s.b"},
		NodeOutcomes: map[string]proposal.NodeOutcome{
			"s.a": {Status: proposal.StatusSkipped},
			"s.b": {Status: proposal.StatusFailed},
		},
	}
	assert.Empty(t, v.FixedNodeIDs())
}
