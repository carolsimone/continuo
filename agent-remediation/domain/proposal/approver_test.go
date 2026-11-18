package proposal_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/carolsimone/continuo/agent-remediation/domain/proposal"
)

func TestMergeApprovers_AddedTakeThePlaceOfTheirNodesAndTheResultIsSortedByNode(t *testing.T) {
	existing := []proposal.Approver{
		{NodeID: "s.b", Owner: "finance", Source: proposal.ApproverSourceBundle},
		{NodeID: "s.d", Owner: "", Source: proposal.ApproverSourceNone},
	}
	added := []proposal.Approver{
		{NodeID: "s.c", Owner: "growth", Source: proposal.ApproverSourceContractFile},
		{NodeID: "s.d", Owner: "ops", Source: proposal.ApproverSourceBundle},
		{NodeID: "s.a", Owner: "sales", Source: proposal.ApproverSourceBundle},
	}

	got := proposal.MergeApprovers(existing, added)

	assert.Equal(t, []proposal.Approver{
		{NodeID: "s.a", Owner: "sales", Source: proposal.ApproverSourceBundle},
		{NodeID: "s.b", Owner: "finance", Source: proposal.ApproverSourceBundle},
		{NodeID: "s.c", Owner: "growth", Source: proposal.ApproverSourceContractFile},
		{NodeID: "s.d", Owner: "ops", Source: proposal.ApproverSourceBundle},
	}, got)
	assert.Equal(t, "finance", existing[0].Owner, "the slices it is given are left as they are")
	assert.Equal(t, "s.c", added[0].NodeID)
	assert.Len(t, existing, 2)
}

func TestMergeApprovers_NothingMergedIsNil(t *testing.T) {
	assert.Nil(t, proposal.MergeApprovers(nil, nil))
	assert.Nil(t, proposal.MergeApprovers([]proposal.Approver{}, []proposal.Approver{}))
}

func TestApproversForNodes_KeepsTheApproversOfTheNodesInTheirOrder(t *testing.T) {
	approvers := []proposal.Approver{
		{NodeID: "s.a", Owner: "sales", Source: proposal.ApproverSourceBundle},
		{NodeID: "s.b", Owner: "finance", Source: proposal.ApproverSourceBundle},
		{NodeID: "s.c", Source: proposal.ApproverSourceNone},
	}

	assert.Equal(t, []proposal.Approver{approvers[0], approvers[2]}, proposal.ApproversForNodes(approvers, []string{"s.c", "s.a", "s.z"}))
	assert.Nil(t, proposal.ApproversForNodes(approvers, nil))
	assert.Nil(t, proposal.ApproversForNodes(nil, []string{"s.a"}))
}
