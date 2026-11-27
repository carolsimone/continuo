package fix_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/carolsimone/continuo/agent-remediation/domain/fix"
	"github.com/carolsimone/continuo/pkg/domain/model"
)

func TestMergeApprovers_AddedTakeThePlaceOfTheirNodesAndTheResultIsSortedByNode(t *testing.T) {
	existing := []fix.Approver{
		{NodeID: "s.b", Owner: "finance", Source: model.ApproverSourceBundle},
		{NodeID: "s.d", Owner: "", Source: model.ApproverSourceNone},
	}
	added := []fix.Approver{
		{NodeID: "s.c", Owner: "growth", Source: model.ApproverSourceContractFile},
		{NodeID: "s.d", Owner: "ops", Source: model.ApproverSourceBundle},
		{NodeID: "s.a", Owner: "sales", Source: model.ApproverSourceBundle},
	}

	got := fix.MergeApprovers(existing, added)

	assert.Equal(t, []fix.Approver{
		{NodeID: "s.a", Owner: "sales", Source: model.ApproverSourceBundle},
		{NodeID: "s.b", Owner: "finance", Source: model.ApproverSourceBundle},
		{NodeID: "s.c", Owner: "growth", Source: model.ApproverSourceContractFile},
		{NodeID: "s.d", Owner: "ops", Source: model.ApproverSourceBundle},
	}, got)
	assert.Equal(t, "finance", existing[0].Owner, "the slices it is given are left as they are")
	assert.Equal(t, "s.c", added[0].NodeID)
	assert.Len(t, existing, 2)
}

func TestMergeApprovers_NothingMergedIsNil(t *testing.T) {
	assert.Nil(t, fix.MergeApprovers(nil, nil))
	assert.Nil(t, fix.MergeApprovers([]fix.Approver{}, []fix.Approver{}))
}

func TestApproversForNodes_KeepsTheApproversOfTheNodesInTheirOrder(t *testing.T) {
	approvers := []fix.Approver{
		{NodeID: "s.a", Owner: "sales", Source: model.ApproverSourceBundle},
		{NodeID: "s.b", Owner: "finance", Source: model.ApproverSourceBundle},
		{NodeID: "s.c", Source: model.ApproverSourceNone},
	}

	assert.Equal(t, []fix.Approver{approvers[0], approvers[2]}, fix.ApproversForNodes(approvers, []string{"s.c", "s.a", "s.z"}))
	assert.Nil(t, fix.ApproversForNodes(approvers, nil))
	assert.Nil(t, fix.ApproversForNodes(nil, []string{"s.a"}))
}
