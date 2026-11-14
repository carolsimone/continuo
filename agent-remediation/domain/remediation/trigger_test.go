package remediation

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/carolsimone/continuo/agent-remediation/domain/workflow"
)

func TestFailure_PrecedentSubjectIsTheFirstMemberOfAnUpstreamCluster(t *testing.T) {
	target := TriggerNode{NodeID: "model.up"}
	first := TriggerNode{NodeID: "model.a"}
	second := TriggerNode{NodeID: "model.b"}
	for _, kind := range []workflow.ClusterKind{workflow.ClusterSharedUpstream, workflow.ClusterCrossService} {
		f := Failure{Kind: kind, Target: target, Members: []TriggerNode{first, second}}
		assert.Equal(t, first, f.PrecedentSubject(), kind)
	}
}

func TestFailure_PrecedentSubjectIsTheTargetOfASingleCluster(t *testing.T) {
	target := TriggerNode{NodeID: "model.a"}
	f := Failure{Kind: workflow.ClusterSingle, Target: target, Members: []TriggerNode{target}}
	assert.Equal(t, target, f.PrecedentSubject())
}

func TestFailure_PrecedentSubjectFallsBackToTheTargetWithoutMembers(t *testing.T) {
	target := TriggerNode{NodeID: "model.up"}
	f := Failure{Kind: workflow.ClusterSharedUpstream, Target: target}
	assert.Equal(t, target, f.PrecedentSubject())
}

func TestTrigger_NodeIDsAreSortedAndLeaveNodesAlone(t *testing.T) {
	tr := Trigger{Nodes: []TriggerNode{{NodeID: "b"}, {NodeID: "a"}, {NodeID: "c"}}}
	assert.Equal(t, []string{"a", "b", "c"}, tr.NodeIDs())
	assert.Equal(t, "b", tr.Nodes[0].NodeID)
}

func TestTrigger_ServicesAreSortedUniqueAndNonEmpty(t *testing.T) {
	tr := Trigger{Nodes: []TriggerNode{{Service: "sales"}, {Service: ""}, {Service: "finance"}, {Service: "sales"}}}
	assert.Equal(t, []string{"finance", "sales"}, tr.Services())
	assert.Empty(t, Trigger{}.Services())
}
