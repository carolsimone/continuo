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
	for _, kind := range []workflow.ClusterKind{workflow.ClusterSharedUpstream, workflow.ClusterCrossProject} {
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

func TestTrigger_ProjectsAreSortedUniqueAndNonEmpty(t *testing.T) {
	tr := Trigger{Nodes: []TriggerNode{{Project: "sales"}, {Project: ""}, {Project: "finance"}, {Project: "sales"}}}
	assert.Equal(t, []string{"finance", "sales"}, tr.Projects())
	assert.Empty(t, Trigger{}.Projects())
}

func TestFailure_ArtifactPrefixNamesTheReleaseNodeRoundAndTry(t *testing.T) {
	f := Failure{Trigger: Trigger{ReleaseID: "rel-1", Round: 2}, Try: 3}
	assert.Equal(t, "proposed-fix/rel-1/model.a/round-2/try-3", f.ArtifactPrefix("model.a"))
	other := Failure{Trigger: Trigger{ReleaseID: "rel-1", Round: 1}, Try: 3}
	assert.NotEqual(t, f.ArtifactPrefix("model.a"), other.ArtifactPrefix("model.a"),
		"try 3 of two rounds stores its artifacts apart")
}
