package runner

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/carolsimone/continuo/agent-remediation/domain/remediation"
	"github.com/carolsimone/continuo/agent-remediation/domain/workflow"
)

func TestRationaleFactsFor_CleansEachMembersErrorLine(t *testing.T) {
	trig := remediation.Trigger{Source: "compile", Nodes: []remediation.TriggerNode{{
		NodeID: "model.a", Service: "svc-a", ErrorExcerpt: "connect failed: password=hunter2 for user 'bob'",
	}}}
	c := remediation.Cluster{Kind: workflow.ClusterSingle, TargetNodeID: "model.a", Members: []string{"model.a"}}

	f := rationaleFactsFor(trig, c, remediation.Draft{})

	require.Len(t, f.Members, 1)
	line := f.Members[0].ErrorLine
	assert.NotContains(t, line, "hunter2", "an unquoted secret of the raw error line never reaches the pull request")
	assert.Contains(t, line, "password=[redacted:secret]")
	assert.NotContains(t, line, "bob", "a quoted data value is still masked")
}
