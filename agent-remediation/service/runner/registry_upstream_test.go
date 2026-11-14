package runner

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/carolsimone/continuo/agent-remediation/domain/remediation"
	"github.com/carolsimone/continuo/agent-remediation/domain/workflow"
	"github.com/carolsimone/continuo/agent-remediation/service/runner/step"
)

func TestRegistry_RegistersTheUpstreamFamily(t *testing.T) {
	r := NewRegistry(Collaborators{})
	want := map[string]step.Kind{
		"upstream_target": step.KindContext,
		"upstream":        step.KindPrompt,
	}
	for name, kind := range want {
		_, m, ok := r.Lookup(workflow.StepName(name))
		require.True(t, ok, name)
		assert.Equal(t, kind, m.Kind, name)
	}
}

func TestRegistry_TypesTheUpstreamTarget(t *testing.T) {
	f, ok := NewRegistry(Collaborators{}).EvidenceType(remediation.KeyUpstreamTarget)
	require.True(t, ok)
	assert.IsType(t, remediation.UpstreamTarget{}, f())
}
