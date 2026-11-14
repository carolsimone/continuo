package runner

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/carolsimone/continuo/agent-remediation/domain/workflow"
	"github.com/carolsimone/continuo/agent-remediation/service/runner/step"
)

func TestRegistry_RegistersTheCrossClusterChecksAndTheGate(t *testing.T) {
	r := NewRegistry(Collaborators{})
	for _, name := range []string{"edits_map_to_a_service", "no_path_edited_twice", "one_manifest_kind"} {
		s, m, ok := r.Lookup(workflow.StepName(name))
		require.True(t, ok, name)
		assert.Equal(t, step.KindCheck, m.Kind, name)
		assert.Equal(t, []workflow.Stage{workflow.StageCrossCluster}, m.Stages, name)
		assert.False(t, m.MayStandAsGateRun, name)
		assert.Implements(t, (*step.CrossCheck)(nil), s, name)
	}
	g, m, ok := r.Lookup("verification_run")
	require.True(t, ok)
	assert.Equal(t, step.KindCheck, m.Kind)
	assert.Equal(t, []workflow.Stage{workflow.StageGate}, m.Stages)
	assert.True(t, m.MayStandAsGateRun)
	assert.Implements(t, (*step.LongCheck)(nil), g)
}
