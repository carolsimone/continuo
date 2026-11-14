package runner

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/carolsimone/continuo/agent-remediation/service/runner/step"
)

func TestRegistry_RegistersTheTwoOutputs(t *testing.T) {
	r := NewRegistry(Collaborators{})

	s, m, ok := r.Lookup("pull_request")
	require.True(t, ok)
	assert.Equal(t, step.KindOutput, m.Kind)
	assert.Empty(t, m.Needs)
	assert.Implements(t, (*step.PassOutput)(nil), s)

	s, m, ok = r.Lookup("hand_over")
	require.True(t, ok)
	assert.Equal(t, step.KindOutput, m.Kind)
	assert.Empty(t, m.Needs)
	assert.Implements(t, (*step.GiveUpOutput)(nil), s)
}
