package runner

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/carolsimone/continuo/agent-remediation/domain/flow"
)

// flowsNamed is a set of flows with the given names and no types.
func flowsNamed(names ...string) flow.Set {
	set := flow.Set{Flows: map[string]flow.Flow{}, Types: map[string]flow.Type{}}
	for _, name := range names {
		set.Flows[name] = flow.Flow{Name: name}
	}
	return set
}

// Every heal trigger runs one flow, so each other flow the files define is
// noted, in name order, and the flow the triggers run is not.
func TestFlowsNoTriggerRuns_NotesEveryFlowButTheTriggers(t *testing.T) {
	set := flowsNamed("my-flow", TriggerFlowName, "another-flow")

	trigger, err := TriggerFlow(set)
	require.NoError(t, err)
	assert.Equal(t, TriggerFlowName, trigger.Name)

	assert.Equal(t, []string{
		"flow another-flow: no heal trigger runs it",
		"flow my-flow: no heal trigger runs it",
	}, FlowsNoTriggerRuns(set))
}

func TestFlowsNoTriggerRuns_TheTriggersFlowAloneHasNoNote(t *testing.T) {
	assert.Empty(t, FlowsNoTriggerRuns(flowsNamed(TriggerFlowName)))
	assert.Empty(t, FlowsNoTriggerRuns(flow.Set{}))
}
