package runner

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/carolsimone/continuo/agent-remediation/domain/workflow"
)

// flowsNamed is a set of flows with the given names and no types.
func flowsNamed(names ...string) workflow.Set {
	set := workflow.Set{Flows: map[string]workflow.Flow{}, Types: map[string]workflow.Type{}}
	for _, name := range names {
		set.Flows[name] = workflow.Flow{Name: name}
	}
	return set
}

// Every heal trigger runs one flow, so each other flow the files define is
// noted, in name order, and the flow the triggers run is not.
func TestFlowsNoTriggerRuns_NotesEveryFlowButTheTriggers(t *testing.T) {
	set := flowsNamed("my-flow", flowRejectedRelease, "another-flow")

	trigger, err := TriggerFlow(set)
	require.NoError(t, err)
	assert.Equal(t, flowRejectedRelease, trigger.Name)

	assert.Equal(t, []string{
		"flow another-flow: no heal trigger runs it",
		"flow my-flow: no heal trigger runs it",
	}, FlowsNoTriggerRuns(set))
}

func TestFlowsNoTriggerRuns_TheTriggersFlowAloneHasNoNote(t *testing.T) {
	assert.Empty(t, FlowsNoTriggerRuns(flowsNamed(flowRejectedRelease)))
	assert.Empty(t, FlowsNoTriggerRuns(workflow.Set{}))
}
