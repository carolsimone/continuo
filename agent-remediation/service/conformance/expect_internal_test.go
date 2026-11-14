package conformance

import (
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/carolsimone/continuo/agent-remediation/domain/remediation"
	"github.com/carolsimone/continuo/agent-remediation/domain/workflow"
)

// rowsWorld is a world whose store lists the given journal rows, in the order
// given.
type rowsWorld struct {
	World
	rows []remediation.StepRun
}

func (w rowsWorld) Steps(uuid.UUID) []remediation.StepRun { return w.rows }

// The journal of a cluster that played several rounds holds one row of its
// prompt step per round. A read picks the row of the round it asks for, so
// its answer is the same in whatever order the store lists the rows.
func TestExpectation_ReadsTheRowOfTheRoundItAsksForInEitherOrder(t *testing.T) {
	id := uuid.New()
	const prompt = "dbt-compile"
	round1 := remediation.StepRun{RemediationID: id, Try: 1, Cluster: 1, Round: 1, Step: prompt, Status: remediation.StepDone, Failures: 1}
	round2 := remediation.StepRun{RemediationID: id, Try: 1, Cluster: 1, Round: 2, Step: prompt, Status: remediation.StepFailed, Failures: 3}
	typ := workflow.Type{Agent: workflow.Agent{Prompt: workflow.StepRef{Name: prompt}}}

	for _, rows := range [][]remediation.StepRun{{round1, round2}, {round2, round1}} {
		e := (&scenarioRun{w: rowsWorld{rows: rows}, id: id}).expect(remediation.Trigger{})

		assert.True(t, e.reachedModel(remediation.Cluster{Try: 1, Cluster: 1}, typ), "round 1's prompt step ran")
		first, ok := e.stepRow(1, 1, 1, prompt)
		require.True(t, ok)
		assert.Equal(t, 1, first.Failures)
		second, ok := e.stepRow(1, 1, 2, prompt)
		require.True(t, ok)
		assert.Equal(t, 3, second.Failures)
		_, ok = e.stepRow(1, 1, 3, prompt)
		assert.False(t, ok)
		assert.NoError(t, e.err())
	}
}

// Two rows of one step in one round of one cluster cannot both be the row a
// read asks for, so the read records the problem instead of answering with
// whichever row the store listed last.
func TestExpectation_TwoRowsOfOneStepInOneRoundAreAProblem(t *testing.T) {
	id := uuid.New()
	row := remediation.StepRun{RemediationID: id, Try: 1, Cluster: 1, Round: 1, Step: "dbt-compile", Status: remediation.StepDone}
	e := (&scenarioRun{w: rowsWorld{rows: []remediation.StepRun{row, row}}, id: id}).expect(remediation.Trigger{})

	_, _ = e.stepRow(1, 1, 1, "dbt-compile")

	require.Error(t, e.err())
	assert.Contains(t, e.err().Error(), "try 1 cluster 1 round 1 has two rows of step dbt-compile")
}
