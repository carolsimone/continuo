package run

import (
	"testing"
	"time"

	"github.com/carolsimone/continuo/pkg/domain/model"
	"github.com/carolsimone/continuo/state/domain/topology"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

// A question is the attempt's dispatch again, its known status the one state
// holds, its timeout its node's; the next one waits for the next window.
func TestQuestion_DispatchCarriesTheKnownStatusAndTheNextWindow(t *testing.T) {
	declared := 5400
	q := Question{RunID: uuid.New(), ScheduleName: "daily", TaskID: uuid.New(), Attempt: 1, Status: AttemptDispatched,
		ChecksSent: 1, JobName: "svc-sch-tbl-7f3e1a2b-r1", Node: topology.Node{Key: "sch.tbl", TimeoutSeconds: &declared},
		Operation: model.OperationRun, MaxRetries: 2}

	d, next := q.Dispatch(TaskDefaults{MaxRetries: 2, Timeout: 24 * time.Hour})

	assert.Equal(t, TaskDispatched{ID: q.RunID, ScheduleName: "daily", TaskID: q.TaskID, Attempt: 1, KnownStatus: AttemptDispatched,
		Node: q.Node, JobName: q.JobName, Operation: model.OperationRun, MaxRetries: 2, Timeout: 90 * time.Minute}, d)
	assert.Equal(t, 20*time.Minute, next, "the second question about an unacknowledged attempt waits 20 minutes for the third")

	q.Status, q.ChecksSent = AttemptRunning, 4
	d, next = q.Dispatch(TaskDefaults{Timeout: 24 * time.Hour})
	assert.Equal(t, AttemptRunning, d.KnownStatus)
	assert.Equal(t, 30*time.Minute, next)
}
