package run

import (
	"time"

	"github.com/carolsimone/continuo/pkg/domain/model"
	"github.com/carolsimone/continuo/state/domain/topology"
	"github.com/google/uuid"
)

// Question is an attempt state asks execution-controller about because
// nothing was heard of it within its window: the attempt, its status as
// state last knew it, how often it has been asked about, and what its
// dispatch carries.
type Question struct {
	RunID        uuid.UUID
	ScheduleName string
	TaskID       uuid.UUID
	Attempt      int32
	Status       AttemptStatus
	ChecksSent   int
	JobName      string
	Node         topology.Node
	Operation    model.Operation
}

// Dispatch is the question as a dispatch of the attempt, its known status the
// attempt's, and how long from now the attempt is asked about again.
// execution-controller answers it with the attempt's latest report, and
// creates the attempt only when it holds no record of it and state still
// knows it as dispatched.
func (q Question) Dispatch(d TaskDefaults) (TaskDispatched, time.Duration) {
	return TaskDispatched{
		ID:           q.RunID,
		ScheduleName: q.ScheduleName,
		TaskID:       q.TaskID,
		Attempt:      q.Attempt,
		KnownStatus:  q.Status,
		Node:         q.Node,
		JobName:      q.JobName,
		Operation:    q.Operation,
		Timeout:      TaskTimeout(q.Node, d),
	}, NextCheckAfter(q.Status, q.ChecksSent+1)
}
