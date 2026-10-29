package run

import (
	"slices"
	"time"

	"github.com/carolsimone/continuo/pkg/domain/model"
	"github.com/carolsimone/continuo/run-controller/domain/topology"
	"github.com/google/uuid"
)

// Question is an attempt state asks execution-controller about because
// nothing was heard of it within its window: the attempt, its status as
// state last knew it, how often it has been asked about, and what its
// dispatch carries.
type Question struct {
	RunID        uuid.UUID
	TenantID     string
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
		TenantID:     q.TenantID,
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

// StopQuestion is the cancel of a cancelling run state sends again because
// attempts it asked to stop have not answered within their window: the run,
// who cancelled it and why, and those attempts.
type StopQuestion struct {
	RunID        uuid.UUID
	TenantID     string
	ScheduleName string
	CancelledBy  string
	Reason       string
	// CancelledAt is when the run was cancelled.
	CancelledAt time.Time
	Attempts    []StopTarget
}

// Cancel is the question as the run's cancel, naming every attempt it waits
// for. execution-controller answers each with the attempt's stopped report
// once no pod of it can run again.
func (q StopQuestion) Cancel() RunCancelled {
	return RunCancelled{
		ID:                 q.RunID,
		TenantID:           q.TenantID,
		Name:               q.ScheduleName,
		By:                 q.CancelledBy,
		CancellationReason: q.Reason,
		Attempts:           slices.Clone(q.Attempts),
	}
}
