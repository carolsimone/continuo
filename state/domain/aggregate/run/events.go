package run

import (
	"time"

	"github.com/carolsimone/continuo/pkg/domain/model"
	"github.com/carolsimone/continuo/state/domain/topology"
	"github.com/google/uuid"
)

// DomainEvent is the marker interface for every event a Run aggregate records
// during mutation. The OutboxPublisher adapter dispatches on the concrete type
// to derive stream name, event_type, wire-format payload, and retry budget.
//
// The marker method runDomainEvent() is unexported so external packages cannot
// add their own events.
type DomainEvent interface {
	runDomainEvent()
	ScheduleID() uuid.UUID
}

// RunFinalized is recorded by Run.RecordTaskStatus when the run reaches its
// terminal status through the finalization decision, by Run.Start when the
// plan holds no pending task, by Run.Cancel and by Run.MarkDispatchTerminal.
// It maps to no stream: the run's terminal status is its row.
type RunFinalized struct {
	ID      uuid.UUID
	Name    string
	Outcome SchedulerStatus
}

func (RunFinalized) runDomainEvent()         {}
func (e RunFinalized) ScheduleID() uuid.UUID { return e.ID }

// RunCancelled is recorded by Run.Cancel. Translated to schedule.cancelled:v1.
type RunCancelled struct {
	ID                 uuid.UUID
	Name               string
	By                 string
	CancellationReason string
}

func (RunCancelled) runDomainEvent()         {}
func (e RunCancelled) ScheduleID() uuid.UUID { return e.ID }

// TaskDispatched is recorded for each attempt of a task state dispatches: by
// Run.Start for each ready task, by Run.RecordAttemptReport and
// Run.RecordTaskStatus for each task a success makes ready, and by
// Run.RecordAttemptReport for the next attempt of a task a retryable failure
// leaves with retries left. It carries the run, the task and the attempt's
// number, state's view of the attempt (KnownStatus: dispatched until
// execution-controller acknowledged it), the node it builds as the run's
// release holds it, the attempt's Kubernetes Job name, the dbt verb, the
// task's retry budget and how long the attempt's container may run.
type TaskDispatched struct {
	ID           uuid.UUID
	ScheduleName string
	TaskID       uuid.UUID
	Attempt      int32
	KnownStatus  AttemptStatus
	Node         topology.Node
	JobName      string
	Operation    model.Operation
	MaxRetries   int32
	Timeout      time.Duration
}

func (TaskDispatched) runDomainEvent()         {}
func (e TaskDispatched) ScheduleID() uuid.UUID { return e.ID }

// RunDispatchTerminal is recorded by Run.MarkDispatchTerminal next to the
// RunFinalized that carries the outcome (skipped for a benign no_tests
// dispatch, otherwise failed). It names why the dispatch ended the run. It maps
// to no stream and no column: the activation handler logs its Reason at WARN.
type RunDispatchTerminal struct {
	ID     uuid.UUID
	Name   string
	Reason string
}

func (RunDispatchTerminal) runDomainEvent()         {}
func (e RunDispatchTerminal) ScheduleID() uuid.UUID { return e.ID }
