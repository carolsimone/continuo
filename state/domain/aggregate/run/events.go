package run

import (
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

// TaskDispatched is recorded by Run.Start for each task it dispatches, and by
// Run.RecordTaskStatus for each task a success makes ready: the
// run, the task, the node it builds as the run's release holds it, its
// Kubernetes Job name, the dbt verb and the task's retry budget. Translated to
// query.model:v1.
type TaskDispatched struct {
	ID           uuid.UUID
	ScheduleName string
	TaskID       uuid.UUID
	Node         topology.Node
	JobName      string
	Operation    model.Operation
	MaxRetries   int32
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
