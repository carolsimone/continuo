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
	RunID() uuid.UUID
}

// RunFinalized is recorded by Run.RecordAttemptReport when every task of the
// run has ended, by Run.Start when the plan holds no pending task, by
// Run.Cancel of a run with nothing in flight, by Run.FailStalled and by
// Run.MarkDispatchTerminal. It maps to no stream: the run's terminal status is
// its row.
type RunFinalized struct {
	ID      uuid.UUID
	Name    string
	Outcome RunStatus
}

func (RunFinalized) runDomainEvent()    {}
func (e RunFinalized) RunID() uuid.UUID { return e.ID }

// RunCancelled is the cancel of a run: who cancelled it, why, and the
// attempts state still waits for. Run.Cancel records it naming no attempt;
// StopQuestion.Cancel re-sends it naming each attempt asked to stop that has
// not answered. Translated to run.cancelled:v1.
type RunCancelled struct {
	ID                 uuid.UUID
	TenantID           string
	Name               string
	By                 string
	CancellationReason string
	Attempts           []StopTarget
}

func (RunCancelled) runDomainEvent()    {}
func (e RunCancelled) RunID() uuid.UUID { return e.ID }

// TaskDispatched is recorded for each attempt of a task state dispatches: by
// Run.Start for each ready task, by Run.RecordAttemptReport for each task a
// success makes ready and for the next attempt of a task a retryable failure
// leaves with retries left. The attempt reconcile produces one too, through
// Question.Dispatch, each time state asks about an attempt that has gone
// quiet: it carries the status state knows the attempt in, and the attempt's
// next question waits for the next window. It carries the run, the task and
// the attempt's number, state's view of the attempt (KnownStatus: dispatched
// until execution-controller acknowledged it), the node it builds as the run's
// release holds it, the attempt's Kubernetes Job name, the dbt verb and how
// long the attempt's container may run. TenantID is the tenant of the run,
// which the event's outbox row stores. No retry budget travels: state decides
// every retry.
type TaskDispatched struct {
	ID           uuid.UUID
	TenantID     string
	ScheduleName string
	TaskID       uuid.UUID
	Attempt      int32
	KnownStatus  AttemptStatus
	Node         topology.Node
	JobName      string
	Operation    model.Operation
	Timeout      time.Duration
}

func (TaskDispatched) runDomainEvent()    {}
func (e TaskDispatched) RunID() uuid.UUID { return e.ID }

// RunDispatchTerminal is recorded by Run.MarkDispatchTerminal next to the
// RunFinalized that carries the outcome (skipped for a benign no_tests
// dispatch, otherwise failed). It names why the dispatch ended the run. It maps
// to no stream and no column: the activation handler logs its Reason at WARN.
type RunDispatchTerminal struct {
	ID     uuid.UUID
	Name   string
	Reason string
}

func (RunDispatchTerminal) runDomainEvent()    {}
func (e RunDispatchTerminal) RunID() uuid.UUID { return e.ID }
