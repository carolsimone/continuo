package run

import (
	"github.com/carolsimone/continuo/pkg/domain/model"
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

// RunStarted is recorded by NewPendingRun (cron + manual trigger).
// Translated by the publisher to a scheduler.started:v1 outbox row.
type RunStarted struct {
	ID              uuid.UUID
	Name            string
	K               Kind
	SourceID        *uuid.UUID
	InitiatedBy     string
	ServiceMetadata map[string]ServiceMetadata
	// Operation is the dbt verb requested for a whole-DAG run ("" | "test" |
	// "build"). Only a manual TriggerSchedule caller passes a non-default
	// value; cron and other activate.Handle callers always pass OperationRun.
	// State does not act on it — it only forwards the value onto
	// scheduler.started:v1 for the orchestrator to build the run projection.
	Operation model.Operation
}

func (RunStarted) runDomainEvent()         {}
func (e RunStarted) ScheduleID() uuid.UUID { return e.ID }

// RunFinalized is recorded by Run.RecordTaskStatus when the run reaches its
// terminal status through the finalization decision, by Run.AcceptDispatch
// when every projected task is already terminal (auto-rollup), and by
// Run.MarkDispatchTerminal. Translated to run.finalized:v1.
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

// RerunRequested is recorded by NewDerivedRun when Kind == KindRerun.
// Translated to trigger.rerun:v1.
type RerunRequested struct {
	ID          uuid.UUID
	Name        string
	SourceID    uuid.UUID
	InitiatedBy string
}

func (RerunRequested) runDomainEvent()         {}
func (e RerunRequested) ScheduleID() uuid.UUID { return e.ID }

// RebaseRequested is recorded by NewDerivedRun when Kind == KindRebase.
// Translated to trigger.rebase:v1.
type RebaseRequested struct {
	ID          uuid.UUID
	Name        string
	SourceID    uuid.UUID
	InitiatedBy string
}

func (RebaseRequested) runDomainEvent()         {}
func (e RebaseRequested) ScheduleID() uuid.UUID { return e.ID }

// SingleNodeRunRequested is recorded by NewSingleNodeRun.
// Translated to trigger.single_node_run:v1.
type SingleNodeRunRequested struct {
	ID             uuid.UUID
	Name           string
	Target         NodeID
	MetadataSource MetadataSource
	Operation      model.Operation
	SourceID       *uuid.UUID
	InitiatedBy    string
}

func (SingleNodeRunRequested) runDomainEvent()         {}
func (e SingleNodeRunRequested) ScheduleID() uuid.UUID { return e.ID }

// RunDispatchTerminal is recorded by Run.MarkDispatchTerminal for observability.
// The publisher emits RunFinalized (status=skipped for a benign no_tests
// dispatch, otherwise failed) onto run.finalized:v1; this auxiliary event is
// informational and currently has no downstream stream. Kept so a future
// "dispatch outcome reason" stream is mechanical to add.
type RunDispatchTerminal struct {
	ID     uuid.UUID
	Name   string
	Reason string
}

func (RunDispatchTerminal) runDomainEvent()         {}
func (e RunDispatchTerminal) ScheduleID() uuid.UUID { return e.ID }
