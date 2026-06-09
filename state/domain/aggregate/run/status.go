package run

import (
	"errors"
	"fmt"
)

// SchedulerStatus is the lifecycle status of a Run.
type SchedulerStatus string

const (
	SchedulerStatusPending   SchedulerStatus = "pending"
	SchedulerStatusRunning   SchedulerStatus = "running"
	SchedulerStatusSucceeded SchedulerStatus = "succeeded"
	SchedulerStatusFailed    SchedulerStatus = "failed"
	SchedulerStatusCancelled SchedulerStatus = "cancelled"
)

func (s SchedulerStatus) IsValid() bool {
	switch s {
	case SchedulerStatusPending, SchedulerStatusRunning, SchedulerStatusSucceeded,
		SchedulerStatusFailed, SchedulerStatusCancelled:
		return true
	}
	return false
}

func (s SchedulerStatus) IsTerminal() bool {
	switch s {
	case SchedulerStatusSucceeded, SchedulerStatusFailed, SchedulerStatusCancelled:
		return true
	}
	return false
}

// TaskStatus is the lifecycle status of one task within a Run.
type TaskStatus string

const (
	TaskStatusPending   TaskStatus = "pending"
	TaskStatusRunning   TaskStatus = "running"
	TaskStatusSucceeded TaskStatus = "succeeded"
	TaskStatusFailed    TaskStatus = "failed"
	TaskStatusCancelled TaskStatus = "cancelled"
	TaskStatusSkipped   TaskStatus = "skipped"
)

func (t TaskStatus) IsValid() bool {
	switch t {
	case TaskStatusPending, TaskStatusRunning, TaskStatusSucceeded,
		TaskStatusFailed, TaskStatusCancelled, TaskStatusSkipped:
		return true
	}
	return false
}

func (t TaskStatus) IsTerminal() bool {
	switch t {
	case TaskStatusSucceeded, TaskStatusFailed, TaskStatusSkipped:
		return true
	}
	return false
}

// ParseTaskStatus converts a wire-format status string to a TaskStatus. Returns
// an error for unknown values. Called at adapter boundaries.
func ParseTaskStatus(s string) (TaskStatus, error) {
	t := TaskStatus(s)
	if !t.IsValid() {
		return "", fmt.Errorf("unknown task status %q", s)
	}
	return t, nil
}

// CallerID identifies which service is performing a task state transition.
type CallerID string

const (
	// CallerRerunReset owns the vestigial failed→pending reset transition.
	// No live caller exercises it: reruns mint a new Run (kind=rerun) via the
	// orchestrator rather than resetting existing tasks, and the ResetTask gRPC
	// path it authorizes has no client. Retained only to keep the transition
	// table total; slated for removal with the orphaned ResetTask vertical.
	CallerRerunReset         CallerID = "rerun-reset"
	CallerExecutorController CallerID = "executor-controller"
	CallerK8sController      CallerID = "k8s-controller"
)

// Kind discriminates run kinds. Wire-stable string values, matches the CHECK
// constraint on scheduler_tracker.kind.
type Kind string

const (
	KindCron          Kind = "cron"
	KindTrigger       Kind = "trigger"
	KindRerun         Kind = "rerun"
	KindRebase        Kind = "rebase"
	KindSingleNodeRun Kind = "single_node_run"
)

func (k Kind) IsValid() bool {
	switch k {
	case KindCron, KindTrigger, KindRerun, KindRebase, KindSingleNodeRun:
		return true
	}
	return false
}

// InitStatus is the lifecycle of run initialization (topology projection).
type InitStatus string

const (
	InitStatusPending    InitStatus = "pending"
	InitStatusInProgress InitStatus = "in_progress"
	InitStatusCompleted  InitStatus = "completed"
)

// MetadataSource is the per-request choice between "latest" topology and the
// pinned snapshot of a previous run. Used by SingleNodeRun.
type MetadataSource string

const (
	MetadataSourceLatest        MetadataSource = "latest"
	MetadataSourceSnapshotOfRun MetadataSource = "snapshot_of_run"
)

// NodeID is a triple identifying one materialised model in the topology.
type NodeID struct {
	ServiceName string
	SchemaName  string
	TableName   string
}

// Transition errors returned by the aggregate's status mutation methods.
var (
	ErrInvalidTransition      = errors.New("invalid state transition")
	ErrUnauthorizedTransition = errors.New("unauthorized state transition")
)

type taskTransition struct {
	from  TaskStatus
	to    TaskStatus
	owner CallerID
}

// allowedTaskTransitions enumerates the (from,to,owner) triples permitted on
// a task. Any transition outside this table yields ErrInvalidTransition (when
// the from→to pair is unknown) or ErrUnauthorizedTransition (when the pair is
// allowed but the requesting caller does not own it).
var allowedTaskTransitions = []taskTransition{
	{TaskStatusFailed, TaskStatusPending, CallerRerunReset},
	{TaskStatusPending, TaskStatusRunning, CallerExecutorController},
	{TaskStatusFailed, TaskStatusRunning, CallerExecutorController},
	{TaskStatusRunning, TaskStatusSucceeded, CallerK8sController},
	{TaskStatusRunning, TaskStatusFailed, CallerK8sController},
}

// canTaskTransition is a pure-domain check used by Run.ResetTaskToPending and
// other in-aggregate mutators. It returns (ok, ownerOK) so callers can return
// the appropriate error variant.
func canTaskTransition(from, to TaskStatus, caller CallerID) (allowed, ownerOK bool) {
	for _, tr := range allowedTaskTransitions {
		if tr.from == from && tr.to == to {
			return true, tr.owner == caller
		}
	}
	return false, false
}

type schedulerTransition struct {
	from SchedulerStatus
	to   SchedulerStatus
}

// allowedSchedulerTransitions enumerates the non-cancel scheduler transitions.
// Cancel is a separate, always-permitted path on a non-terminal Run; see
// Run.Cancel.
var allowedSchedulerTransitions = []schedulerTransition{
	{SchedulerStatusPending, SchedulerStatusRunning},
	{SchedulerStatusRunning, SchedulerStatusSucceeded},
	{SchedulerStatusRunning, SchedulerStatusFailed},
}

func canSchedulerTransition(from, to SchedulerStatus) bool {
	for _, tr := range allowedSchedulerTransitions {
		if tr.from == from && tr.to == to {
			return true
		}
	}
	return false
}
