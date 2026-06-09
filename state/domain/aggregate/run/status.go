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
	ErrInvalidTransition = errors.New("invalid state transition")
)

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
