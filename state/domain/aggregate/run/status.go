package run

import (
	"fmt"
	"slices"
)

// SchedulerStatus is the lifecycle status of a Run.
type SchedulerStatus string

const (
	SchedulerStatusPending SchedulerStatus = "pending"
	SchedulerStatusRunning SchedulerStatus = "running"
	// SchedulerStatusCancelling is a run whose cancel has been requested and
	// whose work is still being stopped. It is active, not terminal: it ends
	// cancelled once every attempt of it has ended.
	SchedulerStatusCancelling SchedulerStatus = "cancelling"
	SchedulerStatusSucceeded  SchedulerStatus = "succeeded"
	SchedulerStatusFailed     SchedulerStatus = "failed"
	SchedulerStatusCancelled  SchedulerStatus = "cancelled"
	// SchedulerStatusSkipped is the terminal status for a run that had no work
	// to do — a Test run over nodes with no dbt tests. It is a benign non-failure.
	SchedulerStatusSkipped SchedulerStatus = "skipped"
)

func (s SchedulerStatus) IsValid() bool {
	switch s {
	case SchedulerStatusPending, SchedulerStatusRunning, SchedulerStatusCancelling, SchedulerStatusSucceeded,
		SchedulerStatusFailed, SchedulerStatusCancelled, SchedulerStatusSkipped:
		return true
	}
	return false
}

func (s SchedulerStatus) IsTerminal() bool {
	switch s {
	case SchedulerStatusSucceeded, SchedulerStatusFailed, SchedulerStatusCancelled,
		SchedulerStatusSkipped:
		return true
	}
	return false
}

// IsActive reports whether a run in status s holds its schedule: it is
// pending, running or cancelling, so no other run of the schedule starts.
func (s SchedulerStatus) IsActive() bool {
	switch s {
	case SchedulerStatusPending, SchedulerStatusRunning, SchedulerStatusCancelling:
		return true
	}
	return false
}

// schedulerStatuses lists every SchedulerStatus value.
var schedulerStatuses = []SchedulerStatus{
	SchedulerStatusPending, SchedulerStatusRunning, SchedulerStatusCancelling, SchedulerStatusSucceeded,
	SchedulerStatusFailed, SchedulerStatusCancelled, SchedulerStatusSkipped,
}

// SchedulerStatuses returns every SchedulerStatus. The CHECK constraint on
// runs.status lists exactly these.
func SchedulerStatuses() []SchedulerStatus { return slices.Clone(schedulerStatuses) }

// TerminalSchedulerStatuses returns every SchedulerStatus for which IsTerminal
// reports true, so a storage guard against terminal runs names the same set the
// aggregate checks.
func TerminalSchedulerStatuses() []SchedulerStatus {
	out := make([]SchedulerStatus, 0, len(schedulerStatuses))
	for _, s := range schedulerStatuses {
		if s.IsTerminal() {
			out = append(out, s)
		}
	}
	return out
}

// ActiveSchedulerStatuses returns every SchedulerStatus for which IsActive
// reports true, so a query for the active runs names the same set the
// aggregate checks.
func ActiveSchedulerStatuses() []SchedulerStatus {
	out := make([]SchedulerStatus, 0, len(schedulerStatuses))
	for _, s := range schedulerStatuses {
		if s.IsActive() {
			out = append(out, s)
		}
	}
	return out
}

// TaskStatus is the lifecycle status of one task within a Run. A task waits
// pending until it is ready; once state dispatches an attempt of it the task
// is dispatched, then queued while execution-controller waits for a slot,
// then running once the attempt's main container starts. A failed attempt
// with retries left dispatches the next one, so the task is failed only once
// it will not run again. A task whose run is being cancelled is stopping while
// its attempt is still open, and cancelled once the attempt has stopped.
type TaskStatus string

const (
	TaskStatusPending    TaskStatus = "pending"
	TaskStatusDispatched TaskStatus = "dispatched"
	TaskStatusQueued     TaskStatus = "queued"
	TaskStatusRunning    TaskStatus = "running"
	TaskStatusStopping   TaskStatus = "stopping"
	TaskStatusSucceeded  TaskStatus = "succeeded"
	TaskStatusFailed     TaskStatus = "failed"
	TaskStatusCancelled  TaskStatus = "cancelled"
	TaskStatusSkipped    TaskStatus = "skipped"
)

// TaskStatuses lists every TaskStatus. The CHECK constraint on
// tasks.status lists exactly these.
func TaskStatuses() []TaskStatus {
	return []TaskStatus{TaskStatusPending, TaskStatusDispatched, TaskStatusQueued, TaskStatusRunning,
		TaskStatusStopping, TaskStatusSucceeded, TaskStatusFailed, TaskStatusCancelled, TaskStatusSkipped}
}

func (t TaskStatus) IsValid() bool {
	return slices.Contains(TaskStatuses(), t)
}

// IsTerminal reports whether a task in status t will not run again in its
// current attempt. A cancelled task is terminal: it never runs again. A
// stopping task is not: its attempt is still open.
func (t TaskStatus) IsTerminal() bool {
	switch t {
	case TaskStatusSucceeded, TaskStatusFailed, TaskStatusCancelled, TaskStatusSkipped:
		return true
	}
	return false
}

// IsInFlight reports whether a task in status t has an attempt in flight: it
// is dispatched, queued, running or stopping.
func (t TaskStatus) IsInFlight() bool {
	switch t {
	case TaskStatusDispatched, TaskStatusQueued, TaskStatusRunning, TaskStatusStopping:
		return true
	}
	return false
}

// InFlightTaskStatuses returns every TaskStatus for which IsInFlight reports
// true, so a query for the tasks in flight names the same set the aggregate
// checks.
func InFlightTaskStatuses() []TaskStatus {
	all := TaskStatuses()
	out := make([]TaskStatus, 0, len(all))
	for _, t := range all {
		if t.IsInFlight() {
			out = append(out, t)
		}
	}
	return out
}

// WaitReason says what a pending task is waiting for besides its upstream
// tasks. A task that waits for nothing has no reason.
type WaitReason string

const (
	// WaitReasonNodeLease is a ready task waiting for the write lease on its
	// node, which another task holds.
	WaitReasonNodeLease WaitReason = "node_lease"
)

// WaitReasons lists every WaitReason. The CHECK constraint on
// tasks.wait_reason lists exactly these.
func WaitReasons() []WaitReason { return []WaitReason{WaitReasonNodeLease} }

func (w WaitReason) IsValid() bool { return slices.Contains(WaitReasons(), w) }

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
// constraint on runs.kind.
type Kind string

const (
	KindCron          Kind = "cron"
	KindTrigger       Kind = "trigger"
	KindRerun         Kind = "rerun"
	KindRebase        Kind = "rebase"
	KindSingleNodeRun Kind = "single_node_run"
	// KindPromoteSeed is the run that materialises the seeds a promoted release
	// changed into the production schema.
	KindPromoteSeed Kind = "promote_seed"
)

func (k Kind) IsValid() bool {
	switch k {
	case KindCron, KindTrigger, KindRerun, KindRebase, KindSingleNodeRun, KindPromoteSeed:
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
