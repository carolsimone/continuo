// Package model is a transitional alias layer. Types previously declared here
// have migrated into state/domain/aggregate/run/ (and projection/). This file
// remains so that callers continue to compile until the migration is complete;
// it is removed in Step 7.
package model

import (
	"database/sql"
	"encoding/json"
	"time"

	"github.com/carolsimone/continuo/state/domain/aggregate/run"
	"github.com/google/uuid"
)

// SchedulerStatus is an alias for run.SchedulerStatus.
type SchedulerStatus = run.SchedulerStatus

const (
	SchedulerStatusPending   = run.SchedulerStatusPending
	SchedulerStatusRunning   = run.SchedulerStatusRunning
	SchedulerStatusSucceeded = run.SchedulerStatusSucceeded
	SchedulerStatusFailed    = run.SchedulerStatusFailed
	SchedulerStatusCancelled = run.SchedulerStatusCancelled
)

// TaskStatus is an alias for run.TaskStatus.
type TaskStatus = run.TaskStatus

const (
	TaskStatusPending   = run.TaskStatusPending
	TaskStatusRunning   = run.TaskStatusRunning
	TaskStatusSucceeded = run.TaskStatusSucceeded
	TaskStatusFailed    = run.TaskStatusFailed
	TaskStatusCancelled = run.TaskStatusCancelled
	TaskStatusSkipped   = run.TaskStatusSkipped
)

// ParseTaskStatus is an alias for run.ParseTaskStatus.
var ParseTaskStatus = run.ParseTaskStatus

// CallerID and constants alias run.CallerID.
type CallerID = run.CallerID

const (
	CallerStartupController  = run.CallerStartupController
	CallerExecutorController = run.CallerExecutorController
	CallerK8sController      = run.CallerK8sController
)

// Transition errors alias the run package's variables.
var (
	ErrInvalidTransition      = run.ErrInvalidTransition
	ErrUnauthorizedTransition = run.ErrUnauthorizedTransition
)

// SchedulerTracker and TaskTracker remain as field carriers until Steps 4-7
// fully cut over the persistence layer. They get a Transition shim each to
// preserve the legacy API surface used by gRPC reset / scheduler handlers.

type SchedulerTracker struct {
	ScheduleID           uuid.UUID                  `json:"schedule_id" db:"schedule_id"`
	ScheduleName         string                     `json:"schedule_name" db:"schedule_name"`
	Status               SchedulerStatus            `json:"status" db:"status"`
	CreatedAt            time.Time                  `json:"created_at" db:"created_at"`
	StartedAt            *time.Time                 `json:"started_at,omitempty" db:"started_at"`
	CompletedAt          *time.Time                 `json:"completed_at,omitempty" db:"completed_at"`
	LastHeartbeatAt      *time.Time                 `json:"last_heartbeat_at,omitempty" db:"last_heartbeat_at"`
	CancelledAt          *time.Time                 `json:"cancelled_at,omitempty" db:"cancelled_at"`
	CancelledBy          *string                    `json:"cancelled_by,omitempty" db:"cancelled_by"`
	CancellationReason   *string                    `json:"cancellation_reason,omitempty" db:"cancellation_reason"`
	InitializationStatus string                     `json:"initialization_status" db:"initialization_status"`
	TotalTaskCount       sql.NullInt32              `json:"total_task_count,omitempty" db:"total_task_count"`
	TerminalTaskCount    int32                      `json:"terminal_task_count" db:"terminal_task_count"`
	Kind                 string                     `json:"kind" db:"kind"`
	SourceRunID          *uuid.UUID                 `json:"source_run_id,omitempty" db:"source_run_id"`
	ServiceMetadata      map[string]ServiceMetadata `json:"service_metadata"`
	ServiceMetadataRaw   json.RawMessage            `json:"-" db:"service_metadata"`
}

// Transition is a thin shim delegating to the run package. Retained for the
// gRPC ResetTask path which still calls TaskTracker.Transition until Step 5.
func (s *SchedulerTracker) Transition(to SchedulerStatus) error {
	if canSchedulerTransitionViaRun(s.Status, to) {
		s.Status = to
		return nil
	}
	return ErrInvalidTransition
}

func (s *SchedulerTracker) GetServiceMetadata() map[string]ServiceMetadata {
	if len(s.ServiceMetadata) > 0 {
		return s.ServiceMetadata
	}
	if len(s.ServiceMetadataRaw) == 0 {
		return map[string]ServiceMetadata{}
	}
	var meta map[string]ServiceMetadata
	if err := json.Unmarshal(s.ServiceMetadataRaw, &meta); err != nil {
		return map[string]ServiceMetadata{}
	}
	if meta == nil {
		return map[string]ServiceMetadata{}
	}
	return meta
}

type TaskTracker struct {
	TaskID              uuid.UUID  `json:"task_id" db:"task_id"`
	ScheduleID          uuid.UUID  `json:"schedule_id" db:"schedule_id"`
	CreatedAt           time.Time  `json:"created_at" db:"created_at"`
	ServiceName         string     `json:"service_name" db:"service_name"`
	SchemaName          string     `json:"schema_name" db:"schema_name"`
	TableName           string     `json:"table_name" db:"table_name"`
	JobName             string     `json:"job_name" db:"job_name"`
	Status              TaskStatus `json:"status" db:"status"`
	RetryCount          int        `json:"retry_count" db:"retry_count"`
	MaxRetries          int        `json:"max_retries" db:"max_retries"`
	CancelledAt         *time.Time `json:"cancelled_at,omitempty" db:"cancelled_at"`
	CancelledBy         *string    `json:"cancelled_by,omitempty" db:"cancelled_by"`
	ManifestVersion     string     `json:"manifest_version" db:"manifest_version"`
	ImageTag            string     `json:"image_tag" db:"image_tag"`
	InheritedFromTaskID *uuid.UUID `json:"inherited_from_task_id,omitempty" db:"inherited_from_task_id"`
}

// Transition is a thin shim delegating to the run package.
func (t *TaskTracker) Transition(caller CallerID, to TaskStatus) error {
	allowed, ownerOK := canTaskTransitionViaRun(t.Status, to, caller)
	if !allowed {
		return ErrInvalidTransition
	}
	if !ownerOK {
		return ErrUnauthorizedTransition
	}
	t.Status = to
	return nil
}

type TaskExecution struct {
	ID                   uuid.UUID  `json:"id" db:"id"`
	TaskID               uuid.UUID  `json:"task_id" db:"task_id"`
	CreatedAt            time.Time  `json:"created_at" db:"created_at"`
	StartedAt            *time.Time `json:"started_at,omitempty" db:"started_at"`
	CompletedAt          *time.Time `json:"completed_at,omitempty" db:"completed_at"`
	ExecutionTimeSeconds *float64   `json:"execution_time_seconds,omitempty" db:"execution_time_seconds"`
	ExecutorID           *string    `json:"executor_id,omitempty" db:"executor_id"`
	K8sJobName           *string    `json:"k8s_job_name,omitempty" db:"k8s_job_name"`
	ErrorMessage         *string    `json:"error_message,omitempty" db:"error_message"`
	CancelledAt          *time.Time `json:"cancelled_at,omitempty" db:"cancelled_at"`
	CancelledBy          *string    `json:"cancelled_by,omitempty" db:"cancelled_by"`
	CancellationReason   *string    `json:"cancellation_reason,omitempty" db:"cancellation_reason"`
	LogS3Key             *string    `json:"log_s3_key,omitempty" db:"log_s3_key"`
}

// NodeRun is one row in a node's execution history — the audit-loud projection
// of a (scheduler_tracker × task_tracker × task_execution) join filtered by
// node identity (service_name, schema_name, table_name).
//
// One NodeRun corresponds to one task_tracker row (i.e. one task instance), not
// one scheduler run. Per-task timing comes from the latest task_execution row
// for that task_id; rows with no execution yet (PENDING tasks) carry nil
// timings and an empty ErrorMessage / LogS3Key. The Kind and TerminalStatus
// fields come from the parent scheduler_tracker.
type NodeRun struct {
	ScheduleID      uuid.UUID  `db:"run_id"`
	ScheduleName    string     `db:"schedule_name"`
	Kind            string     `db:"kind"`
	TerminalStatus  string     `db:"terminal_status"`
	TaskID          uuid.UUID  `db:"task_id"`
	TaskStatus      TaskStatus `db:"task_status"`
	RetryCount      int        `db:"retry_count"`
	ImageTag        string     `db:"image_tag"`
	ManifestVersion string     `db:"manifest_version"`
	CreatedAt       time.Time  `db:"created_at"`
	StartedAt       *time.Time `db:"started_at"`
	CompletedAt     *time.Time `db:"completed_at"`
	ErrorMessage    *string    `db:"error_message"`
	LogS3Key        *string    `db:"log_s3_key"`
}

// ServiceMetadata aliases run.ServiceMetadata.
type ServiceMetadata = run.ServiceMetadata

// canSchedulerTransitionViaRun and canTaskTransitionViaRun are private bridges
// so the legacy Transition shims do not duplicate the table. They will go away
// in Step 7 when the file is deleted.
func canSchedulerTransitionViaRun(from, to SchedulerStatus) bool {
	type t struct{ from, to SchedulerStatus }
	for _, tr := range []t{
		{run.SchedulerStatusPending, run.SchedulerStatusRunning},
		{run.SchedulerStatusRunning, run.SchedulerStatusSucceeded},
		{run.SchedulerStatusRunning, run.SchedulerStatusFailed},
	} {
		if tr.from == from && tr.to == to {
			return true
		}
	}
	return false
}

func canTaskTransitionViaRun(from, to TaskStatus, caller CallerID) (allowed, ownerOK bool) {
	type t struct {
		from, to TaskStatus
		owner    CallerID
	}
	for _, tr := range []t{
		{run.TaskStatusFailed, run.TaskStatusPending, run.CallerStartupController},
		{run.TaskStatusPending, run.TaskStatusRunning, run.CallerExecutorController},
		{run.TaskStatusFailed, run.TaskStatusRunning, run.CallerExecutorController},
		{run.TaskStatusRunning, run.TaskStatusSucceeded, run.CallerK8sController},
		{run.TaskStatusRunning, run.TaskStatusFailed, run.CallerK8sController},
	} {
		if tr.from == from && tr.to == to {
			return true, tr.owner == caller
		}
	}
	return false, false
}
