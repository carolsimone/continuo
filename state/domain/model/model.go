package model

import (
	"database/sql"
	"encoding/json"
	"errors"
	"time"

	"github.com/google/uuid"
)

// SchedulerStatus represents valid states for scheduler_tracker
type SchedulerStatus string

const (
	SchedulerStatusPending   SchedulerStatus = "pending"
	SchedulerStatusRunning   SchedulerStatus = "running"
	SchedulerStatusSucceeded SchedulerStatus = "succeeded"
	SchedulerStatusFailed    SchedulerStatus = "failed"
	SchedulerStatusCancelled SchedulerStatus = "cancelled"
)

// IsValid checks if the SchedulerStatus is valid
func (s SchedulerStatus) IsValid() bool {
	switch s {
	case SchedulerStatusPending, SchedulerStatusRunning, SchedulerStatusSucceeded,
		SchedulerStatusFailed, SchedulerStatusCancelled:
		return true
	}
	return false
}

// TaskStatus represents valid states for task_tracker and task_execution
type TaskStatus string

const (
	TaskStatusPending   TaskStatus = "pending"
	TaskStatusRunning   TaskStatus = "running"
	TaskStatusSucceeded TaskStatus = "succeeded"
	TaskStatusFailed    TaskStatus = "failed"
	TaskStatusCancelled TaskStatus = "cancelled"
	TaskStatusSkipped   TaskStatus = "skipped"
)

// IsValid checks if the TaskStatus is valid
func (t TaskStatus) IsValid() bool {
	switch t {
	case TaskStatusPending, TaskStatusRunning, TaskStatusSucceeded,
		TaskStatusFailed, TaskStatusCancelled, TaskStatusSkipped:
		return true
	}
	return false
}

// CallerID identifies which service is performing a task state transition.
type CallerID string

const (
	CallerStartupController  CallerID = "startup-controller"
	CallerExecutorController CallerID = "executor-controller"
	CallerK8sController      CallerID = "k8s-controller"
)

var (
	ErrInvalidTransition      = errors.New("invalid state transition")
	ErrUnauthorizedTransition = errors.New("unauthorized state transition")
)

type taskTransition struct {
	from  TaskStatus
	to    TaskStatus
	owner CallerID
}

var allowedTaskTransitions = []taskTransition{
	{TaskStatusFailed, TaskStatusPending, CallerStartupController},
	{TaskStatusPending, TaskStatusRunning, CallerExecutorController},
	{TaskStatusFailed, TaskStatusRunning, CallerExecutorController},
	{TaskStatusRunning, TaskStatusSucceeded, CallerK8sController},
	{TaskStatusRunning, TaskStatusFailed, CallerK8sController},
}

type schedulerTransition struct {
	from SchedulerStatus
	to   SchedulerStatus
}

var allowedSchedulerTransitions = []schedulerTransition{
	{SchedulerStatusPending, SchedulerStatusRunning},
	{SchedulerStatusRunning, SchedulerStatusSucceeded},
	{SchedulerStatusRunning, SchedulerStatusFailed},
}

// Transition attempts to move the scheduler to the target status.
// Returns ErrInvalidTransition if the (from, to) pair is not in the allowed set.
// Status is only mutated on success.
// Note: cancelled is handled separately via repo.Cancel() and is not in this table.
func (s *SchedulerTracker) Transition(to SchedulerStatus) error {
	for _, tr := range allowedSchedulerTransitions {
		if tr.from == s.Status && tr.to == to {
			s.Status = to
			return nil
		}
	}
	return ErrInvalidTransition
}

// Transition attempts to move the task to the target status on behalf of the given caller.
// Returns ErrInvalidTransition if the (from, to) pair is not in the allowed set.
// Returns ErrUnauthorizedTransition if the transition exists but this caller does not own it.
// Status is only mutated on success.
func (t *TaskTracker) Transition(caller CallerID, to TaskStatus) error {
	for _, tr := range allowedTaskTransitions {
		if tr.from == t.Status && tr.to == to {
			if tr.owner != caller {
				return ErrUnauthorizedTransition
			}
			t.Status = to
			return nil
		}
	}
	return ErrInvalidTransition
}

// SchedulerTracker represents a scheduler execution run
// Maps to the scheduler_tracker table in PostgreSQL
type SchedulerTracker struct {
	ScheduleID           uuid.UUID         `json:"schedule_id" db:"schedule_id"`
	ScheduleName         string            `json:"schedule_name" db:"schedule_name"`
	Status               SchedulerStatus   `json:"status" db:"status"`
	CreatedAt            time.Time         `json:"created_at" db:"created_at"`
	StartedAt            *time.Time        `json:"started_at,omitempty" db:"started_at"`
	CompletedAt          *time.Time        `json:"completed_at,omitempty" db:"completed_at"`
	LastHeartbeatAt      *time.Time        `json:"last_heartbeat_at,omitempty" db:"last_heartbeat_at"`
	CancelledAt          *time.Time        `json:"cancelled_at,omitempty" db:"cancelled_at"`
	CancelledBy          *string           `json:"cancelled_by,omitempty" db:"cancelled_by"`
	CancellationReason   *string           `json:"cancellation_reason,omitempty" db:"cancellation_reason"`
	InitializationStatus string            `json:"initialization_status" db:"initialization_status"`
	TotalTaskCount       sql.NullInt32     `json:"total_task_count,omitempty" db:"total_task_count"`
	TerminalTaskCount    int32             `json:"terminal_task_count" db:"terminal_task_count"`
	ManifestVersions     map[string]string `json:"manifest_versions"`
	ManifestVersionsRaw  json.RawMessage   `json:"-" db:"manifest_versions"`
}

func (s *SchedulerTracker) GetManifestVersions() map[string]string {
	if len(s.ManifestVersions) > 0 {
		return s.ManifestVersions
	}
	if len(s.ManifestVersionsRaw) == 0 {
		return map[string]string{}
	}

	var versions map[string]string
	if err := json.Unmarshal(s.ManifestVersionsRaw, &versions); err != nil {
		return map[string]string{}
	}
	if versions == nil {
		return map[string]string{}
	}
	return versions
}

// TaskTracker represents a task execution within a schedule
// Maps to the task_tracker table in PostgreSQL
type TaskTracker struct {
	TaskID          uuid.UUID  `json:"task_id" db:"task_id"`
	ScheduleID      uuid.UUID  `json:"schedule_id" db:"schedule_id"`
	CreatedAt       time.Time  `json:"created_at" db:"created_at"`
	ServiceName     string     `json:"service_name" db:"service_name"`
	SchemaName      string     `json:"schema_name" db:"schema_name"`
	TableName       string     `json:"table_name" db:"table_name"`
	JobName         string     `json:"job_name" db:"job_name"`
	Status          TaskStatus `json:"status" db:"status"`
	RetryCount      int        `json:"retry_count" db:"retry_count"`
	MaxRetries      int        `json:"max_retries" db:"max_retries"`
	CancelledAt     *time.Time `json:"cancelled_at,omitempty" db:"cancelled_at"`
	CancelledBy     *string    `json:"cancelled_by,omitempty" db:"cancelled_by"`
	ManifestVersion string     `json:"manifest_version" db:"manifest_version"`
}

// TaskExecution represents a single execution attempt of a task
// Maps to the task_execution table in PostgreSQL
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
