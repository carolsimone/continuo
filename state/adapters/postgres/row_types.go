package postgres

import (
	"database/sql"
	"fmt"
	"time"

	"github.com/carolsimone/continuo/state/domain/aggregate/run"
	"github.com/google/uuid"
)

// SchedulerTracker is the storage row carrier for the scheduler_tracker table.
// It mirrors the table schema one-to-one and is used exclusively within this
// package by the repository implementations. Domain logic operates on
// run.Run aggregates; hydrateRun / dehydrateRun translate between the two.
type SchedulerTracker struct {
	ScheduleID           uuid.UUID                      `json:"schedule_id" db:"schedule_id"`
	ScheduleName         string                         `json:"schedule_name" db:"schedule_name"`
	Status               run.SchedulerStatus            `json:"status" db:"status"`
	CreatedAt            time.Time                      `json:"created_at" db:"created_at"`
	StartedAt            *time.Time                     `json:"started_at,omitempty" db:"started_at"`
	CompletedAt          *time.Time                     `json:"completed_at,omitempty" db:"completed_at"`
	LastHeartbeatAt      *time.Time                     `json:"last_heartbeat_at,omitempty" db:"last_heartbeat_at"`
	CancelledAt          *time.Time                     `json:"cancelled_at,omitempty" db:"cancelled_at"`
	CancelledBy          *string                        `json:"cancelled_by,omitempty" db:"cancelled_by"`
	CancellationReason   *string                        `json:"cancellation_reason,omitempty" db:"cancellation_reason"`
	InitializationStatus string                         `json:"initialization_status" db:"initialization_status"`
	TotalTaskCount       sql.NullInt32                  `json:"total_task_count,omitempty" db:"total_task_count"`
	TerminalTaskCount    int32                          `json:"terminal_task_count" db:"terminal_task_count"`
	Kind                 string                         `json:"kind" db:"kind"`
	SourceRunID          *uuid.UUID                     `json:"source_run_id,omitempty" db:"source_run_id"`
	InitiatedBy          string                         `json:"initiated_by" db:"initiated_by"`
	Operation            string                         `json:"operation" db:"operation"`
	ServiceMetadata      map[string]run.ServiceMetadata `json:"service_metadata"`
	ServiceMetadataRaw   []byte                         `json:"-" db:"service_metadata"`
}

// GetServiceMetadata returns the decoded ServiceMetadata map. ServiceMetadata
// is preferred when already populated (e.g. after a previous decode or an
// explicit assignment). Otherwise ServiceMetadataRaw is decoded on the fly.
// A malformed service_metadata JSONB is surfaced as an error rather than
// silently coerced to an empty map, which would erase a run's per-service
// pinning and corrupt downstream dispatch.
func (s *SchedulerTracker) GetServiceMetadata() (map[string]run.ServiceMetadata, error) {
	if len(s.ServiceMetadata) > 0 {
		return s.ServiceMetadata, nil
	}
	meta, err := unmarshalServiceMetadata(s.ServiceMetadataRaw)
	if err != nil {
		return nil, fmt.Errorf("decode service_metadata for schedule_id %s: %w", s.ScheduleID, err)
	}
	return meta, nil
}

// TaskTracker is the storage row carrier for the task_tracker table.
// It mirrors the table schema one-to-one and is used exclusively within this
// package by the repository implementations. Domain logic operates on
// run.Task values; the adapters translate between the two.
type TaskTracker struct {
	TaskID              uuid.UUID      `json:"task_id" db:"task_id"`
	ScheduleID          uuid.UUID      `json:"schedule_id" db:"schedule_id"`
	CreatedAt           time.Time      `json:"created_at" db:"created_at"`
	ServiceName         string         `json:"service_name" db:"service_name"`
	SchemaName          string         `json:"schema_name" db:"schema_name"`
	TableName           string         `json:"table_name" db:"table_name"`
	JobName             string         `json:"job_name" db:"job_name"`
	Status              run.TaskStatus `json:"status" db:"status"`
	RetryCount          int            `json:"retry_count" db:"retry_count"`
	MaxRetries          int            `json:"max_retries" db:"max_retries"`
	CancelledAt         *time.Time     `json:"cancelled_at,omitempty" db:"cancelled_at"`
	CancelledBy         *string        `json:"cancelled_by,omitempty" db:"cancelled_by"`
	ManifestVersion     string         `json:"manifest_version" db:"manifest_version"`
	ImageTag            string         `json:"image_tag" db:"image_tag"`
	InheritedFromTaskID *uuid.UUID     `json:"inherited_from_task_id,omitempty" db:"inherited_from_task_id"`
}

// TaskExecution is the storage row carrier for the task_execution table.
// It mirrors the table schema one-to-one and is used exclusively within this
// package by the repository implementations.
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
