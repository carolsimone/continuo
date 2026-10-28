package postgres

import (
	"database/sql"
	"time"

	"github.com/carolsimone/continuo/state/domain/aggregate/run"
	"github.com/google/uuid"
)

// RunRow is the storage row carrier for the runs table.
// It mirrors the table schema one-to-one and is used exclusively within this
// package by the repository implementations. Domain logic operates on
// run.Run aggregates; hydrateRun / dehydrateRun translate between the two.
type RunRow struct {
	TenantID           string        `json:"tenant_id" db:"tenant_id"`
	ScheduleID         uuid.UUID     `json:"schedule_id" db:"run_id"`
	ScheduleName       string        `json:"schedule_name" db:"schedule_name"`
	Status             run.RunStatus `json:"status" db:"status"`
	CreatedAt          time.Time     `json:"created_at" db:"created_at"`
	StartedAt          *time.Time    `json:"started_at,omitempty" db:"started_at"`
	CompletedAt        *time.Time    `json:"completed_at,omitempty" db:"completed_at"`
	LastProgressAt     *time.Time    `json:"last_heartbeat_at,omitempty" db:"last_progress_at"`
	CancelledAt        *time.Time    `json:"cancelled_at,omitempty" db:"cancelled_at"`
	CancelledBy        *string       `json:"cancelled_by,omitempty" db:"cancelled_by"`
	CancellationReason *string       `json:"cancellation_reason,omitempty" db:"cancellation_reason"`
	TotalTaskCount     sql.NullInt32 `json:"total_task_count,omitempty" db:"total_task_count"`
	TerminalTaskCount  int32         `json:"terminal_task_count" db:"terminal_task_count"`
	Kind               string        `json:"kind" db:"kind"`
	SourceRunID        *uuid.UUID    `json:"source_run_id,omitempty" db:"source_run_id"`
	InitiatedBy        string        `json:"initiated_by" db:"initiated_by"`
	Operation          string        `json:"operation" db:"operation"`
	ReleaseID          *string       `json:"release_id,omitempty" db:"release_id"`
	PromotionSeq       *int64        `json:"promotion_seq,omitempty" db:"promotion_seq"`
	PlannedAt          *time.Time    `json:"planned_at,omitempty" db:"planned_at"`
	FailureReason      *string       `json:"failure_reason,omitempty" db:"failure_reason"`
	ScheduledFor       *time.Time    `json:"scheduled_for,omitempty" db:"scheduled_for"`
}

// TaskRow is the storage row carrier for the tasks table.
// It mirrors the table schema one-to-one, plus WaitingOnRunID, which the task
// reads join from node_leases. The repository implementations read and write
// it, and the gRPC task handler maps the rows the task reads return; domain
// logic operates on run.Task values, and the adapters translate between the
// two.
type TaskRow struct {
	TenantID            string         `json:"tenant_id" db:"tenant_id"`
	TaskID              uuid.UUID      `json:"task_id" db:"task_id"`
	ScheduleID          uuid.UUID      `json:"schedule_id" db:"run_id"`
	CreatedAt           time.Time      `json:"created_at" db:"created_at"`
	ServiceName         string         `json:"service_name" db:"service_name"`
	SchemaName          string         `json:"schema_name" db:"schema_name"`
	TableName           string         `json:"table_name" db:"table_name"`
	JobName             string         `json:"job_name" db:"job_name"`
	Status              run.TaskStatus `json:"status" db:"status"`
	Attempt             int            `json:"attempt" db:"attempt"`
	MaxRetries          int            `json:"max_retries" db:"max_retries"`
	CancelledAt         *time.Time     `json:"cancelled_at,omitempty" db:"cancelled_at"`
	CancelledBy         *string        `json:"cancelled_by,omitempty" db:"cancelled_by"`
	ImageTag            string         `json:"image_tag" db:"image_tag"`
	InheritedFromTaskID *uuid.UUID     `json:"inherited_from_task_id,omitempty" db:"inherited_from_task_id"`
	Operation           string         `json:"operation" db:"operation"`
	NodeKey             *string        `json:"node_key,omitempty" db:"node_key"`
	DispatchedAt        *time.Time     `json:"dispatched_at,omitempty" db:"dispatched_at"`
	WaitReason          *string        `json:"wait_reason,omitempty" db:"wait_reason"`
	LeaseWaitSince      *time.Time     `json:"lease_wait_since,omitempty" db:"lease_wait_since"`
	// WaitingOnRunID is not a column: it is the run whose task holds the node
	// lease this task waits for, read by the task reads (GetByID,
	// GetByRunAndNode, ListByRunID); nil when the task waits for no
	// lease, no task holds its node, or the row was read by any other query.
	WaitingOnRunID *uuid.UUID `json:"waiting_on_run_id,omitempty" db:"waiting_on_run_id"`
}

// taskAttemptRow is the storage row carrier for the task_attempts table. It
// mirrors the columns attempts read and write, one-to-one. It leaves out the
// scheduling columns (next_check_at, checks_sent), which the attempt SQL
// writes itself: addAttempts sets next_check_at when an attempt is dispatched,
// saveAttempt moves it with each non-final report and clears it once the
// attempt has ended, requestStops restarts checks_sent and sets next_check_at
// to the stop's first window when the attempt is asked to stop, and
// AttemptRepository.MarkQuestioned counts each question about the attempt in
// checks_sent and moves next_check_at to the next window.
type taskAttemptRow struct {
	TaskID           uuid.UUID  `db:"task_id"`
	Attempt          int32      `db:"attempt"`
	JobName          string     `db:"job_name"`
	Status           string     `db:"status"`
	FailureClass     *string    `db:"failure_class"`
	FailureReason    *string    `db:"failure_reason"`
	DispatchedAt     time.Time  `db:"dispatched_at"`
	QueuedAt         *time.Time `db:"queued_at"`
	StartedAt        *time.Time `db:"started_at"`
	FinishedAt       *time.Time `db:"finished_at"`
	ExecutionSeconds *float64   `db:"execution_seconds"`
	ExitCode         *int32     `db:"exit_code"`
	ErrorMessage     *string    `db:"error_message"`
	LogS3Key         *string    `db:"log_s3_key"`
	RunResultsURI    *string    `db:"run_results_uri"`
	ParseCache       *string    `db:"parse_cache"`
	ParseCacheReason *string    `db:"parse_cache_reason"`
}
