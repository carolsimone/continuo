package projection

import (
	"time"

	"github.com/google/uuid"
)

// TaskAttempt is one attempt of a task as ListTaskAttempts shows it: one
// dispatch of the task's Kubernetes Job and what became of it. A fact not
// reported yet is nil.
type TaskAttempt struct {
	TaskID uuid.UUID
	// Attempt is the 0-based attempt number.
	Attempt int
	JobName string
	// Status is dispatched, queued, running, succeeded, failed or cancelled.
	Status string
	// FailureClass and FailureReason say why the attempt failed; nil while
	// it has not failed.
	FailureClass  *string
	FailureReason *string
	ExitCode      *int
	// DispatchedAt is when state dispatched the attempt, on the database
	// clock.
	DispatchedAt     time.Time
	QueuedAt         *time.Time
	StartedAt        *time.Time
	FinishedAt       *time.Time
	ExecutionSeconds *float64
	ErrorMessage     *string
	LogS3Key         *string
	RunResultsURI    *string
}
