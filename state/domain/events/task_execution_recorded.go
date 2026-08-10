package events

import (
	"time"

	"github.com/google/uuid"
)

// TaskExecutionRecorded is the typed form of task.execution.recorded:v1.
// Optional fields are nil-able; the parser converts empty wire values to nil.
type TaskExecutionRecorded struct {
	ExecutionID          uuid.UUID
	TaskID               uuid.UUID
	JobName              *string
	StartedAt            *time.Time
	CompletedAt          *time.Time
	ExecutionTimeSeconds *float64
	ErrorMessage         *string
	LogS3Key             *string
	// RunResultsURI is the S3 object key of the structured result block the
	// pod printed, when it printed one. Nil for executions whose container
	// emits no block.
	RunResultsURI    *string
	ParseCache       *string
	ParseCacheReason *string
}
