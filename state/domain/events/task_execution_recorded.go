package events

import (
	"time"

	"github.com/carolsimone/continuo/state/domain/aggregate/run"
	"github.com/google/uuid"
)

// TaskExecutionRecorded is the typed form of task.execution.recorded:v1.
// JobName names the Kubernetes Job that ran, and through its "-r<n>" suffix
// the attempt the record describes; it is always present. Optional fields are
// nil-able; the parser converts empty wire values to nil.
type TaskExecutionRecorded struct {
	ExecutionID          uuid.UUID
	TaskID               uuid.UUID
	JobName              string
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

// Record returns the facts the event reports about its attempt, as the Run
// aggregate applies them. The execution id identifies the delivery, not the
// attempt, and is not part of the record.
func (e TaskExecutionRecorded) Record() run.ExecutionRecord {
	return run.ExecutionRecord{
		TaskID:               e.TaskID,
		JobName:              e.JobName,
		StartedAt:            e.StartedAt,
		CompletedAt:          e.CompletedAt,
		ExecutionTimeSeconds: e.ExecutionTimeSeconds,
		ErrorMessage:         e.ErrorMessage,
		LogS3Key:             e.LogS3Key,
		RunResultsURI:        e.RunResultsURI,
		ParseCache:           e.ParseCache,
		ParseCacheReason:     e.ParseCacheReason,
	}
}
