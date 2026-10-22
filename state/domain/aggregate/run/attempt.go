package run

import (
	"time"

	"github.com/google/uuid"
)

// AttemptStatus is the lifecycle status of one execution attempt of a task.
type AttemptStatus string

const (
	AttemptDispatched AttemptStatus = "dispatched"
	AttemptQueued     AttemptStatus = "queued"
	AttemptRunning    AttemptStatus = "running"
	AttemptSucceeded  AttemptStatus = "succeeded"
	AttemptFailed     AttemptStatus = "failed"
	AttemptCancelled  AttemptStatus = "cancelled"
)

// Rank orders the statuses along an attempt's life: dispatched 0, queued 1,
// running 2, any final status 3. An unknown status ranks -1.
func (s AttemptStatus) Rank() int {
	switch s {
	case AttemptDispatched:
		return 0
	case AttemptQueued:
		return 1
	case AttemptRunning:
		return 2
	case AttemptSucceeded, AttemptFailed, AttemptCancelled:
		return 3
	}
	return -1
}

// IsFinal reports whether the attempt has ended.
func (s AttemptStatus) IsFinal() bool { return s.Rank() == 3 }

// Attempt is one execution of a task: the Kubernetes Job that ran it, how it
// ended and what it reported. A task that retries has one Attempt per try,
// numbered from 0.
type Attempt struct {
	TaskID  uuid.UUID
	Number  int32
	JobName string
	Status  AttemptStatus

	FailureClass  *string
	FailureReason *string

	// DispatchedAt is when the attempt was dispatched. Attempt 0 takes the
	// task's dispatch time, which state's own clock stamped when it dispatched
	// the task. A later attempt, or attempt 0 of a task with no dispatch
	// time, is nil until it is first saved, when the database clock stamps it.
	DispatchedAt *time.Time
	QueuedAt     *time.Time
	StartedAt    *time.Time
	FinishedAt   *time.Time

	ExecutionSeconds *float64
	ExitCode         *int32

	ErrorMessage     *string
	LogS3Key         *string
	RunResultsURI    *string
	ParseCache       *string
	ParseCacheReason *string
}

// ExecutionRecord is the facts one execution record reports about an attempt,
// in the aggregate's own vocabulary. A nil fact is one the record does not
// carry.
type ExecutionRecord struct {
	TaskID               uuid.UUID
	JobName              string
	StartedAt            *time.Time
	CompletedAt          *time.Time
	ExecutionTimeSeconds *float64
	ErrorMessage         *string
	LogS3Key             *string
	// RunResultsURI is the S3 object key of the structured result block the
	// pod printed, when it printed one.
	RunResultsURI    *string
	ParseCache       *string
	ParseCacheReason *string
}

// Advance moves the status to s only when s ranks higher than the current one,
// so a late or replayed report never walks an attempt backwards. It reports
// whether the status changed.
func (a *Attempt) Advance(s AttemptStatus) bool {
	if s.Rank() <= a.Status.Rank() {
		return false
	}
	a.Status = s
	return true
}

// RecordExecution copies every fact the record carries onto the attempt. A fact
// the record omits leaves the stored value untouched; the attempt's identity
// and status are never changed by a record.
func (a *Attempt) RecordExecution(rec ExecutionRecord) {
	if rec.StartedAt != nil {
		a.StartedAt = rec.StartedAt
	}
	if rec.CompletedAt != nil {
		a.FinishedAt = rec.CompletedAt
	}
	if rec.ExecutionTimeSeconds != nil {
		a.ExecutionSeconds = rec.ExecutionTimeSeconds
	}
	if rec.ErrorMessage != nil {
		a.ErrorMessage = rec.ErrorMessage
	}
	if rec.LogS3Key != nil {
		a.LogS3Key = rec.LogS3Key
	}
	if rec.RunResultsURI != nil {
		a.RunResultsURI = rec.RunResultsURI
	}
	if rec.ParseCache != nil {
		a.ParseCache = rec.ParseCache
	}
	if rec.ParseCacheReason != nil {
		a.ParseCacheReason = rec.ParseCacheReason
	}
}

// attemptStatusOfReport maps a task status a report carries to the status of
// the attempt it describes. Only running, succeeded and failed describe an
// attempt.
func attemptStatusOfReport(s TaskStatus) (AttemptStatus, bool) {
	switch s {
	case TaskStatusRunning:
		return AttemptRunning, true
	case TaskStatusSucceeded:
		return AttemptSucceeded, true
	case TaskStatusFailed:
		return AttemptFailed, true
	}
	return "", false
}

// attemptOfReport returns the attempt a status report belongs to. Normally that
// is the retry count the report carries. A failure that reports the exhausted
// budget (retryCount >= MaxRetries) when no attempt row exists at that count
// belongs to the task's current attempt, or the one after it when the task has
// already failed; it is never above the reported count, because the count is
// the newest attempt the reporter knows of and no later attempt can have run.
// heldAtRetryCount says an attempt row exists at the reported count.
func attemptOfReport(status TaskStatus, retryCount int32, task TaskAttemptState, heldAtRetryCount bool) int32 {
	if status != TaskStatusFailed || retryCount < task.MaxRetries || heldAtRetryCount {
		return retryCount
	}
	attempt := task.RetryCount
	if task.Status == TaskStatusFailed {
		attempt++
	}
	return min(attempt, retryCount)
}
