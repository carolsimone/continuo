package run

import (
	"time"

	"github.com/carolsimone/continuo/pkg/domain/model"
	"github.com/google/uuid"
)

// AttemptStatus is the lifecycle status of one execution attempt of a task:
// a value of the contract's attempt_status vocabulary, which the two task
// streams carry.
type AttemptStatus string

const (
	AttemptDispatched = AttemptStatus(model.AttemptStatusDispatched)
	AttemptQueued     = AttemptStatus(model.AttemptStatusQueued)
	AttemptRunning    = AttemptStatus(model.AttemptStatusRunning)
	AttemptSucceeded  = AttemptStatus(model.AttemptStatusSucceeded)
	AttemptFailed     = AttemptStatus(model.AttemptStatusFailed)
	// AttemptStopped is an attempt that was stopped on request: its run was
	// cancelled and no pod of it can run again.
	AttemptStopped = AttemptStatus(model.AttemptStatusStopped)
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
	case AttemptSucceeded, AttemptFailed, AttemptStopped:
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
	// task's dispatch time, which run-controller's own clock stamped when it dispatched
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

	// NextCheckIn is when, from now on the database clock, run-controller next asks
	// execution-controller about the attempt; zero when saving it leaves the
	// stored next check as it is. A final attempt is never asked about.
	NextCheckIn time.Duration
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
