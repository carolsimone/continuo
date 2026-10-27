package projection

import (
	"time"

	"github.com/google/uuid"

	"github.com/carolsimone/continuo/state/domain/aggregate/run"
)

// RunDetail is one run as the run reads show it: its identity, what it runs
// and on which release, where it stands, and when each step happened. A step
// that has not happened is nil.
type RunDetail struct {
	RunID        uuid.UUID
	ScheduleName string
	Kind         string
	Operation    string
	Status       run.RunStatus
	// FailureReason is why the run failed when no task of it failed; empty
	// otherwise.
	FailureReason string
	// ReleaseID and PromotionSeq name the release the run executes on; empty
	// and 0 when it names none.
	ReleaseID    string
	PromotionSeq int64
	// InitiatedBy is the identity that started the run; empty for a cron fire.
	InitiatedBy        string
	CreatedAt          time.Time
	StartedAt          *time.Time
	CompletedAt        *time.Time
	LastProgressAt     *time.Time
	CancelledAt        *time.Time
	CancelledBy        string
	CancellationReason string
}

// TaskDetail is one task of a run as the task list shows it.
type TaskDetail struct {
	TaskID      uuid.UUID
	RunID       uuid.UUID
	CreatedAt   time.Time
	ServiceName string
	SchemaName  string
	TableName   string
	JobName     string
	Status      run.TaskStatus
	// Attempt is the 0-based number of the task's current attempt.
	Attempt    int
	MaxRetries int
	// CancelledAt and CancelledBy are the stamps a cancel left on the task;
	// they are meaningful only while the task is cancelled.
	CancelledAt *time.Time
	CancelledBy string
	// WaitReason is what the pending task waits for besides its upstreams;
	// empty when it waits for nothing else. WaitingSince is when it began
	// waiting, nil without a reason.
	WaitReason   run.WaitReason
	WaitingSince *time.Time
	// WaitingOnRunID is the run whose task holds the node the task waits to
	// write; nil when the task waits for no lease or no task holds the node.
	WaitingOnRunID *uuid.UUID
}
