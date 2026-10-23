package run

import (
	"time"

	"github.com/carolsimone/continuo/pkg/domain/model"
	"github.com/carolsimone/continuo/state/domain/topology"
	"github.com/google/uuid"
)

// StallAfter is how long a run with no attempt in flight may go without
// progress before state fails it as stalled. Every legitimate wait has its own
// clock, so such a run is left behind by a defect in state; the value is not
// an operator setting.
const StallAfter = 5 * time.Minute

// TaskDefaults are the install's settings for the tasks state plans: the
// retry budget stamped on each task when its run is created, and the timeout
// of a task whose node declares none.
type TaskDefaults struct {
	MaxRetries int32
	Timeout    time.Duration
}

// TaskTimeout is how long the container of an attempt of the task at node may
// run: the node's declared timeout, else the install's default.
func TaskTimeout(node topology.Node, d TaskDefaults) time.Duration {
	timeout, _ := EffectiveTimeout(node.TimeoutSeconds, d)
	return timeout
}

// EffectiveTimeout resolves a node's task timeout from the seconds it
// declares, nil when it declares none: the declared timeout, else the
// install's default. declared reports whether the node declares one, even
// when it equals the default.
func EffectiveTimeout(declaredSeconds *int, d TaskDefaults) (timeout time.Duration, declared bool) {
	if declaredSeconds != nil {
		return time.Duration(*declaredSeconds) * time.Second, true
	}
	return d.Timeout, false
}

// Retryable reports whether a failed attempt of class may be followed by
// another while its task has retries left. A task's own failure, a resource
// kill, a platform loss and a start error can pass on the next try; a timeout
// would take as long again and an invalid dispatch can never run, so neither
// is retried.
func Retryable(class model.FailureClass) bool {
	switch class {
	case model.FailureClassTask, model.FailureClassResource, model.FailureClassInfra, model.FailureClassStart:
		return true
	}
	return false
}

// NextCheckAfter is how long state waits, from now, before it asks
// execution-controller about an attempt in status s that has already been
// asked about checksSent times. An attempt still dispatched is asked about
// after 5 minutes, then 10, 20, 40 and every 60; an acknowledged one after 30
// minutes of silence, and every 30 after that. A final attempt is never asked
// about.
func NextCheckAfter(s AttemptStatus, checksSent int) time.Duration {
	switch s {
	case AttemptDispatched:
		// 5 minutes doubled once per question sent, at most an hour.
		return min(5*time.Minute<<min(checksSent, 4), time.Hour)
	case AttemptQueued, AttemptRunning:
		return 30 * time.Minute
	}
	return 0
}

// NextStopCheckAfter is how long state waits, from now, before it re-sends
// the cancel of a run for an attempt it asked to stop and has re-sent the
// cancel for checksSent times: 5 minutes, then 10, 20, 40 and every 60,
// whatever status the attempt last reported.
func NextStopCheckAfter(checksSent int) time.Duration {
	// 5 minutes doubled once per re-send, at most an hour.
	return min(5*time.Minute<<min(checksSent, 4), time.Hour)
}

// AttemptReport is what execution-controller reported of one attempt of a
// task: its status and every fact known of it so far. Status is queued,
// running, succeeded, failed or stopped; a failed report carries its
// FailureReason. A
// nil fact is one the report does not carry.
type AttemptReport struct {
	TaskID           uuid.UUID
	Attempt          int32
	JobName          string
	Status           AttemptStatus
	QueuedAt         *time.Time
	StartedAt        *time.Time
	FinishedAt       *time.Time
	ExecutionSeconds *float64
	ExitCode         *int32
	FailureReason    model.FailureReason
	ErrorMessage     *string
	LogS3Key         *string
	RunResultsURI    *string
	ParseCache       *string
	ParseCacheReason *string
}

// Apply applies rep to the attempt by rank. A report that ranks higher than
// the attempt's status moves the status; one of equal rank on an attempt that
// has not ended (the usual answer to a question about a long attempt) moves
// nothing but its facts and its next check. A report of lower rank, and any
// report for an attempt that has ended, changes nothing. applied reports
// whether the attempt took the report, changed whether its status moved. An
// applied report copies every fact it carries; a fact it omits keeps its
// stored value.
func (a *Attempt) Apply(rep AttemptReport) (applied, changed bool) {
	if a.Status.IsFinal() || rep.Status.Rank() < a.Status.Rank() {
		return false, false
	}
	changed = a.Advance(rep.Status)
	a.copyFacts(rep)
	a.NextCheckIn = NextCheckAfter(a.Status, 0)
	return true, changed
}

func (a *Attempt) copyFacts(rep AttemptReport) {
	if rep.QueuedAt != nil {
		a.QueuedAt = rep.QueuedAt
	}
	if rep.StartedAt != nil {
		a.StartedAt = rep.StartedAt
	}
	if rep.FinishedAt != nil {
		a.FinishedAt = rep.FinishedAt
	}
	if rep.ExecutionSeconds != nil {
		a.ExecutionSeconds = rep.ExecutionSeconds
	}
	if rep.ExitCode != nil {
		a.ExitCode = rep.ExitCode
	}
	if rep.ErrorMessage != nil {
		a.ErrorMessage = rep.ErrorMessage
	}
	if rep.LogS3Key != nil {
		a.LogS3Key = rep.LogS3Key
	}
	if rep.RunResultsURI != nil {
		a.RunResultsURI = rep.RunResultsURI
	}
	if rep.ParseCache != nil {
		a.ParseCache = rep.ParseCache
	}
	if rep.ParseCacheReason != nil {
		a.ParseCacheReason = rep.ParseCacheReason
	}
	if rep.JobName != "" {
		a.JobName = rep.JobName
	}
	if rep.FailureReason != "" {
		reason, class := string(rep.FailureReason), string(rep.FailureReason.Class())
		a.FailureReason, a.FailureClass = &reason, &class
	}
}
