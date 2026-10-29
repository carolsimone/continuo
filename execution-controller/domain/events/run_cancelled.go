package events

import "github.com/google/uuid"

// RunCancelled is the parsed run.cancelled:v1 payload: run-controller cancelled the
// run, and every attempt of it that has not ended is to be stopped.
// CancelledBy and Reason are the operator's own words, only logged. Attempts
// is empty on the first send; a re-send names each attempt run-controller still waits
// to hear has stopped. TenantID is the tenant of the run's cancel; the report
// of an attempt it names that execution-controller never received is published
// under it.
type RunCancelled struct {
	TenantID    string
	RunID       uuid.UUID
	CancelledBy string
	Reason      string
	Attempts    []AttemptRef
}

// AttemptRef names one attempt of a cancelled run and the Job that runs it.
type AttemptRef struct {
	TaskID  string
	Attempt int
	JobName string
}
