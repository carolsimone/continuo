package events

import "github.com/google/uuid"

// RunCancelled is the parsed run.cancelled:v1 payload: state cancelled the
// run, and every attempt of it that has not ended is to be stopped.
// CancelledBy and Reason are the operator's own words, only logged. Attempts
// is empty on the first send; a re-send names each attempt state still waits
// to hear has stopped.
type RunCancelled struct {
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
