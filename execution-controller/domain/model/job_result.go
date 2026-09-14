package model

import "time"

// JobStatus is the coarse state of a Kubernetes Job as observed by the service.
type JobStatus string

const (
	JobStatusRunning   JobStatus = "running"
	JobStatusSucceeded JobStatus = "succeeded"
	JobStatusFailed    JobStatus = "failed"
	JobStatusUnknown   JobStatus = "unknown"
)

// JobResult is what one observation of a Job yields: its status and, once
// terminal, the timings and exit details read from the Job and its pod.
type JobResult struct {
	Status           JobStatus
	ExitCode         *int32
	TerminationMsg   string
	StartedAt        *time.Time
	CompletedAt      *time.Time
	ExecutionSeconds float64
	// FailedContainer names the first init or main container that exited
	// non-zero; empty when the Job succeeded.
	FailedContainer string
	// InitTerminationMessages maps each terminated init container to its
	// termination message.
	InitTerminationMessages map[string]string
}
