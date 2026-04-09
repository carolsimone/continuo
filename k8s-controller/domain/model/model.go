package model

import "time"

// JobStatus represents K8s job completion status
type JobStatus string

const (
	JobStatusRunning   JobStatus = "running"
	JobStatusSucceeded JobStatus = "succeeded"
	JobStatusFailed    JobStatus = "failed"
	JobStatusUnknown   JobStatus = "unknown" // Job not found or error
)

// K8sPodResult encapsulates pod termination details
type K8sPodResult struct {
	Status           JobStatus
	ExitCode         *int32
	TerminationMsg   string
	StartedAt        *time.Time
	CompletedAt      *time.Time
	ExecutionSeconds float64
}
