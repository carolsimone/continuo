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
	// FailedContainer is the name of the first container (init or main)
	// that terminated non-zero. Empty on success or when pod details are
	// unavailable. Drives failure attribution (compile leg containers map
	// to distinct release-reject reasons).
	FailedContainer string
	// InitTerminationMessages maps init-container name -> termination
	// message for terminated init containers. Carries the
	// hydrate-parse-cache outcome ("hydrated" / "degraded:<reason>").
	InitTerminationMessages map[string]string
}
