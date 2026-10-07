package ports

import (
	"context"
	"time"
)

// JobState is what admission reconciliation needs to know about one Job.
type JobState struct {
	// Finished reports that the Job completed or failed.
	Finished bool
	// FinishedAt is when it finished; zero while it runs.
	FinishedAt time.Time
}

// JobInventory lists the Jobs execution-controller created, keyed by name.
type JobInventory interface {
	ListJobs(ctx context.Context) (map[string]JobState, error)
}
