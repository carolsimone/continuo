package ports

import (
	"context"
	"time"

	"github.com/carolsimone/continuo/execution-controller/domain/model"
)

// JobCache reads attempts' Jobs and pods from the watch's cache.
type JobCache interface {
	// Synced reports whether the cache has listed every Job and pod once.
	Synced() bool
	// Observe returns what the cache holds of jobName's Job and its pods.
	Observe(jobName string) model.JobObservation
	// JobNames returns the name of every Job the cache holds.
	JobNames() []string
}

// JobLookup reads a Job and its pods from the Kubernetes API, past the cache.
type JobLookup interface {
	// ObserveLive returns what the API holds now of jobName's Job and its pods;
	// JobPresent is false when the Job does not exist.
	ObserveLive(ctx context.Context, jobName string) (model.JobObservation, error)
}

// JobTerminator deletes a Job and its pods.
type JobTerminator interface {
	// DeleteJob deletes jobName with foreground propagation: the Job object
	// stays until every pod of it is gone. Deleting a Job that is gone is not
	// an error.
	DeleteJob(ctx context.Context, jobName string) error
}

// PodLogReader reads the log of an attempt's pod.
type PodLogReader interface {
	// GetPodLogs returns the full log and its last tailLines lines of
	// jobName's pod: its main container's, or a failed init container's when
	// the main container wrote nothing. Both are empty when no pod is left.
	GetPodLogs(ctx context.Context, jobName string, tailLines int64) (fullLog, tail string, err error)
}

// ObservationQueue holds the names of the Jobs whose deployments need
// observing. A name queued again before a worker takes it is held once, and
// a name a worker holds is not handed to another until Done.
type ObservationQueue interface {
	Add(jobName string)
	// AddAfter queues jobName once after has passed.
	AddAfter(jobName string, after time.Duration)
	// Get blocks until a name is ready, or returns shutdown=true once the
	// queue is shut down.
	Get() (jobName string, shutdown bool)
	// Done releases a name Get returned.
	Done(jobName string)
	// Retry queues jobName again after a backoff that grows with each retry;
	// Forget resets that backoff.
	Retry(jobName string)
	Forget(jobName string)
	ShutDown()
}

// ObserverMetrics is told which attempts wait for a pod that will not stop.
type ObserverMetrics interface {
	// AwaitingPodStop marks jobName's attempt as waiting longer than the alert
	// threshold for a pod to stop, or clears the mark.
	AwaitingPodStop(jobName string, waiting bool)
}
